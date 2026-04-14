#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "utils.h"
#include "logger.h"
#include "container.h"
#include "monitor_ioctl.h"

// IPC Control Message structures
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

// Supervisor Context
typedef struct {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t g_ctx;
static volatile sig_atomic_t g_child_exited = 0;
static volatile sig_atomic_t g_signal_stop = 0;

static void handle_sigchld(int sig) {
    (void)sig;
    g_child_exited = 1;
}

static void handle_sigstart(int sig) {
    (void)sig;
    g_signal_stop = 1;
}

static void reap_children(void) {
    g_child_exited = 0;
    int status;
    pid_t pid;
    
    pthread_mutex_lock(&g_ctx.metadata_lock);
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *curr = g_ctx.containers;
        while (curr) {
            if (curr->host_pid == pid && curr->state == CONTAINER_RUNNING) {
                if (WIFEXITED(status)) {
                    curr->state = CONTAINER_EXITED;
                    curr->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    curr->state = CONTAINER_KILLED;
                    curr->exit_signal = WTERMSIG(status);
                }
                break;
            }
            curr = curr->next;
        }
    }
    pthread_mutex_unlock(&g_ctx.metadata_lock);
}

typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_arg_t;

static void *producer_thread(void *arg) {
    producer_arg_t *p_arg = (producer_arg_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t bytes_read;
    
    while ((bytes_read = read(p_arg->read_fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        strncpy(item.container_id, p_arg->container_id, CONTAINER_ID_LEN - 1);
        item.container_id[CONTAINER_ID_LEN - 1] = '\0';
        item.length = bytes_read;
        memcpy(item.data, buf, bytes_read);
        bounded_buffer_push(&g_ctx.log_buffer, &item);
    }
    
    close(p_arg->read_fd);
    free(p_arg);
    return NULL;
}

static int spawn_container(control_request_t *req, control_response_t *resp) {
    pthread_mutex_lock(&g_ctx.metadata_lock);
    // Check if ID already exists
    container_record_t *curr = g_ctx.containers;
    while (curr) {
        if (strcmp(curr->id, req->container_id) == 0 && curr->state == CONTAINER_RUNNING) {
            pthread_mutex_unlock(&g_ctx.metadata_lock);
            snprintf(resp->message, sizeof(resp->message), "Container with ID %s is already running", req->container_id);
            resp->status = 1;
            return 1;
        }
        curr = curr->next;
    }

    int pipefd[2];
    if (pipe(pipefd) == -1) {
        pthread_mutex_unlock(&g_ctx.metadata_lock);
        snprintf(resp->message, sizeof(resp->message), "pipe failed");
        resp->status = 1;
        return 1;
    }

    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        pthread_mutex_unlock(&g_ctx.metadata_lock);
        snprintf(resp->message, sizeof(resp->message), "malloc stack failed");
        resp->status = 1;
        return 1;
    }

    child_config_t *config = malloc(sizeof(child_config_t));
    memset(config, 0, sizeof(child_config_t));
    strncpy(config->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(config->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(config->command, req->command, CHILD_COMMAND_LEN - 1);
    config->nice_value = req->nice_value;
    config->log_write_fd = pipefd[1];

    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | CLONE_NEWIPC | SIGCHLD;
    pid_t child_pid = clone(child_fn, stack + STACK_SIZE, flags, config);

    if (child_pid == -1) {
        free(stack);
        free(config);
        close(pipefd[0]);
        close(pipefd[1]);
        pthread_mutex_unlock(&g_ctx.metadata_lock);
        snprintf(resp->message, sizeof(resp->message), "clone failed");
        resp->status = 1;
        return 1;
    }

    close(pipefd[1]); // Supervisor closes write end

    // Start producer thread for this container
    producer_arg_t *p_arg = malloc(sizeof(producer_arg_t));
    p_arg->read_fd = pipefd[0];
    strncpy(p_arg->container_id, req->container_id, CONTAINER_ID_LEN - 1);
    
    pthread_t p_tid;
    pthread_create(&p_tid, NULL, producer_thread, p_arg);
    pthread_detach(p_tid);

    // Save metadata
    container_record_t *record = malloc(sizeof(container_record_t));
    memset(record, 0, sizeof(container_record_t));
    strncpy(record->id, req->container_id, CONTAINER_ID_LEN - 1);
    record->host_pid = child_pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    record->next = g_ctx.containers;
    g_ctx.containers = record;
    pthread_mutex_unlock(&g_ctx.metadata_lock);

    snprintf(resp->message, sizeof(resp->message), "Started container %s with PID %d", req->container_id, child_pid);
    resp->status = 0;
    return 0;
}

static int stop_container(control_request_t *req, control_response_t *resp) {
    pthread_mutex_lock(&g_ctx.metadata_lock);
    container_record_t *curr = g_ctx.containers;
    while (curr) {
        if (strcmp(curr->id, req->container_id) == 0 && curr->state == CONTAINER_RUNNING) {
            kill(curr->host_pid, SIGTERM);
            curr->state = CONTAINER_STOPPED;
            pthread_mutex_unlock(&g_ctx.metadata_lock);
            snprintf(resp->message, sizeof(resp->message), "Stopped container %s", req->container_id);
            resp->status = 0;
            return 0;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&g_ctx.metadata_lock);
    snprintf(resp->message, sizeof(resp->message), "Container %s not found or not running", req->container_id);
    resp->status = 1;
    return 1;
}

static int ps_containers(control_response_t *resp) {
    char buf[CONTROL_MESSAGE_LEN];
    buf[0] = '\0';
    
    pthread_mutex_lock(&g_ctx.metadata_lock);
    container_record_t *curr = g_ctx.containers;
    strcat(buf, "ID\tPID\tSTATE\tEXIT\n");
    while (curr) {
        char line[128];
        snprintf(line, sizeof(line), "%s\t%d\t%s\t%d\n", 
                 curr->id, curr->host_pid, state_to_string(curr->state), 
                 (curr->state == CONTAINER_EXITED) ? curr->exit_code : curr->exit_signal);
        if (strlen(buf) + strlen(line) < sizeof(buf)) {
            strcat(buf, line);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&g_ctx.metadata_lock);
    
    strncpy(resp->message, buf, sizeof(resp->message) - 1);
    resp->status = 0;
    return 0;
}

static int send_control_request(const control_request_t *req) {
    int sock;
    struct sockaddr_un addr;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("client socket failed");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect to supervisor failed");
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) != sizeof(*req)) {
        perror("write to supervisor failed");
        close(sock);
        return 1;
    }

    control_response_t resp;
    int bytes_read;
    if ((bytes_read = read(sock, &resp, sizeof(resp))) > 0) {
        if (strlen(resp.message) > 0) {
            printf("%s", resp.message);
            if (resp.message[strlen(resp.message) - 1] != '\n') {
                printf("\n");
            }
        }
    }

    close(sock);
    return resp.status;
}

static int run_supervisor(const char *rootfs) {
    (void)rootfs; // Supervisor logic does not depend on base-rootfs directory directly

    memset(&g_ctx, 0, sizeof(g_ctx));
    g_ctx.server_fd = -1;
    g_ctx.monitor_fd = -1;

    if (pthread_mutex_init(&g_ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }

    if (bounded_buffer_init(&g_ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&g_ctx.metadata_lock);
        return 1;
    }

    // Signals
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigchld;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_NOCLDSTOP; // NO SA_RESTART so accept() gets interrupted!
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = handle_sigstart;
    sa.sa_flags = 0; // Don't restart accept on SIGINT/SIGTERM
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // Logger
    pthread_create(&g_ctx.logger_thread, NULL, logging_thread, &g_ctx.log_buffer);

    // Socket
    unlink(CONTROL_PATH);
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(sock, 5) < 0) {
        perror("listen");
        return 1;
    }

    g_ctx.server_fd = sock;

    printf("Supervisor running on %s\n", CONTROL_PATH);

    while (!g_signal_stop) {
        if (g_child_exited) {
            reap_children();
        }

        int client_fd = accept(sock, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        control_request_t req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        if (read(client_fd, &req, sizeof(req)) == sizeof(req)) {
            switch (req.kind) {
                case CMD_START:
                    spawn_container(&req, &resp);
                    break;
                case CMD_RUN:
                    spawn_container(&req, &resp);
                    break;
                case CMD_STOP:
                    stop_container(&req, &resp);
                    break;
                case CMD_PS:
                    ps_containers(&resp);
                    break;
                default:
                    snprintf(resp.message, sizeof(resp.message), "Unknown command");
                    resp.status = 1;
            }
            write(client_fd, &resp, sizeof(resp));
        }
        close(client_fd);
    }

    printf("Supervisor shutting down...\n");
    bounded_buffer_begin_shutdown(&g_ctx.log_buffer);
    pthread_join(g_ctx.logger_thread, NULL);

    close(g_ctx.server_fd);
    unlink(CONTROL_PATH);

    // Kill any remaining running containers
    pthread_mutex_lock(&g_ctx.metadata_lock);
    container_record_t *curr = g_ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING) {
            kill(curr->host_pid, SIGTERM);
        }
        container_record_t *next = curr->next;
        free(curr);
        curr = next;
    }
    pthread_mutex_unlock(&g_ctx.metadata_lock);

    bounded_buffer_destroy(&g_ctx.log_buffer);
    pthread_mutex_destroy(&g_ctx.metadata_lock);
    
    // Reap gracefully killed children if necessary
    reap_children();
    
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes) {
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index) {
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0) return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0) return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || nice_value < -20 || nice_value > 19) return -1;
            req->nice_value = (int)nice_value;
            continue;
        }
        return -1;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }
    
    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            usage(argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    
    if (strcmp(argv[1], "start") == 0) {
        if (argc < 5) {
            usage(argv[0]);
            return 1;
        }
        control_request_t req;
        memset(&req, 0, sizeof(req));
        req.kind = CMD_START;
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
        strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
        strncpy(req.command, argv[4], sizeof(req.command) - 1);
        
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
        
        if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
        return send_control_request(&req);
    }

    if (strcmp(argv[1], "run") == 0) {
        if (argc < 5) {
            usage(argv[0]);
            return 1;
        }
        control_request_t req;
        memset(&req, 0, sizeof(req));
        req.kind = CMD_RUN;
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
        strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
        strncpy(req.command, argv[4], sizeof(req.command) - 1);
        
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
        
        if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
        return send_control_request(&req);
    }
    
    if (strcmp(argv[1], "ps") == 0) {
        control_request_t req;
        memset(&req, 0, sizeof(req));
        req.kind = CMD_PS;
        return send_control_request(&req);
    }
    
    if (strcmp(argv[1], "stop") == 0) {
        if (argc < 3) {
            usage(argv[0]);
            return 1;
        }
        control_request_t req;
        memset(&req, 0, sizeof(req));
        req.kind = CMD_STOP;
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
        return send_control_request(&req);
    }
    
    if (strcmp(argv[1], "logs") == 0) {
        if (argc < 3) {
            usage(argv[0]);
            return 1;
        }
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, argv[2]);
        char cmd[PATH_MAX + 10];
        snprintf(cmd, sizeof(cmd), "cat %s", log_path);
        system(cmd);
        return 0;
    }
    
    usage(argv[0]);
    return 1;
}
