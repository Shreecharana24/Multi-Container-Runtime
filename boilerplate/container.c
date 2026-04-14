#define _GNU_SOURCE
#include "container.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/resource.h>

const char *state_to_string(container_state_t state) {
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

int child_fn(void *arg) {
    child_config_t *config = (child_config_t *)arg;

    // Redirect stdout and stderr to the log pipe FIRST
    // so any startup errors are correctly logged to the container's log file
    if (config->log_write_fd >= 0) {
        if (dup2(config->log_write_fd, STDOUT_FILENO) == -1) {
            return 1;
        }
        if (dup2(config->log_write_fd, STDERR_FILENO) == -1) {
            return 1;
        }
        close(config->log_write_fd);
    }

    // Set hostname
    if (sethostname(config->id, strlen(config->id)) != 0) {
        perror("sethostname failed");
        return 1;
    }

    // Change directory and chroot
    if (chdir(config->rootfs) != 0) {
        perror("chdir config->rootfs failed");
        return 1;
    }

    if (chroot(".") != 0) {
        perror("chroot . failed");
        return 1;
    }

    // Change directory to the new root
    if (chdir("/") != 0) {
        perror("chdir / failed");
        return 1;
    }

    // Mount /proc
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc failed");
        return 1;
    }

    // Execute command
    // We need to parse the command string into an argv array
    char *args[32];
    int count = 0;
    char *token = strtok(config->command, " ");
    while (token != NULL && count < 31) {
        args[count++] = token;
        token = strtok(NULL, " ");
    }
    args[count] = NULL;

    if (count == 0) {
        fprintf(stderr, "No command provided\n");
        return 1;
    }
    if (config->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, config->nice_value) != 0) {
            perror("setpriority failed");
        }
    }

    execvp(args[0], args);
    perror("execvp failed");
    return 1;
}
