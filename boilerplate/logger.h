#ifndef LOGGER_H
#define LOGGER_H

#include "utils.h"
#include <pthread.h>
#include <stddef.h>

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

int bounded_buffer_init(bounded_buffer_t *buffer);
void bounded_buffer_destroy(bounded_buffer_t *buffer);
void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer);
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item);

void *logging_thread(void *arg);

#endif // LOGGER_H
