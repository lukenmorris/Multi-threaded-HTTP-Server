#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>

typedef struct queue {
    void **elements; // Array of pointers to queue elements
    int capacity; // Maximum number of elements in the queue
    int size; // Current number of elements in the queue
    int head; // Index of the head of the queue
    int tail; // Index of the tail of the queue
    pthread_mutex_t lock; // Mutex for synchronizing access to the queue
    pthread_cond_t not_empty; // Condition variable to wait for when the queue is empty
    pthread_cond_t not_full; // Condition variable to wait for when the queue is full
} queue_t;

queue_t *queue_new(int size) {
    queue_t *q = (queue_t *) malloc(sizeof(queue_t));
    if (q == NULL) {
        return NULL; // Failed to allocate memory for the queue
    }

    q->elements = (void **) malloc(sizeof(void *) * size);
    if (q->elements == NULL) {
        free(q); // Clean up previously allocated memory
        return NULL; // Failed to allocate memory for the queue elements
    }

    q->capacity = size;
    q->size = 0;
    q->head = 0;
    q->tail = 0;

    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);

    return q;
}

void queue_delete(queue_t **q) {
    if (q == NULL || *q == NULL) {
        return; // Null check to prevent dereferencing null pointer
    }

    pthread_mutex_destroy(&(*q)->lock); // Destroy the mutex
    pthread_cond_destroy(&(*q)->not_empty); // Destroy the not_empty condition variable
    pthread_cond_destroy(&(*q)->not_full); // Destroy the not_full condition variable

    free((*q)->elements); // Free the memory allocated for the elements array
    free(*q); // Free the queue itself
    *q = NULL; // Set the queue pointer to NULL to avoid dangling pointers
}

bool queue_push(queue_t *q, void *elem) {
    if (q == NULL) {
        return false; // Check for NULL queue
    }

    pthread_mutex_lock(&q->lock); // Lock the queue for exclusive access

    while (q->size == q->capacity) {
        // Wait until there is space in the queue
        pthread_cond_wait(&q->not_full, &q->lock);
    }

    // Add the element to the queue
    q->elements[q->tail] = elem;
    q->tail = (q->tail + 1) % q->capacity; // Circular buffer
    q->size++;

    // Signal that the queue is not empty (for consumers waiting to pop)
    pthread_cond_signal(&q->not_empty);

    pthread_mutex_unlock(&q->lock); // Unlock the queue

    return true;
}

bool queue_pop(queue_t *q, void **elem) {
    if (q == NULL || elem == NULL) {
        return false; // Check for NULL pointers
    }

    pthread_mutex_lock(&q->lock); // Lock the queue for exclusive access

    while (q->size == 0) {
        // Wait until the queue is not empty
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    // Remove the element from the queue
    *elem = q->elements[q->head];
    q->head = (q->head + 1) % q->capacity; // Circular buffer
    q->size--;

    // Signal that the queue is not full (for producers waiting to push)
    pthread_cond_signal(&q->not_full);

    pthread_mutex_unlock(&q->lock); // Unlock the queue

    return true;
}
