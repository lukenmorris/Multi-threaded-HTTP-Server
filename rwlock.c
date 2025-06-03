#include <pthread.h>
#include <stdlib.h>
#include <semaphore.h>
#include "rwlock.h"
#include <stdio.h>

typedef struct rwlock {
    pthread_mutex_t lock;
    pthread_cond_t readers_proceed;
    pthread_cond_t writer_proceed;
    int active_readers;
    int waiting_readers;
    int waiting_writers;
    int active_writers;
    PRIORITY priority;
    int n_way;
    int n;
} rwlock_t;

// Create a new rwlock with a specified priority and n_way value
rwlock_t *rwlock_new(PRIORITY p, uint32_t n) {
    rwlock_t *rw = malloc(sizeof(rwlock_t));
    if (rw == NULL) {
        perror("Failed to allocate memory for rwlock.");
        return NULL;
    }

    // Initialize mutex and condition variables
    if (pthread_mutex_init(&rw->lock, NULL) != 0
        || pthread_cond_init(&rw->readers_proceed, NULL) != 0
        || pthread_cond_init(&rw->writer_proceed, NULL) != 0) {
        // If initialization fails, clean up and report error
        free(rw);
        perror("Failed to initialize rwlock components.");
        return NULL;
    }

    // Set initial state and configuration
    rw->active_readers = 0;
    rw->waiting_readers = 0;
    rw->active_writers = 0;
    rw->waiting_writers = 0;
    rw->priority = p;
    rw->n_way
        = n; // Assuming n_way is used similar to 'n' in the correct implementation for N_WAY priority
    rw->n = n; // If using a structure similar to the correct implementation

    return rw;
}

// Delete the rwlock and free its resources
void rwlock_delete(rwlock_t **l) {
    if (l == NULL || *l == NULL)
        return;

    pthread_mutex_destroy(&(*l)->lock);
    pthread_cond_destroy(&(*l)->readers_proceed);
    pthread_cond_destroy(&(*l)->writer_proceed);
    free(*l);
    *l = NULL;
}

// Acquire the lock for reading
void reader_lock(rwlock_t *rw) {
    pthread_mutex_lock(&rw->lock);

    while (rw->active_writers > 0 || (rw->priority == WRITERS && rw->waiting_writers > 0)
           || (rw->priority == N_WAY && rw->n_way <= 0 && rw->waiting_writers > 0)) {
        rw->waiting_readers++;
        pthread_cond_wait(&rw->readers_proceed, &rw->lock);
        rw->waiting_readers--;
    }
    rw->active_readers++;

    if (rw->priority == N_WAY) {
        rw->n_way--;
    }

    pthread_mutex_unlock(&rw->lock);
}

// Release the lock after reading
void reader_unlock(rwlock_t *rw) {
    pthread_mutex_lock(&rw->lock);

    rw->active_readers--;
    if (rw->active_readers == 0) {
        // If no more readers are active, check if there are waiting writers.
        // Signal to waiting writers if there are any, prioritizing writers over readers
        // when both are waiting and a writer could proceed next based on the priority policy.
        if (rw->waiting_writers > 0) {
            // In the case of WRITERS priority, signaling the writers_conditional (writer_proceed in your code)
            // allows a waiting writer to proceed as soon as all readers have finished.
            pthread_cond_signal(&rw->writer_proceed);
        } else if (rw->priority == N_WAY && rw->n_way <= 0 && rw->waiting_writers > 0) {
            // For N_WAY priority, if the read count since the last write has reached its limit (n_way <= 0),
            // and there are waiting writers, signal a writer to proceed.
            pthread_cond_signal(&rw->writer_proceed);
        }
    }

    pthread_mutex_unlock(&rw->lock);
}

// Acquire the lock for writing
void writer_lock(rwlock_t *rw) {
    pthread_mutex_lock(&rw->lock);

    while (rw->active_readers > 0 || rw->active_writers > 0) {
        rw->waiting_writers++;
        pthread_cond_wait(&rw->writer_proceed, &rw->lock);
        rw->waiting_writers--;
    }

    rw->active_writers = 1;

    // For N_WAY, we reset reads_since_last_write since a write is about to occur
    // This ensures that after a write, reads can occur up to n times before another write is allowed
    if (rw->priority == N_WAY) {
        rw->n_way
            = rw->n; // Reset the counter to allow N reads before another write, based on policy
    }

    pthread_mutex_unlock(&rw->lock);
}

// Release the lock after writing
void writer_unlock(rwlock_t *rw) {
    pthread_mutex_lock(&rw->lock);

    rw->active_writers = 0;
    rw->n_way = rw->n; // Reset the n_way counter based on the N_WAY policy

    // The signaling logic here should match the correct implementation's approach more closely
    if (rw->priority == READERS) {
        if (rw->waiting_readers > 0) {
            pthread_cond_broadcast(&rw->readers_proceed);
        } else if (rw->waiting_writers > 0) {
            pthread_cond_signal(&rw->writer_proceed);
        }
    } else if (rw->priority == WRITERS) {
        if (rw->waiting_writers > 0) {
            pthread_cond_signal(&rw->writer_proceed);
        } else if (rw->waiting_readers > 0) {
            pthread_cond_broadcast(&rw->readers_proceed);
        }
    } else if (rw->priority == N_WAY) {
        if (rw->waiting_writers > 0) {
            pthread_cond_signal(&rw->writer_proceed);
        } else if (rw->waiting_readers > 0) {
            pthread_cond_broadcast(&rw->readers_proceed);
        }
    }

    pthread_mutex_unlock(&rw->lock);
}
