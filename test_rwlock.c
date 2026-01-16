/**
 * @file test_rwlock.c
 * @brief Unit tests for reader-writer lock implementation
 * @author Luke R.
 * @date 2025
 *
 * @details
 * This file contains unit tests for reader-writer lock
 * (rwlock.c/rwlock.h). Tests cover:
 *   - Lifecycle management (creation with all priority modes, deletion)
 *   - Basic locking operations (single reader, single writer)
 *   - Concurrent reader access (multiple readers simultaneously)
 *   - Writer exclusivity (no concurrent readers during write)
 *
 * @note Uses Unity testing framework (ThrowTheSwitch/Unity)
 * @see https://github.com/ThrowTheSwitch/Unity
 *
 * Build: make test
 * Run:   ./test_rwlock
 */

#include "unity/unity.h"
#include "rwlock.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>

/*============================================================================*/
/* Unity Required Functions                                                   */
/*============================================================================*/

/**
 * @brief Called before each test function
 */
void setUp(void) {
    /* No per-test setup required NA */
}

/**
 * @brief Called after each test function
 */
void tearDown(void) {
    /* No per-test cleanup required NA */
}

/*============================================================================*/
/* Lifecycle Tests                                                            */
/*============================================================================*/

/**
 * @brief Verify rwlock_new() works for all priority modes
 *
 * Tests creation with READERS, WRITERS, and N_WAY priority.
 * Expected: All modes should return valid non-NULL pointers.
 */
void test_rwlock_new_all_priorities(void) {
    /* Arrange & Act */
    rwlock_t *r1 = rwlock_new(READERS, 0);
    rwlock_t *r2 = rwlock_new(WRITERS, 0);
    rwlock_t *r3 = rwlock_new(N_WAY, 5);

    /* Assert */
    TEST_ASSERT_NOT_NULL(r1);
    TEST_ASSERT_NOT_NULL(r2);
    TEST_ASSERT_NOT_NULL(r3);

    /* Cleanup */
    rwlock_delete(&r1);
    rwlock_delete(&r2);
    rwlock_delete(&r3);
}

/**
 * @brief Verify rwlock_delete() sets the pointer to NULL
 *
 * This prevents dangling pointer bugs after deletion.
 */
void test_rwlock_delete_sets_null(void) {
    /* Arrange */
    rwlock_t *rw = rwlock_new(READERS, 0);

    /* Act */
    rwlock_delete(&rw);

    /* Assert */
    TEST_ASSERT_NULL(rw);
}

/**
 * @brief Verify rwlock_delete() handles NULL safely
 *
 * Passing NULL should not cause a crash or undefined behavior.
 */
void test_rwlock_delete_null_safe(void) {
    /* Arrange */
    rwlock_t *rw = NULL;

    /* Act - should not crash */
    rwlock_delete(&rw);
    rwlock_delete(NULL);

    /* Assert - reaching here means success */
    TEST_PASS();
}

/*============================================================================*/
/* Basic Locking Tests                                                        */
/*============================================================================*/

/**
 * @brief Verify single reader can acquire and release lock
 *
 * Basic sanity check that reader_lock/reader_unlock don't deadlock.
 */
void test_rwlock_single_reader(void) {
    /* Arrange */
    rwlock_t *rw = rwlock_new(READERS, 0);

    /* Act */
    reader_lock(rw);
    reader_unlock(rw);

    /* Cleanup */
    rwlock_delete(&rw);

    /* Assert - reaching here means no deadlock */
    TEST_PASS();
}

/**
 * @brief Verify single writer can acquire and release lock
 *
 * Basic sanity check that writer_lock/writer_unlock don't deadlock.
 */
void test_rwlock_single_writer(void) {
    /* Arrange */
    rwlock_t *rw = rwlock_new(READERS, 0);

    /* Act */
    writer_lock(rw);
    writer_unlock(rw);

    /* Cleanup */
    rwlock_delete(&rw);

    /* Assert - reaching here means no deadlock */
    TEST_PASS();
}

/*============================================================================*/
/* Concurrency Tests                                                          */
/*============================================================================*/

/**
 * @brief Thread arguments for concurrent reader test
 */
typedef struct {
    rwlock_t *rwlock;           /**< Shared rwlock */
    atomic_int *active_readers; /**< Count of currently active readers */
    atomic_int *max_concurrent; /**< Maximum concurrent readers observed */
} reader_args_t;

/**
 * @brief Reader thread function for concurrency testing
 *
 * Acquires reader lock, tracks concurrent readers, holds briefly,
 * then releases.
 *
 * @param arg Pointer to reader_args_t
 * @return NULL
 */
static void *concurrent_reader(void *arg) {
    reader_args_t *args = (reader_args_t *)arg;

    reader_lock(args->rwlock);

    /* Track concurrent reader count */
    int current = atomic_fetch_add(args->active_readers, 1) + 1;

    /* Update max concurrent if this is a new high */
    int max = atomic_load(args->max_concurrent);
    while (current > max) {
        atomic_compare_exchange_weak(args->max_concurrent, &max, current);
    }

    usleep(5000);  /* Hold lock briefly to allow overlap */

    atomic_fetch_sub(args->active_readers, 1);
    reader_unlock(args->rwlock);

    return NULL;
}

/**
 * @brief Verify multiple readers can hold lock simultaneously
 *
 * Creates multiple reader threads and verifies that more than one
 * was active at the same time (readers don't block each other).
 */
void test_rwlock_multiple_readers(void) {
    /* Arrange */
    rwlock_t *rw = rwlock_new(READERS, 0);
    atomic_int active = 0;
    atomic_int max_concurrent = 0;

    reader_args_t args = {
        .rwlock = rw,
        .active_readers = &active,
        .max_concurrent = &max_concurrent
    };

    /* Act - spawn multiple reader threads */
    pthread_t threads[3];
    for (int i = 0; i < 3; i++) {
        pthread_create(&threads[i], NULL, concurrent_reader, &args);
    }
    for (int i = 0; i < 3; i++) {
        pthread_join(threads[i], NULL);
    }

    /* Assert - multiple readers should have been active simultaneously */
    TEST_ASSERT_GREATER_THAN(1, atomic_load(&max_concurrent));

    /* Cleanup */
    rwlock_delete(&rw);
}

/**
 * @brief Thread arguments for reader-writer exclusivity test
 */
typedef struct {
    rwlock_t *rwlock;        /**< Shared rwlock */
    atomic_int *shared_data; /**< Shared data modified by writer */
    atomic_int *violations;  /**< Count of exclusivity violations */
    int iterations;          /**< Number of lock/unlock cycles */
} rw_args_t;

/**
 * @brief Writer thread function for exclusivity testing
 *
 * Acquires writer lock, modifies shared data in a detectable pattern
 * (odd then even), then releases. If readers see odd values, the
 * exclusivity invariant is violated.
 *
 * @param arg Pointer to rw_args_t
 * @return NULL
 */
static void *writer_thread(void *arg) {
    rw_args_t *args = (rw_args_t *)arg;

    for (int i = 0; i < args->iterations; i++) {
        writer_lock(args->rwlock);

        /* Set to odd value (inconsistent state) */
        atomic_fetch_add(args->shared_data, 1);

        /* Brief delay to widen race window if lock is broken */
        for (volatile int j = 0; j < 50; j++);

        /* Set back to even value (consistent state) */
        atomic_fetch_add(args->shared_data, 1);

        writer_unlock(args->rwlock);
    }

    return NULL;
}

/**
 * @brief Reader thread function for exclusivity testing
 *
 * Acquires reader lock and checks if shared data is in an inconsistent
 * state (odd value). If so, increments violation counter.
 *
 * @param arg Pointer to rw_args_t
 * @return NULL
 */
static void *reader_thread(void *arg) {
    rw_args_t *args = (rw_args_t *)arg;

    for (int i = 0; i < args->iterations; i++) {
        reader_lock(args->rwlock);

        /* Readers should only see even (consistent) values */
        if (atomic_load(args->shared_data) % 2 != 0) {
            atomic_fetch_add(args->violations, 1);
        }

        reader_unlock(args->rwlock);
    }

    return NULL;
}

/**
 * @brief Verify writer has exclusive access (no concurrent readers)
 *
 * Creates one writer and multiple readers operating concurrently.
 * The writer modifies shared data in a pattern that would be detected
 * if readers could access during the write operation.
 *
 * Invariant: Readers should never see an odd (mid-write) value.
 */
void test_rwlock_writer_exclusive(void) {
    /* Arrange */
    rwlock_t *rw = rwlock_new(READERS, 0);
    atomic_int shared = 0;
    atomic_int violations = 0;

    rw_args_t args = {
        .rwlock = rw,
        .shared_data = &shared,
        .violations = &violations,
        .iterations = 100
    };

    /* Act - spawn writer and multiple readers */
    pthread_t writer, readers[3];
    pthread_create(&writer, NULL, writer_thread, &args);
    for (int i = 0; i < 3; i++) {
        pthread_create(&readers[i], NULL, reader_thread, &args);
    }

    pthread_join(writer, NULL);
    for (int i = 0; i < 3; i++) {
        pthread_join(readers[i], NULL);
    }

    /* Assert - no reader should have seen an odd (mid-write) value */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&violations));

    /* Assert - final value should be even (each write adds 2) */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&shared) % 2);

    /* Cleanup */
    rwlock_delete(&rw);
}

/*============================================================================*/
/* Test Runner                                                                */
/*============================================================================*/

/**
 * @brief Main entry point for test execution
 *
 * @return 0 if all tests pass, non-zero otherwise
 */
int main(void) {
    UNITY_BEGIN();

    /* Lifecycle tests */
    RUN_TEST(test_rwlock_new_all_priorities);
    RUN_TEST(test_rwlock_delete_sets_null);
    RUN_TEST(test_rwlock_delete_null_safe);

    /* Basic locking tests */
    RUN_TEST(test_rwlock_single_reader);
    RUN_TEST(test_rwlock_single_writer);

    /* Concurrency tests */
    RUN_TEST(test_rwlock_multiple_readers);
    RUN_TEST(test_rwlock_writer_exclusive);

    return UNITY_END();
}
