/**
 * @file test_queue.c
 * @brief Unit tests for thread-safe queue implementation
 *
 * @details
 * This file contains unit tests for the thread-safe blocking queue
 * implementation (queue.c/queue.h). Tests cover:
 *   - Lifecycle management (creation, deletion)
 *   - Error handling (NULL parameters)
 *   - FIFO ordering guarantees
 *   - Circular buffer wrap-around behavior
 *   - Basic producer-consumer concurrency
 *
 * @note Uses Unity testing framework (ThrowTheSwitch/Unity)
 * @see https://github.com/ThrowTheSwitch/Unity
 *
 * Build: make test
 * Run:   ./test_queue
 */

#include "unity/unity.h"
#include "queue.h"

#include <pthread.h>
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
    /* No per-test setup required */
}

/**
 * @brief Called after each test function
 */
void tearDown(void) {
    /* No per-test cleanup required */
}

/*============================================================================*/
/* Lifecycle Tests                                                            */
/*============================================================================*/

/**
 * @brief Verify queue_new() returns a valid pointer
 *
 * Tests that allocating a queue with a reasonable size succeeds.
 */
void test_queue_new_returns_non_null(void) {
    /* Arrange & Act */
    queue_t *q = queue_new(10);

    /* Assert */
    TEST_ASSERT_NOT_NULL(q);

    /* Cleanup */
    queue_delete(&q);
}

/**
 * @brief Verify queue_delete() sets the pointer to NULL
 *
 * prevents dangling pointer bugs after deletion.
 */
void test_queue_delete_sets_null(void) {
    /* Arrange */
    queue_t *q = queue_new(5);

    /* Act */
    queue_delete(&q);

    /* Assert */
    TEST_ASSERT_NULL(q);
}

/**
 * @brief Verify queue_delete() handles NULL safely
 *
 * Passing NULL should not cause a crash or undefined behavior.
 */
void test_queue_delete_null_safe(void) {
    /* Arrange */
    queue_t *q = NULL;

    /* Act - should not crash */
    queue_delete(&q);
    queue_delete(NULL);

    /* Assert - reaching here means success */
    TEST_PASS();
}

/*============================================================================*/
/* Error Handling Tests                                                       */
/*============================================================================*/

/**
 * @brief Verify queue_push() returns false for NULL queue
 *
 * Error case: pushing to a non-existent queue.
 */
void test_queue_push_null_queue_fails(void) {
    /* Arrange */
    int val = 42;

    /* Act & Assert */
    TEST_ASSERT_FALSE(queue_push(NULL, &val));
}

/**
 * @brief Verify queue_pop() returns false for invalid parameters
 *
 * Tests two error cases:
 *   1. NULL queue pointer
 *   2. NULL element output pointer
 */
void test_queue_pop_null_fails(void) {
    void *elem;

    /* Case 1: NULL queue */
    TEST_ASSERT_FALSE(queue_pop(NULL, &elem));

    /* Case 2: NULL element pointer */
    queue_t *q = queue_new(5);
    int val = 1;
    queue_push(q, &val);
    TEST_ASSERT_FALSE(queue_pop(q, NULL));

    /* Cleanup */
    queue_pop(q, &elem);
    queue_delete(&q);
}

/*============================================================================*/
/* FIFO Ordering Tests                                                        */
/*============================================================================*/

/**
 * @brief Verify queue maintains FIFO (First-In-First-Out) order
 *
 * Push elements A, B, C and verify they pop in the same order.
 */
void test_queue_fifo_order(void) {
    /* Arrange */
    queue_t *q = queue_new(10);
    int vals[] = {1, 2, 3, 4, 5};
    const int count = 5;

    /* Act - push all elements */
    for (int i = 0; i < count; i++) {
        queue_push(q, &vals[i]);
    }

    /* Assert - verify FIFO order on pop */
    for (int i = 0; i < count; i++) {
        void *elem;
        queue_pop(q, &elem);
        TEST_ASSERT_EQUAL_PTR(&vals[i], elem);
    }

    /* Cleanup */
    queue_delete(&q);
}

/**
 * @brief Verify circular buffer handles wrap-around correctly
 *
 * Exercises the modulo arithmetic for head/tail indices:
 *   1. Fill the queue
 *   2. Partially drain it
 *   3. Refill (causes indices to wrap)
 *   4. Verify correct ordering
 */
void test_queue_wraparound(void) {
    /* Arrange */
    queue_t *q = queue_new(3);
    int v1 = 1, v2 = 2, v3 = 3, v4 = 4, v5 = 5;
    void *result;

    /* Act - fill queue to capacity */
    queue_push(q, &v1);
    queue_push(q, &v2);
    queue_push(q, &v3);

    /* Act - partial drain (moves head index) */
    queue_pop(q, &result);
    queue_pop(q, &result);

    /* Act - refill (tail index wraps around) */
    queue_push(q, &v4);
    queue_push(q, &v5);

    /* Assert - remaining elements in correct order: v3, v4, v5 */
    queue_pop(q, &result);
    TEST_ASSERT_EQUAL_PTR(&v3, result);
    queue_pop(q, &result);
    TEST_ASSERT_EQUAL_PTR(&v4, result);
    queue_pop(q, &result);
    TEST_ASSERT_EQUAL_PTR(&v5, result);

    /* Cleanup */
    queue_delete(&q);
}

/*============================================================================*/
/* Concurrency Tests                                                          */
/*============================================================================*/

/** Number of items for producer-consumer test */
#define ITEM_COUNT 100

/**
 * @brief Thread arguments for producer/consumer functions
 */
typedef struct {
    queue_t *queue;  /**< Shared queue */
    int *values;     /**< Array of values to produce/consume */
} thread_args_t;

/**
 * @brief Producer thread function
 *
 * Pushes ITEM_COUNT integers onto the queue sequentially.
 *
 * @param arg Pointer to thread_args_t
 * @return NULL
 */
static void *producer_func(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;

    for (int i = 0; i < ITEM_COUNT; i++) {
        queue_push(args->queue, &args->values[i]);
    }

    return NULL;
}

/**
 * @brief Consumer thread function
 *
 * Pops ITEM_COUNT integers from the queue and stores their values.
 *
 * @param arg Pointer to thread_args_t
 * @return NULL
 */
static void *consumer_func(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;

    for (int i = 0; i < ITEM_COUNT; i++) {
        void *elem;
        queue_pop(args->queue, &elem);
        args->values[i] = *((int *)elem);
    }

    return NULL;
}

/**
 * @brief Verify thread-safe producer-consumer operation
 *
 * Creates one producer and one consumer thread operating on
 * a shared queue. Verifies:
 *   - All items are transferred
 *   - FIFO order is preserved
 *   - No data corruption or deadlocks
 */
void test_queue_producer_consumer(void) {
    /* Arrange */
    queue_t *q = queue_new(10);

    int produced[ITEM_COUNT];
    int consumed[ITEM_COUNT];
    for (int i = 0; i < ITEM_COUNT; i++) {
        produced[i] = i;
        consumed[i] = -1;  /* Sentinel value */
    }

    thread_args_t prod_args = {.queue = q, .values = produced};
    thread_args_t cons_args = {.queue = q, .values = consumed};

    /* Act */
    pthread_t producer, consumer;
    pthread_create(&producer, NULL, producer_func, &prod_args);
    pthread_create(&consumer, NULL, consumer_func, &cons_args);

    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);

    /* Assert - FIFO order preserved with single producer/consumer */
    for (int i = 0; i < ITEM_COUNT; i++) {
        TEST_ASSERT_EQUAL_INT(i, consumed[i]);
    }

    /* Cleanup */
    queue_delete(&q);
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
    RUN_TEST(test_queue_new_returns_non_null);
    RUN_TEST(test_queue_delete_sets_null);
    RUN_TEST(test_queue_delete_null_safe);

    /* Error handling tests */
    RUN_TEST(test_queue_push_null_queue_fails);
    RUN_TEST(test_queue_pop_null_fails);

    /* FIFO ordering tests */
    RUN_TEST(test_queue_fifo_order);
    RUN_TEST(test_queue_wraparound);

    /* Concurrency tests */
    RUN_TEST(test_queue_producer_consumer);

    return UNITY_END();
}
