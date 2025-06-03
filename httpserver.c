#include "asgn2_helper_funcs.h"
#include "debug.h"
#include "queue.h"
#include "rwlock.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <regex.h>

#define BUFFERSIZE           2049
#define DEFAULT_THREAD_COUNT 4

typedef struct Request {
    int infd;
    char *cmd;
    char *targetPath;
    char *version;
    int contentLength;
    char *msgBody;
    int remainingBytes;
    char *requestId;
} Request;

struct file_lock {
    rwlock_t *lock;
    char *filename;
    int users;
    pthread_mutex_t mutex; // New mutex for each file lock
};

typedef struct {
    queue_t *queue;
    pthread_mutex_t file_locks_mutex;
    struct file_lock *file_locks;
    int thread_count;
} ServerContext;

ServerContext server_context;

pthread_mutex_t audit_log_mutex = PTHREAD_MUTEX_INITIALIZER;

void sendResponse(int clientfd, int statusCode, const char *message) {
    char response[BUFFERSIZE];
    snprintf(response, BUFFERSIZE, "HTTP/1.1 %d %s\r\nContent-Length: %lu\r\n\r\n%s\n", statusCode,
        message, strlen(message), message);
    write_n_bytes(clientfd, response, strlen(response));
}

void writeAuditLog(const char *operation, const char *uri, int statusCode, const char *requestId) {
    pthread_mutex_lock(&audit_log_mutex); // Acquire lock before writing
    fprintf(stderr, "%s,/%s,%d,%s\n", operation, uri, statusCode, requestId ? requestId : "0");
    pthread_mutex_unlock(&audit_log_mutex); // Release lock after writing
}

int parseRequest(Request *req) {

    char buffer[BUFFERSIZE];
    ssize_t bytesRead = read_until(req->infd, buffer, BUFFERSIZE - 1, "\r\n\r\n");
    if (bytesRead == -1) {
        sendResponse(req->infd, 400, "Bad Request");
        return -1;
    }
    buffer[bytesRead] = '\0'; // Add null terminator

    regex_t requestLineRegex;
    regex_t headerFieldRegex;

    regmatch_t matches[4];

    int rc = regcomp(&requestLineRegex, REQUEST_LINE_REGEX, REG_EXTENDED);
    if (rc != 0) {
        sendResponse(req->infd, 500, "Internal Server Error");
        return -1;
    }

    rc = regexec(&requestLineRegex, buffer, 4, matches, 0);
    if (rc != 0) {
        sendResponse(req->infd, 400, "Bad Request");
        regfree(&requestLineRegex);
        return -1;
    }

    req->cmd = strndup(buffer + matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so);
    req->targetPath = strndup(buffer + matches[2].rm_so, matches[2].rm_eo - matches[2].rm_so);
    req->version = strndup(buffer + matches[3].rm_so, matches[3].rm_eo - matches[3].rm_so);

    regfree(&requestLineRegex);

    // Check if the HTTP method is supported
    if (strncmp(req->cmd, "GET", 3) != 0 && strncmp(req->cmd, "PUT", 3) != 0) {
        sendResponse(req->infd, 501, "Not Implemented");
        return -1;
    }

    // Check the HTTP version
    if (strncmp(req->version, "HTTP/1.1", 8) != 0) {
        sendResponse(req->infd, 505, "Version Not Supported");
        return -1;
    }

    char *headerStart = strstr(buffer, "\r\n") + 2;
    char *headerEnd = strstr(headerStart, "\r\n\r\n");
    if (headerEnd == NULL) {
        sendResponse(req->infd, 400, "Bad Request");
        return -1;
    }

    req->contentLength = -1;
    req->requestId = NULL;

    rc = regcomp(&headerFieldRegex, HEADER_FIELD_REGEX, REG_EXTENDED);
    if (rc != 0) {
        sendResponse(req->infd, 500, "Internal Server Error");
        return -1;
    }

    char *currentHeader = headerStart;
    while (currentHeader < headerEnd) {
        rc = regexec(&headerFieldRegex, currentHeader, 3, matches, 0);
        if (rc == 0) {
            char *key
                = strndup(currentHeader + matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so);
            char *value
                = strndup(currentHeader + matches[2].rm_so, matches[2].rm_eo - matches[2].rm_so);

            if (strcmp(key, "Content-Length") == 0) {
                req->contentLength = atoi(value);
            } else if (strcmp(key, "Request-Id") == 0) {
                req->requestId = strdup(value);
            }

            free(key);
            free(value);

            currentHeader += matches[0].rm_eo;
        } else {
            sendResponse(req->infd, 400, "Bad Request");
            regfree(&headerFieldRegex);
            return -1;
        }
    }

    regfree(&headerFieldRegex);

    if (headerEnd + 4 < buffer + bytesRead) {
        req->msgBody = headerEnd + 4;
        req->remainingBytes = bytesRead - (headerEnd + 4 - buffer);
    } else {
        req->msgBody = NULL;
        req->remainingBytes = 0;
    }

    return 0;
}

static struct file_lock *find_file_lock(const char *URI) {
    pthread_mutex_lock(&server_context.file_locks_mutex);

    for (int i = 0; i < server_context.thread_count; i++) {
        pthread_mutex_lock(&server_context.file_locks[i].mutex); // Lock individual file lock

        if (server_context.file_locks[i].filename == NULL) {
            // Initialize new file lock
            server_context.file_locks[i].lock = rwlock_new(N_WAY, 1);
            server_context.file_locks[i].filename = strdup(URI);
            server_context.file_locks[i].users = 1;
            pthread_mutex_unlock(&server_context.file_locks[i].mutex);
            pthread_mutex_unlock(&server_context.file_locks_mutex);
            return &server_context.file_locks[i];
        } else if (strcmp(server_context.file_locks[i].filename, URI) == 0) {
            // Increment users for existing lock
            server_context.file_locks[i].users++;
            pthread_mutex_unlock(&server_context.file_locks[i].mutex);
            pthread_mutex_unlock(&server_context.file_locks_mutex);
            return &server_context.file_locks[i];
        }

        pthread_mutex_unlock(&server_context.file_locks[i].mutex); // Unlock if not used
    }

    pthread_mutex_unlock(&server_context.file_locks_mutex);
    return NULL; // No available lock found
}

static void release_file_lock(struct file_lock *lock) {
    pthread_mutex_lock(&server_context.file_locks_mutex);
    if (--lock->users == 0) {
        rwlock_delete(&lock->lock);
        free(lock->filename);
        lock->filename = NULL;
    }
    pthread_mutex_unlock(&server_context.file_locks_mutex);
}

int handleGet(Request *req) {
    const char *uri = req->targetPath;
    struct file_lock *lock = find_file_lock(uri);
    if (lock == NULL) {
        writeAuditLog("GET", uri, 500, req->requestId);
        sendResponse(req->infd, 500, "Internal Server Error");
        return -1;
    }

    reader_lock(lock->lock);

    // Open file
    int fd = open(uri, O_RDONLY);
    if (fd == -1) {
        reader_unlock(lock->lock);
        release_file_lock(lock);
        if (errno == ENOENT) {
            writeAuditLog("GET", uri, 404, req->requestId);
            sendResponse(req->infd, 404, "Not Found");
        } else if (errno == EACCES) {
            writeAuditLog("GET", uri, 403, req->requestId);
            sendResponse(req->infd, 403, "Forbidden");
        } else {
            writeAuditLog("GET", uri, 500, req->requestId);
            sendResponse(req->infd, 500, "Internal Server Error");
        }
        return -1;
    }

    // Get file stats
    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        reader_unlock(lock->lock);
        release_file_lock(lock);
        writeAuditLog("GET", uri, 500, req->requestId);
        sendResponse(req->infd, 500, "Internal Server Error");
        return -1;
    }

    // Check if the target path is a directory
    if (S_ISDIR(st.st_mode)) {
        close(fd);
        reader_unlock(lock->lock);
        release_file_lock(lock);
        writeAuditLog("GET", uri, 403, req->requestId);
        sendResponse(req->infd, 403, "Forbidden");
        return -1;
    }

    // Get file size
    off_t size = st.st_size;

    // Send response headers
    char headers[BUFFERSIZE];
    snprintf(headers, BUFFERSIZE, "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n", size);
    write_n_bytes(req->infd, headers, strlen(headers));

    // Send file content
    off_t offset = 0;
    while (offset < size) {
        char buffer[BUFFERSIZE];
        ssize_t bytesRead = pread(fd, buffer, BUFFERSIZE, offset);
        if (bytesRead == -1) {
            close(fd);
            reader_unlock(lock->lock);
            release_file_lock(lock);
            writeAuditLog("GET", uri, 500, req->requestId);
            sendResponse(req->infd, 500, "Internal Server Error");
            return -1;
        }
        write_n_bytes(req->infd, buffer, bytesRead);
        offset += bytesRead;
    }

    close(fd);
    reader_unlock(lock->lock);
    release_file_lock(lock);
    writeAuditLog("GET", uri, 200, req->requestId);

    return 0;
}

int handlePut(Request *req) {
    const char *uri = req->targetPath;
    struct file_lock *lock = find_file_lock(uri);
    if (lock == NULL) {
        writeAuditLog("PUT", uri, 500, req->requestId);
        sendResponse(req->infd, 500, "Internal Server Error");
        return -1;
    }

    writer_lock(lock->lock); // Acquire writer lock

    if (req->contentLength == -1) {
        writer_unlock(lock->lock); // Release lock on error
        release_file_lock(lock);
        writeAuditLog("PUT", uri, 400, req->requestId);
        sendResponse(req->infd, 400, "Bad Request");
        return -1;
    }

    struct stat st;
    int fileExists = (stat(uri, &st) == 0);

    if (fileExists && S_ISDIR(st.st_mode)) {
        writer_unlock(lock->lock); // Release lock on error
        release_file_lock(lock);
        writeAuditLog("PUT", uri, 403, req->requestId);
        sendResponse(req->infd, 403, "Forbidden");
        return -1;
    }

    int fd = open(uri, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd == -1) {
        writer_unlock(lock->lock); // Release lock on error
        release_file_lock(lock);
        if (errno == EACCES) {
            writeAuditLog("PUT", uri, 403, req->requestId);
            sendResponse(req->infd, 403, "Forbidden");
        } else {
            writeAuditLog("PUT", uri, 500, req->requestId);
            sendResponse(req->infd, 500, "Internal Server Error");
        }
        return -1;
    }

    // Write initial data from the request body
    ssize_t bytesWritten = write_n_bytes(fd, req->msgBody, req->remainingBytes);
    if (bytesWritten == -1) {
        // ... (error handling: close, unlock, release, unlink, audit, response)
    }

    // Handle the remaining data if any
    if (bytesWritten < req->contentLength) {
        ssize_t remainingBytes = req->contentLength - bytesWritten;
        char buffer[BUFFERSIZE];
        while (remainingBytes > 0) {
            ssize_t bytesRead = read_n_bytes(req->infd, buffer, BUFFERSIZE);
            if (bytesRead == -1) {
                // ... (error handling: close, unlock, release, unlink, audit, response)
            }
            ssize_t bytesWritten = write_n_bytes(fd, buffer, bytesRead);
            if (bytesWritten == -1) {
                // ... (error handling: close, unlock, release, unlink, audit, response)
            }
            remainingBytes -= bytesWritten;
        }
    }

    close(fd);
    writer_unlock(lock->lock); // Release writer lock
    release_file_lock(lock);

    if (fileExists) {
        writeAuditLog("PUT", uri, 200, req->requestId);
        sendResponse(req->infd, 200, "OK");
    } else {
        writeAuditLog("PUT", uri, 201, req->requestId);
        sendResponse(req->infd, 201, "Created");
    }

    return 0;
}

void *workerThread() {
    while (1) {
        Request *req;
        queue_pop(server_context.queue, (void **) &req);
        if (req == NULL) {
            break;
        }

        if (parseRequest(req) != 0) {
            sendResponse(req->infd, 400, "Bad Request");
            close(req->infd);
            free(req->cmd);
            free(req->targetPath);
            free(req->version);
            free(req->requestId);
            free(req);
            continue;
        }

        if (strcmp(req->cmd, "GET") == 0) {
            handleGet(req);
        } else if (strcmp(req->cmd, "PUT") == 0) {
            handlePut(req);
        } else {
            writeAuditLog(req->cmd, req->targetPath, 501, req->requestId);
            sendResponse(req->infd, 501, "Not Implemented");
        }

        close(req->infd);
        free(req->cmd);
        free(req->targetPath);
        free(req->version);
        free(req->requestId);
        free(req);
    }

    return NULL;
}

void sigintHandler() {
    // Gracefully shutdown the server
    pthread_mutex_lock(&server_context.file_locks_mutex);
    for (int i = 0; i < server_context.thread_count; i++) {
        if (server_context.file_locks[i].filename != NULL) {
            rwlock_delete(&server_context.file_locks[i].lock);
            free(server_context.file_locks[i].filename);
            pthread_mutex_destroy(&server_context.file_locks[i].mutex); // Destroy mutex
        }
    }
    pthread_mutex_unlock(&server_context.file_locks_mutex);
    pthread_mutex_destroy(&server_context.file_locks_mutex);

    queue_delete(&server_context.queue);

    exit(0);
}

int main(int argc, char *argv[]) {
    int port = 0;
    int threadCount = DEFAULT_THREAD_COUNT;

    int opt;
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
        case 't': threadCount = atoi(optarg); break;
        default: fprintf(stderr, "Usage: %s [-t threads] <port>\n", argv[0]); exit(EXIT_FAILURE);
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "Usage: %s [-t threads] <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    port = atoi(argv[optind]);

    if (port < 1 || port > 65535) {
        fprintf(stderr, "Invalid port: %d\n", port);
        exit(EXIT_FAILURE);
    }

    signal(SIGINT, sigintHandler);
    signal(SIGTERM, sigintHandler);

    server_context.queue = queue_new(threadCount);
    pthread_mutex_init(&server_context.file_locks_mutex, NULL);
    server_context.file_locks = malloc(threadCount * sizeof(struct file_lock));
    memset(server_context.file_locks, 0, threadCount * sizeof(struct file_lock));
    server_context.thread_count = threadCount;

    pthread_t *threads = malloc(threadCount * sizeof(pthread_t));
    for (int i = 0; i < threadCount; i++) {
        pthread_create(&threads[i], NULL, workerThread, NULL);
    }

    Listener_Socket sock;
    if (listener_init(&sock, port) == -1) {
        fprintf(stderr, "Failed to listen on port %d\n", port);
        exit(EXIT_FAILURE);
    }

    while (1) {
        int clientfd = listener_accept(&sock);
        if (clientfd == -1) {
            fprintf(stderr, "Failed to accept connection\n");
            continue;
        }

        Request *req = malloc(sizeof(Request));
        req->infd = clientfd;
        req->cmd = NULL;
        req->targetPath = NULL;
        req->version = NULL;
        req->contentLength = -1;
        req->msgBody = NULL;
        req->remainingBytes = 0;
        req->requestId = NULL;

        queue_push(server_context.queue, req);
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);

    return 0;
}
