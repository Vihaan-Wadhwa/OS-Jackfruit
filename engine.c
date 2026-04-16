/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <sys/resource.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#ifndef CLONE_NEWPID
#define CLONE_NEWPID 0
#endif

#ifndef CLONE_NEWUTS
#define CLONE_NEWUTS 0
#endif

#ifndef CLONE_NEWNS
#define CLONE_NEWNS 0
#endif

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int notify_fd;
    int log_read_fd;
    pthread_t producer_thread;
    int producer_started;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

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

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int log_read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_args_t;

static volatile sig_atomic_t g_should_stop = 0;
static volatile sig_atomic_t g_got_sigchld = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
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

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 1;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;

    for (;;) {
        log_item_t item;
        char log_path[PATH_MAX];
        int fd;
        ssize_t written = 0;

        if (bounded_buffer_pop(&ctx->log_buffer, &item) == 0)
            break;

        log_path[0] = '\0';
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *cur = ctx->containers; cur; cur = cur->next) {
            if (strncmp(cur->id, item.container_id, sizeof(cur->id)) == 0) {
                strncpy(log_path, cur->log_path, sizeof(log_path) - 1);
                log_path[sizeof(log_path) - 1] = '\0';
                break;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0')
            continue;

        fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        while (written < (ssize_t)item.length) {
            ssize_t rc = write(fd, item.data + written, item.length - (size_t)written);
            if (rc < 0) {
                if (errno == EINTR)
                    continue;
                break;
            }
            written += rc;
        }

        close(fd);
    }

    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *config = arg;

    if (sethostname(config->id, strlen(config->id)) != 0)
        perror("sethostname");

    if (config->nice_value != 0)
        setpriority(PRIO_PROCESS, 0, config->nice_value);

    if (dup2(config->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(config->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        _exit(1);
    }
    close(config->log_write_fd);

#if defined(__linux__)
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0)
        perror("mount private");
#endif

    if (chroot(config->rootfs) != 0 || chdir("/") != 0) {
        perror("chroot/chdir");
        _exit(1);
    }

    mkdir("/proc", 0555);
#if defined(__linux__)
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("mount /proc");
#endif

    execlp("/bin/sh", "sh", "-c", config->command, (char *)NULL);
    perror("exec");
    _exit(1);
}

static ssize_t read_full(int fd, void *buf, size_t len)
{
    size_t total = 0;

    while (total < len) {
        ssize_t rc = read(fd, (char *)buf + total, len - total);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (rc == 0)
            break;
        total += (size_t)rc;
    }

    return (ssize_t)total;
}

static int write_full(int fd, const void *buf, size_t len)
{
    size_t total = 0;

    while (total < len) {
        ssize_t rc = write(fd, (const char *)buf + total, len - total);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += (size_t)rc;
    }

    return 0;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    for (container_record_t *cur = ctx->containers; cur; cur = cur->next) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
    }
    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_args_t *args = arg;
    supervisor_ctx_t *ctx = args->ctx;

    for (;;) {
        log_item_t item;
        char buf[LOG_CHUNK_SIZE];
        ssize_t rc = read(args->log_read_fd, buf, sizeof(buf));

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        if (rc == 0)
            break;

        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, args->container_id, sizeof(item.container_id) - 1);
        item.length = (size_t)rc;
        memcpy(item.data, buf, item.length);

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(args->log_read_fd);
    free(args);
    return NULL;
}

static void signal_handler(int sig)
{
    if (sig == SIGCHLD)
        g_got_sigchld = 1;
    else if (sig == SIGINT || sig == SIGTERM)
        g_should_stop = 1;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *cur = ctx->containers; cur; cur = cur->next) {
            if (cur->host_pid != pid)
                continue;

            if (WIFEXITED(status)) {
                cur->exit_code = WEXITSTATUS(status);
                cur->state = cur->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                cur->exit_signal = WTERMSIG(status);
                if (cur->stop_requested)
                    cur->state = CONTAINER_STOPPED;
                else if (cur->exit_signal == SIGKILL)
                    cur->state = CONTAINER_KILLED;
                else
                    cur->state = CONTAINER_EXITED;
            }

            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, cur->id, cur->host_pid);

            if (cur->notify_fd >= 0) {
                control_response_t resp;
                memset(&resp, 0, sizeof(resp));
                resp.status = 0;
                if (WIFEXITED(status))
                    snprintf(resp.message, sizeof(resp.message),
                             "container %s exited code=%d\n", cur->id, cur->exit_code);
                else if (WIFSIGNALED(status))
                    snprintf(resp.message, sizeof(resp.message),
                             "container %s signaled=%d\n", cur->id, cur->exit_signal);

                write_full(cur->notify_fd, &resp, sizeof(resp));
                close(cur->notify_fd);
                cur->notify_fd = -1;
            }

            pthread_mutex_unlock(&ctx->metadata_lock);
            if (cur->producer_started)
                pthread_join(cur->producer_thread, NULL);
            pthread_mutex_lock(&ctx->metadata_lock);
            break;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sigaction sa;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    (void)rootfs;

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    if (ctx.monitor_fd < 0)
        perror("open /dev/container_monitor");

    if (mkdir(LOG_DIR, 0755) != 0 && errno != EEXIST) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    while (!g_should_stop) {
        struct pollfd pfd;
        int poll_rc;

        if (g_got_sigchld) {
            g_got_sigchld = 0;
            reap_children(&ctx);
        }

        pfd.fd = ctx.server_fd;
        pfd.events = POLLIN;
        poll_rc = poll(&pfd, 1, 250);
        if (poll_rc < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }

        if (poll_rc == 0)
            continue;

        if (pfd.revents & POLLIN) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                continue;
            }

            control_request_t req;
            control_response_t resp;
            ssize_t got = read_full(client_fd, &req, sizeof(req));
            if (got != (ssize_t)sizeof(req)) {
                close(client_fd);
                continue;
            }

            memset(&resp, 0, sizeof(resp));
            resp.status = 0;

            if (req.kind == CMD_START || req.kind == CMD_RUN) {
                int pipe_fds[2];
                void *stack;
                child_config_t *config;
                pid_t child_pid;
                container_record_t *record;

                pthread_mutex_lock(&ctx.metadata_lock);
                record = find_container_locked(&ctx, req.container_id);
                pthread_mutex_unlock(&ctx.metadata_lock);
                if (record) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message),
                             "container %s already exists\n", req.container_id);
                    write_full(client_fd, &resp, sizeof(resp));
                    close(client_fd);
                    continue;
                }

                if (pipe(pipe_fds) != 0) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "pipe failed: %s\n", strerror(errno));
                    write_full(client_fd, &resp, sizeof(resp));
                    close(client_fd);
                    continue;
                }

                stack = malloc(STACK_SIZE);
                config = calloc(1, sizeof(*config));
                if (!stack || !config) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "allocation failed\n");
                    write_full(client_fd, &resp, sizeof(resp));
                    close(pipe_fds[0]);
                    close(pipe_fds[1]);
                    free(stack);
                    free(config);
                    close(client_fd);
                    continue;
                }

                strncpy(config->id, req.container_id, sizeof(config->id) - 1);
                strncpy(config->rootfs, req.rootfs, sizeof(config->rootfs) - 1);
                strncpy(config->command, req.command, sizeof(config->command) - 1);
                config->nice_value = req.nice_value;
                config->log_write_fd = pipe_fds[1];

                child_pid = clone(child_fn, (char *)stack + STACK_SIZE,
                                  CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                                  config);
                if (child_pid < 0) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "clone failed: %s\n", strerror(errno));
                    write_full(client_fd, &resp, sizeof(resp));
                    close(pipe_fds[0]);
                    close(pipe_fds[1]);
                    free(stack);
                    free(config);
                    close(client_fd);
                    continue;
                }

                close(pipe_fds[1]);

                record = calloc(1, sizeof(*record));
                if (!record) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "metadata allocation failed\n");
                    write_full(client_fd, &resp, sizeof(resp));
                    close(pipe_fds[0]);
                    close(client_fd);
                    continue;
                }

                strncpy(record->id, req.container_id, sizeof(record->id) - 1);
                record->host_pid = child_pid;
                record->started_at = time(NULL);
                record->state = CONTAINER_RUNNING;
                record->soft_limit_bytes = req.soft_limit_bytes;
                record->hard_limit_bytes = req.hard_limit_bytes;
                record->exit_code = -1;
                record->exit_signal = 0;
                record->stop_requested = 0;
                record->notify_fd = (req.kind == CMD_RUN) ? client_fd : -1;
                record->log_read_fd = pipe_fds[0];
                record->producer_started = 0;
                snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, record->id);

                producer_args_t *pargs = calloc(1, sizeof(*pargs));
                if (pargs) {
                    pargs->ctx = &ctx;
                    pargs->log_read_fd = pipe_fds[0];
                    strncpy(pargs->container_id, record->id, sizeof(pargs->container_id) - 1);
                    if (pthread_create(&record->producer_thread, NULL, producer_thread, pargs) == 0)
                        record->producer_started = 1;
                    else
                        close(pipe_fds[0]);
                } else {
                    close(pipe_fds[0]);
                }

                pthread_mutex_lock(&ctx.metadata_lock);
                record->next = ctx.containers;
                ctx.containers = record;
                pthread_mutex_unlock(&ctx.metadata_lock);

                if (ctx.monitor_fd >= 0) {
                    if (register_with_monitor(ctx.monitor_fd, record->id, record->host_pid,
                                              record->soft_limit_bytes, record->hard_limit_bytes) != 0) {
                        perror("register_with_monitor");
                    }
                }

                free(stack);
                free(config);

                if (req.kind == CMD_START) {
                    snprintf(resp.message, sizeof(resp.message),
                             "started container %s pid=%d\n", record->id, record->host_pid);
                    write_full(client_fd, &resp, sizeof(resp));
                    close(client_fd);
                }

                continue;
            }

            if (req.kind == CMD_PS) {
                snprintf(resp.message, sizeof(resp.message), "ok\n");
                write_full(client_fd, &resp, sizeof(resp));
                pthread_mutex_lock(&ctx.metadata_lock);
                for (container_record_t *cur = ctx.containers; cur; cur = cur->next) {
                    char line[256];
                    int len = snprintf(line, sizeof(line),
                                       "id=%s pid=%d state=%s soft=%lu hard=%lu exit=%d signal=%d\n",
                                       cur->id, cur->host_pid, state_to_string(cur->state),
                                       cur->soft_limit_bytes, cur->hard_limit_bytes,
                                       cur->exit_code, cur->exit_signal);
                    write_full(client_fd, line, (size_t)len);
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                close(client_fd);
                continue;
            }

            if (req.kind == CMD_LOGS) {
                container_record_t *record;
                int fd;
                char buf[1024];

                pthread_mutex_lock(&ctx.metadata_lock);
                record = find_container_locked(&ctx, req.container_id);
                pthread_mutex_unlock(&ctx.metadata_lock);

                if (!record) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message),
                             "container %s not found\n", req.container_id);
                    write_full(client_fd, &resp, sizeof(resp));
                    close(client_fd);
                    continue;
                }

                snprintf(resp.message, sizeof(resp.message), "ok\n");
                write_full(client_fd, &resp, sizeof(resp));
                fd = open(record->log_path, O_RDONLY);
                if (fd >= 0) {
                    ssize_t r;
                    while ((r = read(fd, buf, sizeof(buf))) > 0)
                        write_full(client_fd, buf, (size_t)r);
                    close(fd);
                }
                close(client_fd);
                continue;
            }

            if (req.kind == CMD_STOP) {
                container_record_t *record;
                pthread_mutex_lock(&ctx.metadata_lock);
                record = find_container_locked(&ctx, req.container_id);
                if (record) {
                    record->stop_requested = 1;
                    kill(record->host_pid, SIGTERM);
                }
                pthread_mutex_unlock(&ctx.metadata_lock);

                if (!record) {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message),
                             "container %s not found\n", req.container_id);
                } else {
                    snprintf(resp.message, sizeof(resp.message),
                             "stop requested for %s\n", req.container_id);
                }
                write_full(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            resp.status = 1;
            snprintf(resp.message, sizeof(resp.message), "unknown command\n");
            write_full(client_fd, &resp, sizeof(resp));
            close(client_fd);
        }
    }

    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *cur = ctx.containers; cur; cur = cur->next) {
        cur->stop_requested = 1;
        kill(cur->host_pid, SIGTERM);
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    reap_children(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 1;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != 0) {
        perror("write");
        close(fd);
        return 1;
    }

    if (read_full(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Failed to read response\n");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0') {
        if (resp.status == 0)
            fputs(resp.message, stdout);
        else
            fputs(resp.message, stderr);
    }

    for (;;) {
        char buf[1024];
        ssize_t r = read(fd, buf, sizeof(buf));
        if (r < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (r == 0)
            break;
        fwrite(buf, 1, (size_t)r, stdout);
    }

    close(fd);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}