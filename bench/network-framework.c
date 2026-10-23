/* Does Network.framework get round the descriptor lock that serialises a process's socket calls on macOS? N
 * connections over loopback TCP each ping-pong 64-byte messages with an echo server, as one process or a process per
 * connection (each with its own listener):
 *
 *   nw   Network.framework: an nw_listener_t and nw_connection_t clients, every connection, either side, on a
 *        serial dispatch queue of its own, so Dispatch runs them on as many threads as it likes
 *   tcp  BSD sockets, for comparison: a pair of threads per connection, the client's and the server's, each
 *        blocking in `read` on its socket (busy-polling two non-blocking sockets in one process was slower still,
 *        the two threads competing for the descriptor lock)
 *
 *   cc -O2 -o /tmp/network-framework bench/network-framework.c -framework Network
 *   /tmp/network-framework nw|tcp threads|procs N [round trips]
 *
 * Both disable Nagle's algorithm. Sampling a run shows which system calls Network.framework makes, and so whether it
 * uses the user-space stack (Skywalk channels, `__channel_sync`) or the kernel's sockets. See "SwiftNIO meets the same
 * lock" in io-backends/kqueue/TODO.md.
 */
#include <Network/Network.h>
#include <arpa/inet.h>
#include <dispatch/dispatch.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static long rounds;

static nw_parameters_t tcp_parameters(void) {
  return nw_parameters_create_secure_tcp(NW_PARAMETERS_DISABLE_PROTOCOL, ^(nw_protocol_options_t options) {
    nw_tcp_options_set_no_delay(options, true);
  });
}

/* Echoes every message on `connection` until it ends. */
static void echo(nw_connection_t connection) {
  nw_connection_receive(connection, 64, 64,
                        ^(dispatch_data_t content, nw_content_context_t context, bool complete, nw_error_t error) {
                          if (content == NULL || error != NULL) {
                            nw_connection_cancel(connection);
                            return;
                          }
                          nw_connection_send(connection, content, NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT, false,
                                             ^(nw_error_t send_error) {});
                          echo(connection);
                        });
}

/* Sends a message, waits for its echo, `left` more times, then signals `done`. */
static void ping(nw_connection_t connection, dispatch_data_t message, long left, dispatch_semaphore_t done) {
  if (left == 0) {
    dispatch_semaphore_signal(done);
    return;
  }
  nw_connection_send(connection, message, NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT, false, ^(nw_error_t error) {
    if (error != NULL) abort();
  });
  nw_connection_receive(connection, 64, 64,
                        ^(dispatch_data_t content, nw_content_context_t context, bool complete, nw_error_t error) {
                          if (content == NULL || error != NULL) abort();
                          ping(connection, message, left - 1, done);
                        });
}

/* A listener, with `n` connections to it each ping-ponging `rounds` messages; returns once all are done. */
static void run_nw(int n) {
  nw_listener_t listener = nw_listener_create(tcp_parameters());
  dispatch_queue_t listen_queue = dispatch_queue_create("listener", DISPATCH_QUEUE_SERIAL);
  dispatch_semaphore_t ready = dispatch_semaphore_create(0);
  nw_listener_set_queue(listener, listen_queue);
  nw_listener_set_state_changed_handler(listener, ^(nw_listener_state_t state, nw_error_t error) {
    if (state == nw_listener_state_ready) dispatch_semaphore_signal(ready);
    if (state == nw_listener_state_failed) abort();
  });
  nw_listener_set_new_connection_handler(listener, ^(nw_connection_t connection) {
    nw_connection_set_queue(connection, dispatch_queue_create("server", DISPATCH_QUEUE_SERIAL));
    nw_connection_start(connection);
    echo(connection);
  });
  nw_listener_start(listener);
  dispatch_semaphore_wait(ready, DISPATCH_TIME_FOREVER);
  char port[8];
  snprintf(port, sizeof(port), "%u", nw_listener_get_port(listener));
  nw_endpoint_t endpoint = nw_endpoint_create_host("127.0.0.1", port);

  char bytes[64] = {0};
  dispatch_data_t message = dispatch_data_create(bytes, 64, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
  dispatch_semaphore_t done = dispatch_semaphore_create(0);
  for (int i = 0; i < n; i++) {
    nw_connection_t connection = nw_connection_create(endpoint, tcp_parameters());
    nw_connection_set_queue(connection, dispatch_queue_create("client", DISPATCH_QUEUE_SERIAL));
    nw_connection_set_state_changed_handler(connection, ^(nw_connection_state_t state, nw_error_t error) {
      if (state == nw_connection_state_ready) ping(connection, message, rounds, done);
      if (state == nw_connection_state_failed) abort();
    });
    nw_connection_start(connection);
  }
  for (int i = 0; i < n; i++) dispatch_semaphore_wait(done, DISPATCH_TIME_FOREVER);
}

static void *tcp_echo(void *p) {
  int fd = *(int *)p;
  char b[64];
  for (long i = 0; i < rounds; i++) {
    long got = 0;
    while (got < 64) {
      long r = read(fd, b + got, 64 - got);
      if (r > 0) got += r;
      else if (r == 0) abort();
    }
    if (write(fd, b, 64) != 64) abort();
  }
  return 0;
}

static void *tcp_client(void *p) {
  int fd = *(int *)p;
  char b[64] = {0};
  for (long i = 0; i < rounds; i++) {
    if (write(fd, b, 64) != 64) abort();
    long got = 0;
    while (got < 64) {
      long r = read(fd, b + got, 64 - got);
      if (r > 0) got += r;
      else if (r == 0) abort();
    }
  }
  return 0;
}

/* `n` loopback TCP connections, each with a client and an echo thread blocking in `read`. */
static void run_tcp(int n) {
  int listener = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
  socklen_t len = sizeof(addr);
  if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(listener, 128) != 0) abort();
  getsockname(listener, (struct sockaddr *)&addr, &len);
  int *fds = malloc(sizeof(int) * 2 * n);
  for (int i = 0; i < n; i++) {
    fds[2 * i] = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fds[2 * i], (struct sockaddr *)&addr, sizeof(addr)) != 0) abort();
    fds[2 * i + 1] = accept(listener, 0, 0);
    for (int k = 0; k < 2; k++) {
      int one = 1;
      setsockopt(fds[2 * i + k], IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
  }
  pthread_t *ts = malloc(sizeof(pthread_t) * 2 * n);
  for (int i = 0; i < n; i++) {
    pthread_create(&ts[2 * i], 0, tcp_echo, &fds[2 * i + 1]);
    pthread_create(&ts[2 * i + 1], 0, tcp_client, &fds[2 * i]);
  }
  for (int i = 0; i < 2 * n; i++) pthread_join(ts[i], 0);
}

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "usage: %s nw|tcp threads|procs N [round trips]\n", argv[0]);
    return 2;
  }
  int nw = !strcmp(argv[1], "nw");
  int n = atoi(argv[3]);
  rounds = argc > 4 ? atol(argv[4]) : 100000;
  if (n < 1 || n > 64) return 2;
  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  if (!strcmp(argv[2], "threads")) {
    if (nw) run_nw(n);
    else run_tcp(n);
  } else {
    for (int i = 0; i < n; i++)
      if (fork() == 0) {
        if (nw) run_nw(1);
        else run_tcp(1);
        _exit(0);
      }
    for (int i = 0; i < n; i++) wait(0);
  }
  clock_gettime(CLOCK_MONOTONIC, &t1);
  double seconds = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
  printf("%-3s %-7s x%d: %.2f us per round trip per connection, %.0fk round trips/s in all\n", argv[1], argv[2], n,
         seconds * 1e6 / rounds, n * rounds / seconds / 1e3);
  return 0;
}
