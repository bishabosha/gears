/* The C side of `asyncio.darwin`: Apple's Network.framework for sockets, and Dispatch sources for descriptors, behind an
 * interface that Scala Native can drive from a thread of its own.
 *
 * Network.framework delivers everything on Dispatch's threads, as blocks: a receive's data, a send's completion, a
 * connection's state, a listener's new connections. Scala Native cannot run managed code on a thread it did not start,
 * so nothing here calls back into Scala. Instead each reactor (`dn_reactor`) has a queue of notifications that the
 * blocks post to and the reactor's own thread drains (`dn_reactor_drain`), waiting on a condition variable in between
 * (`dn_reactor_wait`), and each handle (`dn_handle`) keeps what the blocks delivered until the reactor's thread takes
 * it: received bytes in a stash, received datagrams as a queue of packets, accepted connections as a queue, and the
 * connection's state and errors. An op is a waiter (`dn_wait`) registered on the handle for what it waits for, with
 * the reactor and token to notify, and the reactor's thread acts on a notification by asking the handle again
 * (`dn_read` and the like), which either finishes the op or registers the waiter again.
 *
 * A receive once issued cannot be withdrawn, so a cancelled read leaves its receive outstanding, and what it brings
 * lands in the stash for the next read on the handle. Sends copy their bytes first, so a cancelled write leaves the
 * caller's buffer alone. Closing a handle takes its waiters, so their ops fail on their reactors, and cancels the
 * connection; the handle is freed once Network.framework has delivered its last callback, the cancelled state.
 *
 * Every function here returns at once, except `dn_reactor_wait` and `dn_listen`, which waits for the listener to be
 * ready or to fail, so that listening can report failure where it is called.
 */
#ifdef __APPLE__

#include <Network/Network.h>
#include <arpa/inet.h>
#include <dispatch/dispatch.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

/* Codes the handle functions return. Non-negative values are counts; these are the rest. */
enum {
  DN_EOF = -1,
  DN_CLOSED = -2, /* the handle was closed */
  DN_BUSY = -3,   /* a waiter for the same thing is already registered */
  DN_ERROR = -4,  /* an error: `dn_handle_error` has the errno */
  DN_WAITING = -5 /* nothing yet: a waiter was registered */
};

/* What a notification is about. */
enum { DN_READ = 1, DN_WRITE = 2, DN_CONNECT = 3 };

/* Kinds of handle. */
enum { DN_STREAM = 1, DN_LISTENER = 2, DN_DGRAM = 3, DN_DGRAM_BOUND = 4, DN_FLOW = 5, DN_FD = 6 };

typedef struct dn_reactor dn_reactor_t;
typedef struct dn_handle dn_handle_t;

/* An op waiting on a handle: the reactor and token to notify, and what for. Allocated when registered, posted to the
 * reactor's queue when notified, and freed when the reactor drains it. */
typedef struct dn_wait {
  struct dn_wait *next;
  dn_reactor_t *reactor;
  long token;
  int kind;
  int closed; /* notified because the handle was closed */
} dn_wait_t;

/* What `dn_reactor_drain` hands the reactor's thread. */
typedef struct dn_done {
  long token;
  int kind;
  int closed;
} dn_done_t;

struct dn_reactor {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  dn_wait_t *head, *tail;
  int woken;
  int parked; /* a thread waits in `dn_reactor_wait`, so posting must signal */
  _Atomic int ready; /* something is queued, or a wake-up is pending: checked without the lock */
};

dn_reactor_t *dn_reactor_create(void) {
  dn_reactor_t *r = calloc(1, sizeof(dn_reactor_t));
  pthread_mutex_init(&r->lock, NULL);
  pthread_cond_init(&r->cond, NULL);
  return r;
}

void dn_reactor_destroy(dn_reactor_t *r) {
  pthread_mutex_lock(&r->lock);
  dn_wait_t *w = r->head;
  while (w) {
    dn_wait_t *next = w->next;
    free(w);
    w = next;
  }
  r->head = r->tail = NULL;
  pthread_mutex_unlock(&r->lock);
  pthread_mutex_destroy(&r->lock);
  pthread_cond_destroy(&r->cond);
  free(r);
}

static void dn_post(dn_wait_t *w) {
  dn_reactor_t *r = w->reactor;
  w->next = NULL;
  pthread_mutex_lock(&r->lock);
  if (r->tail) r->tail->next = w;
  else r->head = w;
  r->tail = w;
  atomic_store(&r->ready, 1);
  if (r->parked) pthread_cond_signal(&r->cond);
  pthread_mutex_unlock(&r->lock);
}

void dn_reactor_wake(dn_reactor_t *r) {
  pthread_mutex_lock(&r->lock);
  r->woken = 1;
  atomic_store(&r->ready, 1);
  if (r->parked) pthread_cond_signal(&r->cond);
  pthread_mutex_unlock(&r->lock);
}

/* Whether something is queued or a wake-up is pending, without blocking or locking. */
int dn_reactor_ready(dn_reactor_t *r) { return atomic_load(&r->ready); }

/* Waits until something is queued, or `dn_reactor_wake` is called, or `timeout_ns` pass (negative: for ever). A
 * pending wake-up is consumed. */
void dn_reactor_wait(dn_reactor_t *r, long long timeout_ns) {
  pthread_mutex_lock(&r->lock);
  if (!r->head && !r->woken) {
    r->parked = 1;
    if (timeout_ns < 0) {
      while (!r->head && !r->woken) pthread_cond_wait(&r->cond, &r->lock);
    } else {
      struct timespec rel = {.tv_sec = timeout_ns / 1000000000LL, .tv_nsec = timeout_ns % 1000000000LL};
      if (!r->head && !r->woken) pthread_cond_timedwait_relative_np(&r->cond, &r->lock, &rel);
    }
    r->parked = 0;
  }
  r->woken = 0;
  atomic_store(&r->ready, r->head != NULL);
  pthread_mutex_unlock(&r->lock);
}

/* Takes up to `max` notifications into `out`, freeing their waiters; consumes a pending wake-up. */
int dn_reactor_drain(dn_reactor_t *r, dn_done_t *out, int max) {
  if (!atomic_load(&r->ready)) return 0;
  pthread_mutex_lock(&r->lock);
  int n = 0;
  while (n < max && r->head) {
    dn_wait_t *w = r->head;
    r->head = w->next;
    if (!r->head) r->tail = NULL;
    out[n].token = w->token;
    out[n].kind = w->kind;
    out[n].closed = w->closed;
    free(w);
    n++;
  }
  r->woken = 0;
  atomic_store(&r->ready, r->head != NULL);
  pthread_mutex_unlock(&r->lock);
  return n;
}

/* A received datagram, with its sender. */
typedef struct dn_packet {
  struct dn_packet *next;
  size_t len;
  struct sockaddr_storage from;
  socklen_t from_len;
  uint8_t bytes[];
} dn_packet_t;

struct dn_handle {
  pthread_mutex_t lock;
  int kind;
  int closed;
  _Atomic int refs; /* the owner's, plus one while Network.framework may still call back, plus one per source */

  dn_wait_t *read_wait, *write_wait, *connect_wait;

  nw_connection_t connection;
  nw_listener_t listener;
  nw_parameters_t parameters;     /* a bound datagram socket's, for the flows it opens */
  nw_endpoint_t local_endpoint;   /* likewise, its own address */
  int state;                      /* 0 not ready yet, 1 ready, 2 failed, 3 cancelled */
  int error;                      /* the errno of a failed connection or listener */
  int read_error, write_error;    /* errnos of a failed receive or send */

  /* A stream's received bytes not yet taken. */
  uint8_t *stash;
  size_t stash_len, stash_cap;
  int receiving; /* a receive is outstanding */
  int read_eof;
  int sends;          /* sends outstanding */
  size_t sent_bytes;  /* bytes of those sends */
  int cancel_pending; /* closed with sends outstanding: the last send's completion cancels the connection */

  /* A listener's accepted connections, and a bound datagram socket's flows; a flow's parent and the sender it stands
   * for; received datagrams. */
  struct dn_handle *accepted_head, *accepted_tail, *next_accepted;
  struct dn_handle *flows, *next_flow, *parent;
  struct sockaddr_storage remote;
  socklen_t remote_len;
  dn_packet_t *packets_head, *packets_tail;
  size_t packet_max; /* how large a datagram to receive, from the first receive's buffer */

  /* Set by `dn_close` on a listener, which waits on it until the listener's cancelled state, so that its port is free
   * once `close` returns. */
  dispatch_semaphore_t cancelled;

  /* A descriptor: its Dispatch sources, created when first waited on, each suspended between events. */
  int fd;
  dispatch_source_t read_source, write_source;
  int read_suspended, write_suspended;
};

/* The queue Network.framework and the sources deliver on: the global queue of the QoS class `ASYNCIO_DARWIN_QOS`
 * names (interactive, initiated, default, utility), user-initiated by default. */
static qos_class_t dn_qos(void) {
  static qos_class_t cached = QOS_CLASS_UNSPECIFIED;
  if (cached == QOS_CLASS_UNSPECIFIED) {
    const char *value = getenv("ASYNCIO_DARWIN_QOS");
    cached = value == NULL ? QOS_CLASS_USER_INITIATED
             : strcmp(value, "interactive") == 0 ? QOS_CLASS_USER_INTERACTIVE
             : strcmp(value, "default") == 0 ? QOS_CLASS_DEFAULT
             : strcmp(value, "utility") == 0 ? QOS_CLASS_UTILITY
             : QOS_CLASS_USER_INITIATED;
  }
  return cached;
}

static dispatch_queue_t dn_queue(void) { return dispatch_get_global_queue(dn_qos(), 0); }

/* Gives the calling thread, a reactor's, the same QoS class as the queue, so that wake-ups between them are not
 * demoted; `ASYNCIO_DARWIN_THREAD_QOS=0` leaves it as it is, for measuring. */
void dn_reactor_thread_qos(void) {
  const char *value = getenv("ASYNCIO_DARWIN_THREAD_QOS");
  if (value && strcmp(value, "0") == 0) return;
  pthread_set_qos_class_self_np(dn_qos(), 0);
}

static dn_handle_t *dn_handle_new(int kind) {
  dn_handle_t *h = calloc(1, sizeof(dn_handle_t));
  pthread_mutex_init(&h->lock, NULL);
  h->kind = kind;
  h->fd = -1;
  atomic_store(&h->refs, 1);
  return h;
}

static void dn_handle_retain(dn_handle_t *h) { atomic_fetch_add(&h->refs, 1); }

static void dn_handle_release(dn_handle_t *h) {
  if (atomic_fetch_sub(&h->refs, 1) == 1) {
    if (h->connection) nw_release(h->connection);
    if (h->listener) nw_release(h->listener);
    if (h->parameters) nw_release(h->parameters);
    if (h->local_endpoint) nw_release(h->local_endpoint);
    free(h->stash);
    dn_packet_t *p = h->packets_head;
    while (p) {
      dn_packet_t *next = p->next;
      free(p);
      p = next;
    }
    pthread_mutex_destroy(&h->lock);
    free(h);
  }
}

/* The errno of a Network.framework error; `EIO` for one of another domain. */
static int dn_errno(nw_error_t error) {
  if (error == NULL) return 0;
  if (nw_error_get_error_domain(error) == nw_error_domain_posix) return nw_error_get_error_code(error);
  int code = nw_error_get_error_code(error);
  return code > 0 && code < 200 ? code : EIO;
}

/* The errno of a handle's last failure, for `DN_ERROR`. */
int dn_handle_error(dn_handle_t *h) {
  pthread_mutex_lock(&h->lock);
  int e = h->read_error ? h->read_error : h->write_error ? h->write_error : h->error;
  pthread_mutex_unlock(&h->lock);
  return e ? e : EIO;
}

/* Registers a waiter for `kind` in `slot`, which must be empty; returns DN_WAITING, or DN_BUSY. Under the lock. */
static int dn_register(dn_wait_t **slot, dn_reactor_t *reactor, long token, int kind) {
  if (*slot) return DN_BUSY;
  dn_wait_t *w = calloc(1, sizeof(dn_wait_t));
  w->reactor = reactor;
  w->token = token;
  w->kind = kind;
  *slot = w;
  return DN_WAITING;
}

/* Takes a waiter out of `slot`, under the lock, to post it afterwards. */
static dn_wait_t *dn_take(dn_wait_t **slot) {
  dn_wait_t *w = *slot;
  *slot = NULL;
  return w;
}

static void dn_post_closed(dn_wait_t *w) {
  if (w) {
    w->closed = 1;
    dn_post(w);
  }
}

static void dn_post_if(dn_wait_t *w) {
  if (w) dn_post(w);
}

/* Withdraws the waiter for `kind` if it is still `token`'s: 1 if it was, 0 if it had already been notified. */
int dn_cancel_wait(dn_handle_t *h, int kind, long token) {
  pthread_mutex_lock(&h->lock);
  dn_wait_t **slot = kind == DN_READ ? &h->read_wait : kind == DN_WRITE ? &h->write_wait : &h->connect_wait;
  int found = *slot && (*slot)->token == token;
  if (found) {
    free(*slot);
    *slot = NULL;
  }
  pthread_mutex_unlock(&h->lock);
  return found;
}

/* ---- Parameters and endpoints ---- */

static nw_parameters_t dn_tcp_parameters(void) {
  return nw_parameters_create_secure_tcp(NW_PARAMETERS_DISABLE_PROTOCOL, NW_PARAMETERS_DEFAULT_CONFIGURATION);
}

static nw_parameters_t dn_udp_parameters(void) {
  return nw_parameters_create_secure_udp(NW_PARAMETERS_DISABLE_PROTOCOL, NW_PARAMETERS_DEFAULT_CONFIGURATION);
}

static nw_endpoint_t dn_endpoint(const char *host, int port) {
  char text[16];
  snprintf(text, sizeof(text), "%d", port);
  return nw_endpoint_create_host(host, text);
}

/* Restricts `parameters` to the IP version of `host`, so that a listener on "::1" is not also on IPv4. */
static void dn_restrict_family(nw_parameters_t parameters, const char *host) {
  nw_protocol_stack_t stack = nw_parameters_copy_default_protocol_stack(parameters);
  nw_protocol_options_t ip = nw_protocol_stack_copy_internet_protocol(stack);
  nw_ip_options_set_version(ip, strchr(host, ':') ? nw_ip_version_6 : nw_ip_version_4);
  nw_release(ip);
  nw_release(stack);
}

/* ---- Stream connections ---- */

/* Whether a read that took bytes issues the next receive at once, so that the next read finds its bytes waiting:
 * `ASYNCIO_DARWIN_PREFETCH=0` in the environment turns it off, for measuring. */
static int dn_prefetch(void) {
  static int cached = -1;
  if (cached < 0) {
    const char *value = getenv("ASYNCIO_DARWIN_PREFETCH");
    cached = value == NULL || strcmp(value, "0") != 0;
  }
  return cached;
}

static void dn_stash_append(dn_handle_t *h, dispatch_data_t content) {
  size_t size = dispatch_data_get_size(content);
  if (size == 0) return;
  if (h->stash_len + size > h->stash_cap) {
    size_t cap = h->stash_cap ? h->stash_cap : 4096;
    while (cap < h->stash_len + size) cap *= 2;
    h->stash = realloc(h->stash, cap);
    h->stash_cap = cap;
  }
  __block size_t at = h->stash_len;
  __block uint8_t *stash = h->stash;
  dispatch_data_apply(content, ^bool(dispatch_data_t region, size_t offset, const void *buffer, size_t length) {
    memcpy(stash + at, buffer, length);
    at += length;
    return true;
  });
  h->stash_len += size;
}

/* Issues a receive of up to `max` bytes, under the lock. Its block stashes what arrives and notifies the read waiter
 * of the time, whichever reactor's. */
static void dn_receive(dn_handle_t *h, uint32_t max) {
  h->receiving = 1;
  dn_handle_retain(h);
  nw_connection_receive(h->connection, 1, max,
                        ^(dispatch_data_t content, nw_content_context_t context, bool is_complete, nw_error_t error) {
                          pthread_mutex_lock(&h->lock);
                          h->receiving = 0;
                          if (content) dn_stash_append(h, content);
                          if (is_complete && context && nw_content_context_get_is_final(context)) h->read_eof = 1;
                          if (error && !h->closed) h->read_error = dn_errno(error);
                          dn_wait_t *w = dn_take(&h->read_wait);
                          pthread_mutex_unlock(&h->lock);
                          dn_post_if(w);
                          dn_handle_release(h);
                        });
}

/* Installs the state handler of a stream or datagram connection, and starts it. */
static void dn_connection_start(dn_handle_t *h) {
  dn_handle_retain(h); /* until the cancelled state, Network.framework's last callback */
  nw_connection_set_queue(h->connection, dn_queue());
  nw_connection_set_state_changed_handler(h->connection, ^(nw_connection_state_t state, nw_error_t error) {
    dn_wait_t *c = NULL, *r = NULL, *w = NULL;
    int done = 0;
    pthread_mutex_lock(&h->lock);
    if (state == nw_connection_state_ready) {
      h->state = 1;
      c = dn_take(&h->connect_wait);
    } else if (state == nw_connection_state_failed || (state == nw_connection_state_waiting && error != NULL)) {
      /* Network.framework keeps a refused or unreachable connection `waiting` for a path to appear; a backend whose
       * connect fails, as POSIX's does, takes the error for its outcome. */
      h->state = 2;
      h->error = dn_errno(error);
      if (!h->read_error) h->read_error = h->error;
      if (!h->write_error) h->write_error = h->error;
      c = dn_take(&h->connect_wait);
      r = dn_take(&h->read_wait);
      w = dn_take(&h->write_wait);
    } else if (state == nw_connection_state_cancelled) {
      h->state = 3;
      done = 1;
      c = dn_take(&h->connect_wait);
      r = dn_take(&h->read_wait);
      w = dn_take(&h->write_wait);
    }
    pthread_mutex_unlock(&h->lock);
    if (done) {
      dn_post_closed(c);
      dn_post_closed(r);
      dn_post_closed(w);
      dn_handle_release(h);
    } else {
      dn_post_if(c);
      dn_post_if(r);
      dn_post_if(w);
    }
  });
  nw_connection_start(h->connection);
}

/* A stream connection to `host`:`port`, started; perform a connect op to wait until it is connected. */
dn_handle_t *dn_connect(const char *host, int port) {
  dn_handle_t *h = dn_handle_new(DN_STREAM);
  nw_endpoint_t endpoint = dn_endpoint(host, port);
  nw_parameters_t parameters = dn_tcp_parameters();
  h->connection = nw_connection_create(endpoint, parameters);
  nw_release(endpoint);
  nw_release(parameters);
  dn_connection_start(h);
  return h;
}

/* The connect op: 1 once connected, DN_WAITING with a waiter registered, DN_ERROR if it failed, DN_CLOSED. */
int dn_wait_connected(dn_handle_t *h, dn_reactor_t *reactor, long token) {
  pthread_mutex_lock(&h->lock);
  int result;
  if (h->closed) result = DN_CLOSED;
  else if (h->state == 1) result = 1;
  else if (h->state == 2) result = DN_ERROR;
  else result = dn_register(&h->connect_wait, reactor, token, DN_CONNECT);
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* The read op on a stream: takes up to `cap` stashed bytes into `buf` and returns how many; DN_EOF at the end of the
 * stream; DN_ERROR; DN_CLOSED; or DN_WAITING with a waiter registered and a receive outstanding. After taking bytes
 * it keeps a receive outstanding, so that the next read finds its bytes waiting. */
int dn_read(dn_handle_t *h, uint8_t *buf, int cap, dn_reactor_t *reactor, long token) {
  pthread_mutex_lock(&h->lock);
  int result;
  if (h->closed) result = DN_CLOSED;
  else if (h->stash_len > 0) {
    size_t n = h->stash_len < (size_t)cap ? h->stash_len : (size_t)cap;
    memcpy(buf, h->stash, n);
    h->stash_len -= n;
    if (h->stash_len > 0) memmove(h->stash, h->stash + n, h->stash_len);
    result = (int)n;
    if (dn_prefetch() && h->stash_len == 0 && !h->read_eof && !h->read_error && !h->receiving && h->state == 1)
      dn_receive(h, cap);
  } else if (h->read_eof) result = DN_EOF;
  else if (h->read_error) result = DN_ERROR;
  else {
    result = dn_register(&h->read_wait, reactor, token, DN_READ);
    if (result == DN_WAITING && !h->receiving) dn_receive(h, cap);
  }
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* How many bytes of sends a handle may have outstanding before a write waits for a completion: the pacing Network
 * framework asks for, as a kernel socket's send buffer paces writes. `ASYNCIO_DARWIN_SEND_WINDOW` in the environment
 * overrides it, in bytes; 0 makes every write wait for its send, for measuring. */
static size_t dn_send_window(void) {
  static long cached = -1;
  if (cached < 0) {
    const char *value = getenv("ASYNCIO_DARWIN_SEND_WINDOW");
    cached = value ? atol(value) : 256 * 1024;
  }
  return (size_t)cached;
}

/* Sends `len` bytes, copied, as one message: on a stream, bytes of the stream; on a datagram socket, one datagram.
 * While the handle's outstanding sends are within the window, the write is done: returns `len` at once, and the
 * send's completion is only accounted for. Otherwise returns DN_WAITING with a waiter registered, notified when a
 * send has been processed; DN_BUSY, DN_CLOSED, or DN_ERROR for a failed connection. */
static int dn_send_on(dn_handle_t *h, nw_connection_t connection, const uint8_t *bytes, int len, dn_reactor_t *reactor,
                      long token) {
  if (h->closed) return DN_CLOSED;
  if (h->write_error) return DN_ERROR;
  if (h->write_wait) return DN_BUSY;
  int fits = h->sent_bytes < dn_send_window();
  if (!fits) {
    int registered = dn_register(&h->write_wait, reactor, token, DN_WRITE);
    if (registered != DN_WAITING) return registered;
  }
  dispatch_data_t data = dispatch_data_create(bytes, len, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
  h->sends++;
  h->sent_bytes += len;
  dn_handle_retain(h);
  nw_connection_send(connection, data, NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT, true, ^(nw_error_t error) {
    pthread_mutex_lock(&h->lock);
    h->sends--;
    h->sent_bytes -= len;
    if (error && !h->closed) h->write_error = dn_errno(error);
    dn_wait_t *w = dn_take(&h->write_wait);
    int cancel = h->cancel_pending && h->sends == 0;
    if (cancel) h->cancel_pending = 0;
    pthread_mutex_unlock(&h->lock);
    dn_post_if(w);
    if (cancel) nw_connection_cancel(h->connection); /* the close that waited for this send */
    dn_handle_release(h);
  });
  nw_release(data);
  return fits ? len : DN_WAITING;
}

int dn_write(dn_handle_t *h, const uint8_t *bytes, int len, dn_reactor_t *reactor, long token) {
  pthread_mutex_lock(&h->lock);
  int result = dn_send_on(h, h->connection, bytes, len, reactor, token);
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* After a write's waiter was notified: 1 if the send went, DN_ERROR if it failed, DN_CLOSED. */
int dn_write_outcome(dn_handle_t *h) {
  pthread_mutex_lock(&h->lock);
  int result = h->closed ? DN_CLOSED : h->write_error ? DN_ERROR : 1;
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* ---- Listeners ---- */

/* An accepted connection, started on arrival. */
static void dn_accepted(dn_handle_t *listener, nw_connection_t connection) {
  dn_handle_t *h = dn_handle_new(DN_STREAM);
  h->connection = connection;
  nw_retain(connection);
  dn_connection_start(h);
  pthread_mutex_lock(&listener->lock);
  if (listener->closed) {
    pthread_mutex_unlock(&listener->lock);
    nw_connection_cancel(h->connection);
    dn_handle_release(h);
    return;
  }
  if (listener->accepted_tail) listener->accepted_tail->next_accepted = h;
  else listener->accepted_head = h;
  listener->accepted_tail = h;
  dn_wait_t *w = dn_take(&listener->read_wait);
  pthread_mutex_unlock(&listener->lock);
  dn_post_if(w);
}

/* Starts `h`'s listener and waits until it is ready, or failed: 0, or the errno. */
static int dn_listener_start(dn_handle_t *h) {
  dispatch_semaphore_t settled = dispatch_semaphore_create(0);
  dn_handle_retain(h);
  nw_listener_set_queue(h->listener, dn_queue());
  nw_listener_set_state_changed_handler(h->listener, ^(nw_listener_state_t state, nw_error_t error) {
    int signal = 0, done = 0;
    pthread_mutex_lock(&h->lock);
    if (state == nw_listener_state_ready && h->state == 0) {
      h->state = 1;
      signal = 1;
    } else if (state == nw_listener_state_failed) {
      if (h->state == 0) signal = 1;
      h->state = 2;
      h->error = dn_errno(error);
    } else if (state == nw_listener_state_cancelled) {
      h->state = 3;
      done = 1;
    }
    dn_wait_t *r = done || state == nw_listener_state_failed ? dn_take(&h->read_wait) : NULL;
    dispatch_semaphore_t cancelled = done ? h->cancelled : NULL;
    pthread_mutex_unlock(&h->lock);
    if (signal) dispatch_semaphore_signal(settled);
    if (done) {
      dn_post_closed(r);
      if (cancelled) dispatch_semaphore_signal(cancelled); /* before the release: `dn_close` holds a reference too */
      dn_handle_release(h);
    } else dn_post_if(r);
  });
  nw_listener_start(h->listener);
  dispatch_semaphore_wait(settled, DISPATCH_TIME_FOREVER);
  dispatch_release(settled);
  pthread_mutex_lock(&h->lock);
  int error = h->state == 2 ? (h->error ? h->error : EIO) : 0;
  pthread_mutex_unlock(&h->lock);
  return error;
}

/* A stream listener on `host`:`port`, ready; or NULL with `*error` set. */
dn_handle_t *dn_listen(const char *host, int port, int reuse, int *error) {
  dn_handle_t *h = dn_handle_new(DN_LISTENER);
  nw_parameters_t parameters = dn_tcp_parameters();
  if (reuse) nw_parameters_set_reuse_local_address(parameters, true);
  dn_restrict_family(parameters, host);
  nw_endpoint_t local = dn_endpoint(host, port);
  nw_parameters_set_local_endpoint(parameters, local);
  nw_release(local);
  h->listener = nw_listener_create(parameters);
  nw_release(parameters);
  if (h->listener == NULL) {
    *error = EINVAL;
    dn_handle_release(h);
    return NULL;
  }
  nw_listener_set_new_connection_handler(h->listener, ^(nw_connection_t connection) {
    dn_accepted(h, connection);
  });
  *error = dn_listener_start(h);
  if (*error) {
    nw_listener_cancel(h->listener);
    dn_handle_release(h);
    return NULL;
  }
  return h;
}

/* The accept op: the next accepted connection, or NULL with `*code` DN_WAITING, DN_CLOSED, DN_ERROR or DN_BUSY. */
dn_handle_t *dn_accept(dn_handle_t *h, dn_reactor_t *reactor, long token, int *code) {
  pthread_mutex_lock(&h->lock);
  dn_handle_t *accepted = NULL;
  if (h->closed) *code = DN_CLOSED;
  else if (h->accepted_head) {
    accepted = h->accepted_head;
    h->accepted_head = accepted->next_accepted;
    if (!h->accepted_head) h->accepted_tail = NULL;
    accepted->next_accepted = NULL;
    *code = 1;
  } else if (h->state == 2) *code = DN_ERROR;
  else *code = dn_register(&h->read_wait, reactor, token, DN_READ);
  pthread_mutex_unlock(&h->lock);
  return accepted;
}

/* ---- Datagrams ---- */

static void dn_packets_append(dn_handle_t *owner, dispatch_data_t content, const struct sockaddr *from,
                              socklen_t from_len) {
  size_t size = content ? dispatch_data_get_size(content) : 0;
  dn_packet_t *p = calloc(1, sizeof(dn_packet_t) + size);
  p->len = size;
  if (from && from_len > 0 && from_len <= sizeof(p->from)) {
    memcpy(&p->from, from, from_len);
    p->from_len = from_len;
  }
  if (size > 0) {
    __block size_t at = 0;
    dispatch_data_apply(content, ^bool(dispatch_data_t region, size_t offset, const void *buffer, size_t length) {
      memcpy(p->bytes + at, buffer, length);
      at += length;
      return true;
    });
  }
  if (owner->packets_tail) owner->packets_tail->next = p;
  else owner->packets_head = p;
  owner->packets_tail = p;
}

/* Copies the socket address of `endpoint` into `h->remote`, if it has one. */
static void dn_remote_from(dn_handle_t *h, nw_endpoint_t endpoint) {
  if (endpoint == NULL) return;
  const struct sockaddr *address = nw_endpoint_get_address(endpoint);
  if (address) {
    socklen_t len = address->sa_family == AF_INET6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);
    memcpy(&h->remote, address, len);
    h->remote_len = len;
  }
}

/* Learns a flow's peer, once it is connected, from its path's remote endpoint, which is a numeric address even when
 * the connection's own endpoint is not. Outside the lock. */
static void dn_learn_remote(dn_handle_t *h) {
  nw_path_t path = nw_connection_copy_current_path(h->connection);
  if (path == NULL) return;
  nw_endpoint_t remote = nw_path_copy_effective_remote_endpoint(path);
  pthread_mutex_lock(&h->lock);
  if (h->remote_len == 0) dn_remote_from(h, remote);
  pthread_mutex_unlock(&h->lock);
  if (remote) nw_release(remote);
  nw_release(path);
}

/* Issues a receive of one datagram on `h`'s connection, under the lock; what arrives is queued on `owner`, `h` itself
 * or the bound socket a flow belongs to, with the sender `from`, and `owner`'s read waiter notified. A flow keeps one
 * receive outstanding at all times, so that its datagrams are taken in. */
static void dn_receive_datagram(dn_handle_t *h, dn_handle_t *owner) {
  h->receiving = 1;
  dn_handle_retain(h);
  nw_connection_receive_message(h->connection, ^(dispatch_data_t content, nw_content_context_t context,
                                                 bool is_complete, nw_error_t error) {
    if (h->remote_len == 0 && owner != h) dn_learn_remote(h);
    if (owner != h) pthread_mutex_lock(&owner->lock);
    pthread_mutex_lock(&h->lock);
    h->receiving = 0;
    int again = 0;
    if (content != NULL && !owner->closed) {
      dn_packets_append(owner, content, (const struct sockaddr *)&h->remote, h->remote_len);
    }
    if (error && !h->closed) {
      h->read_error = dn_errno(error);
      if (owner == h) owner->read_error = h->read_error;
    } else if (!h->closed && h->state == 1 && owner != h) again = 1;
    if (again) dn_receive_datagram(h, owner);
    dn_wait_t *w = dn_take(&owner->read_wait);
    pthread_mutex_unlock(&h->lock);
    if (owner != h) pthread_mutex_unlock(&owner->lock);
    dn_post_if(w);
    dn_handle_release(h);
  });
}

/* A datagram socket connected to `host`:`port`, bound to `local_host`:`local_port` if `local_host` is given. */
dn_handle_t *dn_datagram_connect(const char *host, int port, const char *local_host, int local_port) {
  dn_handle_t *h = dn_handle_new(DN_DGRAM);
  nw_endpoint_t endpoint = dn_endpoint(host, port);
  nw_parameters_t parameters = dn_udp_parameters();
  if (local_host) {
    nw_endpoint_t local = dn_endpoint(local_host, local_port);
    nw_parameters_set_local_endpoint(parameters, local);
    nw_parameters_set_reuse_local_address(parameters, true);
    nw_release(local);
  }
  h->connection = nw_connection_create(endpoint, parameters);
  nw_release(endpoint);
  nw_release(parameters);
  dn_connection_start(h);
  return h;
}

/* A flow of a bound datagram socket: a connection to one remote peer, inbound (from the listener) or opened to send. */
static dn_handle_t *dn_flow(dn_handle_t *bound, nw_connection_t connection, int started) {
  dn_handle_t *flow = dn_handle_new(DN_FLOW);
  flow->connection = connection;
  flow->parent = bound;
  nw_endpoint_t endpoint = nw_connection_copy_endpoint(connection);
  dn_remote_from(flow, endpoint);
  if (endpoint) nw_release(endpoint);
  dn_handle_retain(bound); /* a flow's callbacks reach its parent */
  dn_connection_start(flow);
  pthread_mutex_lock(&bound->lock);
  pthread_mutex_lock(&flow->lock);
  flow->next_flow = bound->flows;
  bound->flows = flow;
  dn_receive_datagram(flow, bound);
  pthread_mutex_unlock(&flow->lock);
  pthread_mutex_unlock(&bound->lock);
  return flow;
}

/* A datagram socket bound to `host`:`port`, receiving from anyone and sending to anyone. Each peer that sends to it is
 * a flow, as Network.framework has it. */
dn_handle_t *dn_datagram_bind(const char *host, int port, int *error) {
  dn_handle_t *h = dn_handle_new(DN_DGRAM_BOUND);
  h->parameters = dn_udp_parameters();
  nw_parameters_set_reuse_local_address(h->parameters, true);
  dn_restrict_family(h->parameters, host);
  h->local_endpoint = dn_endpoint(host, port);
  nw_parameters_set_local_endpoint(h->parameters, h->local_endpoint);
  h->listener = nw_listener_create(h->parameters);
  if (h->listener == NULL) {
    *error = EINVAL;
    dn_handle_release(h);
    return NULL;
  }
  nw_listener_set_new_connection_handler(h->listener, ^(nw_connection_t connection) {
    pthread_mutex_lock(&h->lock);
    int closed = h->closed;
    pthread_mutex_unlock(&h->lock);
    if (closed) {
      nw_connection_cancel(connection);
      return;
    }
    nw_retain(connection);
    dn_flow(h, connection, 0);
  });
  *error = dn_listener_start(h);
  if (*error) {
    nw_listener_cancel(h->listener);
    dn_handle_release(h);
    return NULL;
  }
  return h;
}

/* The receive op: the next datagram into `buf`, up to `cap`, truncated beyond, with its sender into `from` (0 bytes
 * of it for a connected socket, whose sender is its peer); its size, DN_WAITING, DN_ERROR, DN_CLOSED or DN_BUSY. */
int dn_receive_from(dn_handle_t *h, uint8_t *buf, int cap, struct sockaddr_storage *from, socklen_t *from_len,
                    dn_reactor_t *reactor, long token) {
  pthread_mutex_lock(&h->lock);
  int result;
  if (h->closed) result = DN_CLOSED;
  else if (h->packets_head) {
    dn_packet_t *p = h->packets_head;
    h->packets_head = p->next;
    if (!h->packets_head) h->packets_tail = NULL;
    size_t n = p->len < (size_t)cap ? p->len : (size_t)cap;
    memcpy(buf, p->bytes, n);
    memcpy(from, &p->from, sizeof(*from));
    *from_len = p->from_len;
    free(p);
    result = (int)n;
    if (h->kind == DN_DGRAM && !h->receiving && h->state == 1 && !h->read_error) dn_receive_datagram(h, h);
  } else if (h->read_error) result = DN_ERROR;
  else {
    result = dn_register(&h->read_wait, reactor, token, DN_READ);
    if (result == DN_WAITING && h->kind == DN_DGRAM && !h->receiving) dn_receive_datagram(h, h);
  }
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* Sends one datagram from a bound socket to `host`:`port`: on the flow of that peer if it has sent to us, or on a
 * new flow from the bound address otherwise. As `dn_write` for the rest. */
int dn_send_to(dn_handle_t *h, const uint8_t *bytes, int len, const char *host, int port, dn_reactor_t *reactor,
               long token) {
  if (h->kind != DN_DGRAM_BOUND) return dn_write(h, bytes, len, reactor, token);
  nw_endpoint_t endpoint = dn_endpoint(host, port);
  pthread_mutex_lock(&h->lock);
  if (h->closed) {
    pthread_mutex_unlock(&h->lock);
    nw_release(endpoint);
    return DN_CLOSED;
  }
  /* The flow for this peer, by the numeric address and port. */
  dn_handle_t *flow = NULL;
  struct sockaddr_storage target;
  socklen_t target_len = 0;
  {
    /* Parse the numeric host ourselves: endpoints of type host carry no sockaddr. */
    struct sockaddr_in *v4 = (struct sockaddr_in *)&target;
    struct sockaddr_in6 *v6 = (struct sockaddr_in6 *)&target;
    memset(&target, 0, sizeof(target));
    if (strchr(host, ':')) {
      v6->sin6_family = AF_INET6;
      v6->sin6_port = htons(port);
      if (inet_pton(AF_INET6, host, &v6->sin6_addr) == 1) target_len = sizeof(*v6);
    } else {
      v4->sin_family = AF_INET;
      v4->sin_port = htons(port);
      if (inet_pton(AF_INET, host, &v4->sin_addr) == 1) target_len = sizeof(*v4);
    }
  }
  for (dn_handle_t *f = h->flows; f && target_len; f = f->next_flow) {
    if (f->remote_len == target_len) {
      int same = 0;
      if (target_len == sizeof(struct sockaddr_in)) {
        struct sockaddr_in *a = (struct sockaddr_in *)&f->remote, *b = (struct sockaddr_in *)&target;
        same = a->sin_port == b->sin_port && a->sin_addr.s_addr == b->sin_addr.s_addr;
      } else {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)&f->remote, *b = (struct sockaddr_in6 *)&target;
        same = a->sin6_port == b->sin6_port && memcmp(&a->sin6_addr, &b->sin6_addr, sizeof(b->sin6_addr)) == 0;
      }
      if (same) {
        flow = f;
        break;
      }
    }
  }
  pthread_mutex_unlock(&h->lock);
  if (!flow) {
    nw_connection_t connection = nw_connection_create(endpoint, h->parameters);
    flow = dn_flow(h, connection, 1);
    if (flow->remote_len == 0 && target_len) {
      memcpy(&flow->remote, &target, target_len);
      flow->remote_len = target_len;
    }
  }
  nw_release(endpoint);
  pthread_mutex_lock(&h->lock);
  int result = dn_send_on(h, flow->connection, bytes, len, reactor, token);
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* ---- Descriptors: pipes and FIFOs, through Dispatch sources ---- */

dn_handle_t *dn_fd_open(int fd) {
  dn_handle_t *h = dn_handle_new(DN_FD);
  h->fd = fd;
  return h;
}

/* Creates the source for `read`, under the lock, active: it fires once the descriptor is ready, takes the waiter,
 * suspends itself, and the next wait resumes it. The descriptor is closed by the last source's cancel handler. */
static dispatch_source_t dn_source(dn_handle_t *h, int read) {
  dispatch_source_t source =
      dispatch_source_create(read ? DISPATCH_SOURCE_TYPE_READ : DISPATCH_SOURCE_TYPE_WRITE, h->fd, 0, dn_queue());
  dn_handle_retain(h);
  dispatch_source_set_event_handler(source, ^{
    pthread_mutex_lock(&h->lock);
    dn_wait_t *w = dn_take(read ? &h->read_wait : &h->write_wait);
    if (!h->closed) {
      dispatch_suspend(source);
      if (read) h->read_suspended = 1;
      else h->write_suspended = 1;
    }
    pthread_mutex_unlock(&h->lock);
    dn_post_if(w);
  });
  dispatch_source_set_cancel_handler(source, ^{
    pthread_mutex_lock(&h->lock);
    if (read) h->read_source = NULL;
    else h->write_source = NULL;
    int last = h->read_source == NULL && h->write_source == NULL;
    pthread_mutex_unlock(&h->lock);
    if (last) close(h->fd);
    dispatch_release(source);
    dn_handle_release(h);
  });
  dispatch_activate(source);
  return source;
}

/* Waits for the descriptor to be readable, or writable: DN_WAITING, DN_BUSY or DN_CLOSED. */
int dn_fd_wait(dn_handle_t *h, int read, dn_reactor_t *reactor, long token) {
  pthread_mutex_lock(&h->lock);
  int result;
  if (h->closed) result = DN_CLOSED;
  else {
    result = dn_register(read ? &h->read_wait : &h->write_wait, reactor, token, read ? DN_READ : DN_WRITE);
    if (result == DN_WAITING) {
      if (read) {
        if (!h->read_source) h->read_source = dn_source(h, 1);
        else if (h->read_suspended) {
          h->read_suspended = 0;
          dispatch_resume(h->read_source);
        }
      } else {
        if (!h->write_source) h->write_source = dn_source(h, 0);
        else if (h->write_suspended) {
          h->write_suspended = 0;
          dispatch_resume(h->write_source);
        }
      }
    }
  }
  pthread_mutex_unlock(&h->lock);
  return result;
}

/* ---- Closing ---- */

/* Closes a handle, from any thread: its waiters are notified as closed, so their ops fail on their reactors; its
 * connection, listener or sources are cancelled, and a descriptor is closed once its sources are; and the owner's
 * reference goes. Network.framework's last callback, or the last source's cancel handler, frees it. A listener's close
 * waits until it is cancelled, so that its port is free when this returns; a connection's does not wait. */
void dn_close(dn_handle_t *h) {
  pthread_mutex_lock(&h->lock);
  if (h->closed) {
    pthread_mutex_unlock(&h->lock);
    return;
  }
  h->closed = 1;
  dn_wait_t *c = dn_take(&h->connect_wait), *r = dn_take(&h->read_wait), *w = dn_take(&h->write_wait);
  dn_handle_t *accepted = h->accepted_head;
  h->accepted_head = h->accepted_tail = NULL;
  dn_handle_t *flows = h->flows;
  h->flows = NULL;
  dispatch_source_t rs = h->read_source, ws = h->write_source;
  int resume_r = h->read_suspended, resume_w = h->write_suspended;
  h->read_suspended = h->write_suspended = 0;
  if (h->listener && h->state != 3) h->cancelled = dispatch_semaphore_create(0);
  dispatch_semaphore_t cancelled = h->cancelled;
  /* A connection with sends outstanding is cancelled once they have gone, as a kernel socket's close sends what its
   * buffer holds: cancelling at once would drop them. Cancellation then fails the receives outstanding. */
  int cancel_now = h->connection == NULL || h->sends == 0;
  if (!cancel_now) h->cancel_pending = 1;
  pthread_mutex_unlock(&h->lock);
  dn_post_closed(c);
  dn_post_closed(r);
  dn_post_closed(w);
  while (accepted) {
    dn_handle_t *next = accepted->next_accepted;
    dn_close(accepted);
    accepted = next;
  }
  while (flows) {
    dn_handle_t *next = flows->next_flow;
    dn_close(flows);
    flows = next;
  }
  if (h->connection && cancel_now) nw_connection_cancel(h->connection);
  if (h->listener) {
    /* A listener's close waits for its cancelled state, so that its port can be listened on again at once. */
    nw_listener_cancel(h->listener);
    if (cancelled) {
      dispatch_semaphore_wait(cancelled, DISPATCH_TIME_FOREVER);
      dispatch_release(cancelled);
    }
  }
  if (h->kind == DN_FD) {
    if (rs) {
      if (resume_r) dispatch_resume(rs);
      dispatch_source_cancel(rs);
    }
    if (ws) {
      if (resume_w) dispatch_resume(ws);
      dispatch_source_cancel(ws);
    }
    if (!rs && !ws) close(h->fd);
  }
  if (h->kind == DN_FLOW && h->parent) dn_handle_release(h->parent);
  dn_handle_release(h);
}

#endif /* __APPLE__ */
