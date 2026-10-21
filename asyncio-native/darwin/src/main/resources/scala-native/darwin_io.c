/* The C side of `asyncio.darwin`: only what Scala Native cannot do itself. Network.framework and Dispatch deliver
 * everything as blocks, which Scala Native cannot build, on Dispatch's threads, which it cannot run managed code on. So
 * each block here is a trampoline: it copies its arguments into an event, queues the event on the handle it is for,
 * and wakes the reactor watching that handle. The reactor's thread, a Scala thread, drains the handles with events
 * (`dn_reactor_drain`), takes their events (`dn_handle_next`), and does everything else in Scala: what the bytes mean,
 * who waits for them, what to issue next. The one wait here, `dn_reactor_wait`, parks that thread until a handle has
 * events or `dn_reactor_wake` is called.
 *
 * A handle (`dn_handle`) is a connection, a listener, a flow of a bound datagram socket (whose events go to the bound
 * socket), or a descriptor with its Dispatch sources. It is freed once the Scala side has released it and the
 * framework has delivered its last callback, the cancelled state, or the last source's cancellation handler, which
 * also closes a descriptor. `dn_listen` waits for the listener to be ready or to fail, and `dn_cancel` of a listener
 * for its cancelled state, so that listening fails where it is called and a closed listener's port is free at once.
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
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* Kinds of event, in `dn_event.kind`. */
enum {
  DN_EV_RECEIVED = 1,  /* bytes of a stream, or none: `len`, `complete`, `final`, `error` */
  DN_EV_SENT = 2,      /* a send was processed: `len` bytes, `error` */
  DN_EV_READY = 3,     /* the connection is ready */
  DN_EV_FAILED = 4,    /* the connection or listener failed: `error` */
  DN_EV_CANCELLED = 5, /* the framework's last callback */
  DN_EV_ACCEPTED = 6,  /* a listener accepted `accepted`, a started connection handle */
  DN_EV_PACKET = 7,    /* a datagram: `len` bytes from `from`, on `flow` if a bound socket's */
  DN_EV_FLOW = 8,      /* a bound datagram socket has a new inbound flow, `flow` */
  DN_EV_READABLE = 9,  /* a descriptor's read source fired */
  DN_EV_WRITABLE = 10  /* a descriptor's write source fired */
};

/* Kinds of handle. */
enum { DN_STREAM = 1, DN_LISTENER = 2, DN_DGRAM = 3, DN_DGRAM_BOUND = 4, DN_FLOW = 5, DN_FD = 6 };

typedef struct dn_reactor dn_reactor_t;
typedef struct dn_handle dn_handle_t;

/* An event, as `dn_handle_next` copies it out: mirrored by `DarwinIo.Event` in Scala, field for field. */
typedef struct dn_event {
  struct dn_event *next;
  int kind;
  int error;      /* an errno, 0 for none */
  int complete;   /* a receive's `is_complete` */
  int final;      /* its context is the stream's final one, so `complete` means the end of the stream */
  size_t len;     /* bytes received, or sent */
  void *data;     /* unused */
  void *map;      /* unused */
  uint8_t *bytes; /* the received bytes, `len` of them, copied in the callback; freed by `dn_event_done` */
  dn_handle_t *flow;    /* the flow of a bound datagram socket the event is for, else NULL */
  dn_handle_t *accepted;
  socklen_t from_len;
  struct sockaddr_storage from;
} dn_event_t;

struct dn_reactor {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  dn_handle_t *head, *tail; /* handles with events, each linked once */
  int woken;
  int parked;
  _Atomic int ready;
};

struct dn_handle {
  int64_t tag;          /* the Scala side's, written and read there: the handle's number, so a drained handle is found
                           without a lookup; first, so that its offset is zero */
  pthread_mutex_t lock; /* guards the event queue and `watcher` */
  _Atomic int refs;     /* the owner's, plus one while the framework may still call back, plus one per source */
  int kind;
  nw_connection_t connection;
  nw_listener_t listener;
  nw_parameters_t parameters;   /* a bound datagram socket's, for the flows it opens */
  nw_endpoint_t local_endpoint; /* likewise, its own address */
  dn_event_t *head, *tail;
  dn_reactor_t *watcher;      /* the reactor to wake for events, or NULL */
  int watch_mask;             /* the kinds of event to wake it for, a bit per kind: the rest only queue */
  dn_handle_t *parent;        /* a flow's bound socket, whose queue its events go to */
  struct sockaddr_storage remote; /* a flow's peer */
  socklen_t remote_len;
  /* Under the watcher reactor's lock: whether linked in its list, and on which. */
  dn_handle_t *next_ready;
  dn_reactor_t *queued_on;
  /* A descriptor and its sources, each suspended between events. */
  int fd;
  dispatch_source_t read_source, write_source;
  int read_suspended, write_suspended;
  /* A listener's close waits on it for the cancelled state. */
  dispatch_semaphore_t cancelled;
  uint64_t trace_queued_at; /* tracing: when the last event was queued */
  /* Sends outstanding, and whether a cancel waits for them: cancelling drops sends still queued, where a kernel
   * socket's close sends what its buffer holds, so a closed connection is cancelled once its sends have gone. */
  int sends;
  int cancel_pending;
};


/* ---- Tracing, for measuring: ASYNCIO_DARWIN_TRACE=1 prints at exit how long events waited to be taken ---- */
#include <mach/mach_time.h>
static int dn_trace_on = -1;
static double dn_trace_ns_per_tick = 1.0;
static _Atomic uint64_t dn_trace_q_count, dn_trace_q_sum, dn_trace_q_max, dn_trace_cb_sum, dn_trace_cb_count;
static _Atomic uint64_t dn_trace_d_count, dn_trace_d_sum; /* queued -> drained by the reactor, before the Scala side */
static _Atomic uint64_t dn_trace_buckets[7]; /* <=2, <=5, <=10, <=20, <=50, <=100, >100 us */
static void dn_trace_report(void) {
  if (dn_trace_q_count == 0) return;
  fprintf(stderr, "dn trace: queued->drained: n=%llu mean=%.2fus\n", (unsigned long long)dn_trace_d_count,
          dn_trace_d_count ? dn_trace_d_sum * dn_trace_ns_per_tick / 1000.0 / dn_trace_d_count : 0.0);
  fprintf(stderr, "dn trace: queued->taken: n=%llu mean=%.2fus max=%.1fus; callback: mean=%.2fus; buckets us <=2:%llu <=5:%llu <=10:%llu <=20:%llu <=50:%llu <=100:%llu >100:%llu\n",
          (unsigned long long)dn_trace_q_count, dn_trace_q_sum * dn_trace_ns_per_tick / 1000.0 / dn_trace_q_count,
          dn_trace_q_max * dn_trace_ns_per_tick / 1000.0,
          dn_trace_cb_count ? dn_trace_cb_sum * dn_trace_ns_per_tick / 1000.0 / dn_trace_cb_count : 0.0,
          (unsigned long long)dn_trace_buckets[0], (unsigned long long)dn_trace_buckets[1], (unsigned long long)dn_trace_buckets[2],
          (unsigned long long)dn_trace_buckets[3], (unsigned long long)dn_trace_buckets[4], (unsigned long long)dn_trace_buckets[5],
          (unsigned long long)dn_trace_buckets[6]);
}
static int dn_tracing(void) {
  if (dn_trace_on < 0) {
    const char *v = getenv("ASYNCIO_DARWIN_TRACE");
    dn_trace_on = v != NULL && strcmp(v, "0") != 0;
    if (dn_trace_on) {
      mach_timebase_info_data_t tb;
      mach_timebase_info(&tb);
      dn_trace_ns_per_tick = (double)tb.numer / tb.denom;
      atexit(dn_trace_report);
    }
  }
  return dn_trace_on;
}
static void dn_trace_taken(uint64_t queued_at) {
  if (!queued_at) return;
  uint64_t d = mach_absolute_time() - queued_at;
  atomic_fetch_add(&dn_trace_q_count, 1);
  atomic_fetch_add(&dn_trace_q_sum, d);
  uint64_t m = dn_trace_q_max;
  while (d > m && !atomic_compare_exchange_weak(&dn_trace_q_max, &m, d)) {}
  double us = d * dn_trace_ns_per_tick / 1000.0;
  int b = us <= 2 ? 0 : us <= 5 ? 1 : us <= 10 ? 2 : us <= 20 ? 3 : us <= 50 ? 4 : us <= 100 ? 5 : 6;
  atomic_fetch_add(&dn_trace_buckets[b], 1);
}
static void dn_trace_callback(uint64_t entered_at) {
  atomic_fetch_add(&dn_trace_cb_count, 1);
  atomic_fetch_add(&dn_trace_cb_sum, mach_absolute_time() - entered_at);
}

/* ---- Reactors ---- */

dn_reactor_t *dn_reactor_create(void) {
  dn_reactor_t *r = calloc(1, sizeof(dn_reactor_t));
  pthread_mutex_init(&r->lock, NULL);
  pthread_cond_init(&r->cond, NULL);
  return r;
}

void dn_reactor_destroy(dn_reactor_t *r) {
  pthread_mutex_lock(&r->lock);
  dn_handle_t *h = r->head;
  while (h) {
    dn_handle_t *next = h->next_ready;
    h->next_ready = NULL;
    h->queued_on = NULL;
    h = next;
  }
  r->head = r->tail = NULL;
  pthread_mutex_unlock(&r->lock);
  pthread_mutex_destroy(&r->lock);
  pthread_cond_destroy(&r->cond);
  free(r);
}

void dn_reactor_wake(dn_reactor_t *r) {
  pthread_mutex_lock(&r->lock);
  r->woken = 1;
  atomic_store(&r->ready, 1);
  if (r->parked) pthread_cond_signal(&r->cond);
  pthread_mutex_unlock(&r->lock);
}

/* Whether a handle has events or a wake-up is pending, without blocking or locking. */
int dn_reactor_ready(dn_reactor_t *r) { return atomic_load(&r->ready); }

/* Waits until a handle has events, or `dn_reactor_wake` is called, or `timeout_ns` pass (negative: for ever). */
void dn_reactor_wait(dn_reactor_t *r, long long timeout_ns) {
  pthread_mutex_lock(&r->lock);
  if (!r->head && !r->woken) {
    r->parked = 1;
    if (timeout_ns < 0) {
      while (!r->head && !r->woken) pthread_cond_wait(&r->cond, &r->lock);
    } else {
      struct timespec rel = {.tv_sec = timeout_ns / 1000000000LL, .tv_nsec = timeout_ns % 1000000000LL};
      pthread_cond_timedwait_relative_np(&r->cond, &r->lock, &rel);
    }
    r->parked = 0;
  }
  r->woken = 0;
  atomic_store(&r->ready, r->head != NULL);
  pthread_mutex_unlock(&r->lock);
}

/* Takes up to `max` handles that have events into `out`. A pending wake-up is left for `dn_reactor_wait` to
 * consume: it may be for something queued on the Scala side after that side looked, which only the wait must not
 * miss; consuming it here lost it, once, and parked the reactor on a result already in its queue. */
int dn_reactor_drain(dn_reactor_t *r, dn_handle_t **out, int max) {
  if (!atomic_load(&r->ready)) return 0;
  pthread_mutex_lock(&r->lock);
  int n = 0;
  while (n < max && r->head) {
    dn_handle_t *h = r->head;
    r->head = h->next_ready;
    if (!r->head) r->tail = NULL;
    h->next_ready = NULL;
    h->queued_on = NULL;
    out[n++] = h;
    if (dn_trace_on > 0 && h->trace_queued_at) {
      atomic_fetch_add(&dn_trace_d_count, 1);
      atomic_fetch_add(&dn_trace_d_sum, mach_absolute_time() - h->trace_queued_at);
    }
  }
  atomic_store(&r->ready, r->head != NULL || r->woken);
  pthread_mutex_unlock(&r->lock);
  return n;
}

/* Links `h` into `r`'s list of handles with events, once, and wakes `r`. */
static void dn_reactor_offer(dn_reactor_t *r, dn_handle_t *h) {
  pthread_mutex_lock(&r->lock);
  if (h->queued_on == NULL) {
    h->queued_on = r;
    h->next_ready = NULL;
    if (r->tail) r->tail->next_ready = h;
    else r->head = h;
    r->tail = h;
    atomic_store(&r->ready, 1);
    if (r->parked) pthread_cond_signal(&r->cond);
  }
  pthread_mutex_unlock(&r->lock);
}

/* ---- Handles and their events ---- */

static dispatch_queue_t dn_queue(void) { return dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0); }

static dn_handle_t *dn_handle_new(int kind) {
  dn_handle_t *h = calloc(1, sizeof(dn_handle_t));
  pthread_mutex_init(&h->lock, NULL);
  h->kind = kind;
  h->fd = -1;
  atomic_store(&h->refs, 1);
  return h;
}

static void dn_handle_retain(dn_handle_t *h) { atomic_fetch_add(&h->refs, 1); }

static void dn_event_free(dn_event_t *e) {
  free(e->bytes);
  free(e);
}

/* Copies received content into the event, on the callback's thread, so that nothing of Dispatch's crosses to the
 * reactor's: it then touches no reference counts or mappings of the framework's objects. */
static void dn_event_copy(dn_event_t *e, dispatch_data_t content) {
  size_t size = dispatch_data_get_size(content);
  if (size == 0) return;
  uint8_t *bytes = malloc(size);
  __block size_t at = 0;
  dispatch_data_apply(content, ^bool(dispatch_data_t region, size_t offset, const void *buffer, size_t length) {
    memcpy(bytes + at, buffer, length);
    at += length;
    return true;
  });
  e->bytes = bytes;
  e->len = size;
}

/* Drops a reference; the last frees the handle, its queued events, and takes it off a reactor's list. */
void dn_handle_release(dn_handle_t *h) {
  if (atomic_fetch_sub(&h->refs, 1) != 1) return;
  pthread_mutex_lock(&h->lock);
  dn_reactor_t *r = h->queued_on;
  pthread_mutex_unlock(&h->lock);
  if (r) {
    pthread_mutex_lock(&r->lock);
    if (h->queued_on == r) {
      dn_handle_t **link = &r->head, *prev = NULL;
      while (*link && *link != h) {
        prev = *link;
        link = &(*link)->next_ready;
      }
      if (*link == h) {
        *link = h->next_ready;
        if (r->tail == h) r->tail = prev;
      }
      h->queued_on = NULL;
    }
    pthread_mutex_unlock(&r->lock);
  }
  dn_event_t *e = h->head;
  while (e) {
    dn_event_t *next = e->next;
    dn_event_free(e);
    e = next;
  }
  if (h->connection) nw_release(h->connection);
  if (h->listener) nw_release(h->listener);
  if (h->parameters) nw_release(h->parameters);
  if (h->local_endpoint) nw_release(h->local_endpoint);
  if (h->cancelled) dispatch_release(h->cancelled);
  pthread_mutex_destroy(&h->lock);
  free(h);
}

/* Queues `e` on `h`, or on the bound socket `h` is a flow of, and wakes the watcher. */
static void dn_enqueue(dn_handle_t *h, dn_event_t *e) {
  dn_handle_t *target = h->parent ? h->parent : h;
  if (h->parent) e->flow = h;
  e->next = NULL;
  pthread_mutex_lock(&target->lock);
  if (target->tail) target->tail->next = e;
  else target->head = e;
  target->tail = e;
  dn_reactor_t *watcher = (target->watch_mask & (1 << e->kind)) ? target->watcher : NULL;
  if (dn_tracing() && e->kind == DN_EV_RECEIVED) target->trace_queued_at = mach_absolute_time();
  pthread_mutex_unlock(&target->lock);
  if (watcher) dn_reactor_offer(watcher, target);
}

static dn_event_t *dn_event(int kind) {
  dn_event_t *e = calloc(1, sizeof(dn_event_t));
  e->kind = kind;
  return e;
}

/* Sets the reactor to wake for `h`'s events of the kinds in `mask`, a bit per kind, NULL for none; events of other
 * kinds only queue, for the next drain: a send's completion wakes nobody while no write waits for one. If events are
 * already queued, that reactor is woken. */
void dn_handle_watch(dn_handle_t *h, dn_reactor_t *r, int mask) {
  pthread_mutex_lock(&h->lock);
  h->watcher = r;
  h->watch_mask = mask;
  int pending = h->head != NULL;
  pthread_mutex_unlock(&h->lock);
  if (r && pending) dn_reactor_offer(r, h);
}

/* Copies the next queued event into `*out`: 1, or 0 if none. The caller frees what it holds with `dn_event_done`. */
int dn_handle_next(dn_handle_t *h, dn_event_t *out) {
  pthread_mutex_lock(&h->lock);
  dn_event_t *e = h->head;
  if (e) {
    h->head = e->next;
    if (!h->head) h->tail = NULL;
    if (e->kind == DN_EV_RECEIVED && dn_tracing()) {
      dn_trace_taken(h->trace_queued_at);
      h->trace_queued_at = 0;
    }
  }
  pthread_mutex_unlock(&h->lock);
  if (!e) return 0;
  *out = *e;
  out->next = NULL;
  free(e);
  return 1;
}

void dn_event_done(dn_event_t *e) {
  free(e->bytes);
  e->bytes = NULL;
}

/* The errno of a Network.framework error; `EIO` for one of another domain. */
static int dn_errno(nw_error_t error) {
  if (error == NULL) return 0;
  if (nw_error_get_error_domain(error) == nw_error_domain_posix) return nw_error_get_error_code(error);
  int code = nw_error_get_error_code(error);
  return code > 0 && code < 200 ? code : EIO;
}

/* ---- Connections ---- */

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

/* Installs the state handler of a connection, and starts it: ready, failed and cancelled become events; so does a
 * `waiting` state with an error, as failed, since the framework would otherwise keep a refused or unreachable
 * connection waiting for a path to appear, where POSIX `connect` fails. */
static void dn_connection_start(dn_handle_t *h) {
  dn_handle_retain(h); /* until the cancelled state, the framework's last callback */
  nw_connection_set_queue(h->connection, dn_queue());
  nw_connection_set_state_changed_handler(h->connection, ^(nw_connection_state_t state, nw_error_t error) {
    if (state == nw_connection_state_ready) dn_enqueue(h, dn_event(DN_EV_READY));
    else if (state == nw_connection_state_failed || (state == nw_connection_state_waiting && error != NULL)) {
      dn_event_t *e = dn_event(DN_EV_FAILED);
      e->error = dn_errno(error);
      dn_enqueue(h, e);
    } else if (state == nw_connection_state_cancelled) {
      dn_enqueue(h, dn_event(DN_EV_CANCELLED));
      dn_handle_release(h);
    }
  });
  nw_connection_start(h->connection);
}

/* A stream connection to `host`:`port`, started. */
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

/* Issues a receive of 1 to `max` bytes; what arrives is a `DN_EV_RECEIVED` event. */
void dn_receive(dn_handle_t *h, uint32_t max) {
  dn_handle_retain(h);
  nw_connection_receive(h->connection, 1, max,
                        ^(dispatch_data_t content, nw_content_context_t context, bool is_complete, nw_error_t error) {
                          uint64_t entered = dn_tracing() ? mach_absolute_time() : 0;
                          dn_event_t *e = dn_event(DN_EV_RECEIVED);
                          if (content) dn_event_copy(e, content);
                          e->complete = is_complete;
                          e->final = context != NULL && nw_content_context_get_is_final(context);
                          e->error = dn_errno(error);
                          dn_enqueue(h, e);
                          if (entered) dn_trace_callback(entered);
                          dn_handle_release(h);
                        });
}

/* Issues a receive of one datagram; it is a `DN_EV_PACKET` event, with the flow's peer as its sender. */
void dn_receive_message(dn_handle_t *h) {
  dn_handle_retain(h);
  nw_connection_receive_message(h->connection, ^(dispatch_data_t content, nw_content_context_t context,
                                                 bool is_complete, nw_error_t error) {
    dn_event_t *e = dn_event(DN_EV_PACKET);
    if (content) dn_event_copy(e, content);
    e->error = dn_errno(error);
    if (h->remote_len) {
      memcpy(&e->from, &h->remote, h->remote_len);
      e->from_len = h->remote_len;
    }
    dn_enqueue(h, e);
    dn_handle_release(h);
  });
}

/* Sends `len` bytes, copied, as one message: bytes of a stream, or one datagram; `DN_EV_SENT` once processed. */
void dn_send(dn_handle_t *h, const uint8_t *bytes, int len) {
  dispatch_data_t data = dispatch_data_create(bytes, len, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
  dn_handle_retain(h);
  pthread_mutex_lock(&h->lock);
  h->sends++;
  pthread_mutex_unlock(&h->lock);
  nw_connection_send(h->connection, data, NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT, true, ^(nw_error_t error) {
    dn_event_t *e = dn_event(DN_EV_SENT);
    e->len = len;
    e->error = dn_errno(error);
    pthread_mutex_lock(&h->lock);
    int cancel = --h->sends == 0 && h->cancel_pending;
    if (cancel) h->cancel_pending = 0;
    pthread_mutex_unlock(&h->lock);
    dn_enqueue(h, e);
    if (cancel) nw_connection_cancel(h->connection); /* the cancel that waited for this send */
    dn_handle_release(h);
  });
  nw_release(data);
}

/* ---- Listeners ---- */

/* Starts `h`'s listener and waits until it is ready, or failed: 0, or the errno. Its new connections, and a bound
 * datagram socket's new flows, are started and become events. */
static int dn_listener_start(dn_handle_t *h) {
  dispatch_semaphore_t settled = dispatch_semaphore_create(0);
  __block int outcome = -1;
  dn_handle_retain(h);
  nw_listener_set_queue(h->listener, dn_queue());
  nw_listener_set_state_changed_handler(h->listener, ^(nw_listener_state_t state, nw_error_t error) {
    if (state == nw_listener_state_ready) {
      if (outcome < 0) {
        outcome = 0;
        dispatch_semaphore_signal(settled);
      }
    } else if (state == nw_listener_state_failed) {
      int code = dn_errno(error);
      if (outcome < 0) {
        outcome = code ? code : EIO;
        dispatch_semaphore_signal(settled);
      } else {
        dn_event_t *e = dn_event(DN_EV_FAILED);
        e->error = code;
        dn_enqueue(h, e);
      }
    } else if (state == nw_listener_state_cancelled) {
      dn_enqueue(h, dn_event(DN_EV_CANCELLED));
      if (h->cancelled) dispatch_semaphore_signal(h->cancelled);
      dn_handle_release(h);
    }
  });
  nw_listener_start(h->listener);
  dispatch_semaphore_wait(settled, DISPATCH_TIME_FOREVER);
  dispatch_release(settled);
  return outcome;
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
    dn_handle_t *accepted = dn_handle_new(DN_STREAM);
    nw_retain(connection);
    accepted->connection = connection;
    dn_connection_start(accepted);
    dn_event_t *e = dn_event(DN_EV_ACCEPTED);
    e->accepted = accepted;
    dn_enqueue(h, e);
  });
  *error = dn_listener_start(h);
  if (*error) {
    nw_listener_cancel(h->listener);
    dn_handle_release(h);
    return NULL;
  }
  return h;
}

/* ---- Datagrams ---- */

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

/* Copies the socket address of `endpoint`, if it has one, as `h`'s peer. */
static void dn_remote_from(dn_handle_t *h, nw_endpoint_t endpoint) {
  if (endpoint == NULL) return;
  const struct sockaddr *address = nw_endpoint_get_address(endpoint);
  if (address) {
    socklen_t len = address->sa_family == AF_INET6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);
    memcpy(&h->remote, address, len);
    h->remote_len = len;
  }
}

/* A flow of a bound datagram socket: a connection to one peer, inbound from its listener or opened to send; its
 * events go to the bound socket's queue, naming the flow. */
static dn_handle_t *dn_flow(dn_handle_t *bound, nw_connection_t connection) {
  dn_handle_t *flow = dn_handle_new(DN_FLOW);
  flow->connection = connection;
  flow->parent = bound;
  nw_endpoint_t endpoint = nw_connection_copy_endpoint(connection);
  dn_remote_from(flow, endpoint);
  if (endpoint) nw_release(endpoint);
  dn_handle_retain(bound); /* a flow's events reach its parent */
  dn_connection_start(flow);
  return flow;
}

/* A datagram socket bound to `host`:`port`, receiving from anyone and sending to anyone, as Network.framework has it:
 * a listener whose inbound flows, one per peer, are `DN_EV_FLOW` events. */
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
    nw_retain(connection);
    dn_handle_t *flow = dn_flow(h, connection);
    dn_event_t *e = dn_event(DN_EV_FLOW);
    e->flow = flow;
    dn_enqueue(h, e);
  });
  *error = dn_listener_start(h);
  if (*error) {
    nw_listener_cancel(h->listener);
    dn_handle_release(h);
    return NULL;
  }
  return h;
}

/* Opens an outbound flow of a bound datagram socket to `host`:`port`, from the bound address. */
dn_handle_t *dn_flow_open(dn_handle_t *bound, const char *host, int port) {
  nw_endpoint_t endpoint = dn_endpoint(host, port);
  nw_connection_t connection = nw_connection_create(endpoint, bound->parameters);
  nw_release(endpoint);
  dn_handle_t *flow = dn_flow(bound, connection);
  if (flow->remote_len == 0) {
    struct sockaddr_in *v4 = (struct sockaddr_in *)&flow->remote;
    struct sockaddr_in6 *v6 = (struct sockaddr_in6 *)&flow->remote;
    if (strchr(host, ':')) {
      v6->sin6_family = AF_INET6;
      v6->sin6_port = htons(port);
      if (inet_pton(AF_INET6, host, &v6->sin6_addr) == 1) flow->remote_len = sizeof(*v6);
    } else {
      v4->sin_family = AF_INET;
      v4->sin_port = htons(port);
      if (inet_pton(AF_INET, host, &v4->sin_addr) == 1) flow->remote_len = sizeof(*v4);
    }
  }
  return flow;
}

/* A flow's peer, into `*from`: its length, 0 if not known. */
int dn_flow_remote(dn_handle_t *flow, struct sockaddr_storage *from) {
  if (flow->remote_len == 0) {
    /* Once connected, the path's remote endpoint is a numeric address even when the connection's own is not. */
    nw_path_t path = nw_connection_copy_current_path(flow->connection);
    if (path) {
      nw_endpoint_t remote = nw_path_copy_effective_remote_endpoint(path);
      dn_remote_from(flow, remote);
      if (remote) nw_release(remote);
      nw_release(path);
    }
  }
  memcpy(from, &flow->remote, sizeof(*from));
  return (int)flow->remote_len;
}

/* ---- Descriptors: pipes and FIFOs, through Dispatch sources ---- */

dn_handle_t *dn_fd_open(int fd) {
  dn_handle_t *h = dn_handle_new(DN_FD);
  h->fd = fd;
  return h;
}

/* Creates the source for `read`, active: it fires once the descriptor is ready, queues the event, and suspends
 * itself; the next `dn_fd_arm` resumes it. The last source's cancellation handler closes the descriptor. */
static dispatch_source_t dn_source(dn_handle_t *h, int read) {
  dispatch_source_t source =
      dispatch_source_create(read ? DISPATCH_SOURCE_TYPE_READ : DISPATCH_SOURCE_TYPE_WRITE, h->fd, 0, dn_queue());
  dn_handle_retain(h);
  dispatch_source_set_event_handler(source, ^{
    dispatch_suspend(source);
    pthread_mutex_lock(&h->lock);
    if (read) h->read_suspended = 1;
    else h->write_suspended = 1;
    pthread_mutex_unlock(&h->lock);
    dn_enqueue(h, dn_event(read ? DN_EV_READABLE : DN_EV_WRITABLE));
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

/* Arms the source for `read`: `DN_EV_READABLE` or `DN_EV_WRITABLE` once the descriptor is ready. */
void dn_fd_arm(dn_handle_t *h, int read) {
  pthread_mutex_lock(&h->lock);
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
  pthread_mutex_unlock(&h->lock);
}

/* ---- Cancelling ---- */

/* Cancels a handle's connection, listener or sources, from any thread. A connection's outstanding receives and
 * sends then complete with errors, and its cancelled state is its last event; a listener's close waits for that
 * state, so that its port is free when this returns; a descriptor is closed once its sources are cancelled. The
 * caller still holds its reference, released with `dn_handle_release`. A connection with sends outstanding is
 * cancelled once they have gone, since cancelling drops sends still queued. */
void dn_cancel(dn_handle_t *h) {
  if (h->connection) {
    pthread_mutex_lock(&h->lock);
    int now = h->sends == 0;
    if (!now) h->cancel_pending = 1;
    pthread_mutex_unlock(&h->lock);
    if (now) nw_connection_cancel(h->connection);
  }
  if (h->listener) {
    pthread_mutex_lock(&h->lock);
    if (!h->cancelled) h->cancelled = dispatch_semaphore_create(0);
    pthread_mutex_unlock(&h->lock);
    nw_listener_cancel(h->listener);
    dispatch_semaphore_wait(h->cancelled, DISPATCH_TIME_FOREVER);
  }
  if (h->kind == DN_FD) {
    pthread_mutex_lock(&h->lock);
    dispatch_source_t rs = h->read_source, ws = h->write_source;
    int resume_r = h->read_suspended, resume_w = h->write_suspended;
    h->read_suspended = h->write_suspended = 0;
    pthread_mutex_unlock(&h->lock);
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
}

#endif /* __APPLE__ */
