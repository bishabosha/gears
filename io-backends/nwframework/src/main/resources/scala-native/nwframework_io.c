/* The C side of `gears.async.io.nwframework`: only what Scala Native cannot do itself. Network.framework delivers
 * everything as blocks, which Scala Native cannot build, on Dispatch's threads, which it cannot run managed code on.
 * So each block here is a trampoline: it copies its arguments into an event, queues the event on the handle it is
 * for, and offers the handle to the reactor watching it, a kqueue reactor, which it wakes by triggering a user event
 * on its kqueue (`NWF_EXTENSION_EVENT`), once until that reactor drains it. The reactor's thread, a Scala thread,
 * drains the handles with events (`nwf_reactor_drain`), takes their events (`nwf_handle_next`), and does everything
 * else in Scala: what the bytes mean, who waits for them, what to issue next.
 *
 * A handle (`nwf_handle`) is a connection or a listener. It is freed once the Scala side has released it and the
 * framework has delivered its last callback, the cancelled state. `nwf_cancel` of a listener waits for that state, so
 * that a listener closed with it has let its port go.
 */
#ifdef __APPLE__

#include <Network/Network.h>
#include <arpa/inet.h>
#include <dispatch/dispatch.h>
#include <errno.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <unistd.h>

/* Kinds of event, in `nwf_event.kind`. */
enum {
  NWF_EV_RECEIVED = 1,  /* bytes of a stream, or none: `len`, `complete`, `final`, `error` */
  NWF_EV_SENT = 2,      /* a send was processed: `len` bytes, `error` */
  NWF_EV_READY = 3,     /* the connection is ready */
  NWF_EV_FAILED = 4,    /* the connection or listener failed: `error` */
  NWF_EV_CANCELLED = 5, /* the framework's last callback */
  NWF_EV_ACCEPTED = 6,  /* a listener accepted `accepted`, a started connection handle */
  NWF_EV_PACKET = 7     /* a datagram of `len` bytes, from the connection's peer */
};

/* Kinds of handle. */
enum { NWF_STREAM = 1, NWF_LISTENER = 2, NWF_DGRAM = 3 };

/* The user event the reactor's extension waits on, `KqueueReactor.ExtensionEvent`. */
#define NWF_EXTENSION_EVENT 1

typedef struct nwf_reactor nwf_reactor_t;
typedef struct nwf_handle nwf_handle_t;

/* An event, as `nwf_handle_next` copies it out: mirrored by `NWFrameworkIo.Event` in Scala, field for field. */
typedef struct nwf_event {
  struct nwf_event *next;
  int kind;
  int error;      /* an errno, 0 for none */
  int complete;   /* a receive's `is_complete` */
  int final;      /* its context is the stream's final one, so `complete` means the end of the stream */
  size_t len;     /* bytes received, or sent */
  void *data;     /* the framework's content, held for the reactor to copy (`nwf_receive_held`), else NULL */
  uint8_t *bytes; /* the received bytes, `len` of them, copied in the callback; freed by `nwf_event_done` */
  nwf_handle_t *accepted;
} nwf_event_t;

/* A kqueue reactor's side here: the handles with events for it, and its kqueue, whose user event wakes it. */
struct nwf_reactor {
  pthread_mutex_t lock;
  int kq;                    /* -1 once destroyed: nothing triggers it after */
  nwf_handle_t *head, *tail; /* handles with events, each linked once */
  int signalled;             /* the user event was triggered since the reactor last drained everything */
  _Atomic int ready;         /* handles are linked, for a check without the lock */
};

struct nwf_handle {
  int64_t tag;          /* the Scala side's, written and read there: the handle's number, so a drained handle is found
                           without a lookup; first, so that its offset is zero */
  pthread_mutex_t lock; /* guards the event queue and `watcher` */
  _Atomic int refs;     /* the owner's, plus one while the framework may still call back, plus one per source */
  int kind;
  nw_connection_t connection;
  nw_listener_t listener;
  nwf_event_t *head, *tail;
  nwf_reactor_t *watcher;      /* the reactor to wake for events, or NULL */
  int watch_mask;             /* the kinds of event to wake it for, a bit per kind: the rest only queue */
  /* Under the watcher reactor's lock: whether linked in its list, and on which. */
  nwf_handle_t *next_ready;
  nwf_reactor_t *queued_on;
  /* A listener's close waits on it for the cancelled state. */
  dispatch_semaphore_t cancelled;
  uint64_t trace_queued_at; /* tracing: when the last event was queued */
  /* Sends outstanding, and whether a cancel waits for them: cancelling drops sends still queued, where a kernel
   * socket's close sends what its buffer holds, so a closed connection is cancelled once its sends have gone. */
  int sends;
  int cancel_pending;
};


/* ---- Tracing, for measuring: GEARS_IO_NWFRAMEWORK_TRACE=1 prints at exit how long events waited to be taken ---- */
#include <mach/mach_time.h>
static int nwf_trace_on = -1;
static double nwf_trace_ns_per_tick = 1.0;
static _Atomic uint64_t nwf_trace_q_count, nwf_trace_q_sum, nwf_trace_q_max, nwf_trace_cb_sum, nwf_trace_cb_count;
static _Atomic uint64_t nwf_trace_d_count, nwf_trace_d_sum; /* queued -> drained by the reactor, before the Scala side */
static _Atomic uint64_t nwf_trace_buckets[7]; /* <=2, <=5, <=10, <=20, <=50, <=100, >100 us */
static void nwf_trace_report(void) {
  if (nwf_trace_q_count == 0) return;
  fprintf(stderr, "dn trace: queued->drained: n=%llu mean=%.2fus\n", (unsigned long long)nwf_trace_d_count,
          nwf_trace_d_count ? nwf_trace_d_sum * nwf_trace_ns_per_tick / 1000.0 / nwf_trace_d_count : 0.0);
  fprintf(stderr, "dn trace: queued->taken: n=%llu mean=%.2fus max=%.1fus; callback: mean=%.2fus; buckets us <=2:%llu <=5:%llu <=10:%llu <=20:%llu <=50:%llu <=100:%llu >100:%llu\n",
          (unsigned long long)nwf_trace_q_count, nwf_trace_q_sum * nwf_trace_ns_per_tick / 1000.0 / nwf_trace_q_count,
          nwf_trace_q_max * nwf_trace_ns_per_tick / 1000.0,
          nwf_trace_cb_count ? nwf_trace_cb_sum * nwf_trace_ns_per_tick / 1000.0 / nwf_trace_cb_count : 0.0,
          (unsigned long long)nwf_trace_buckets[0], (unsigned long long)nwf_trace_buckets[1], (unsigned long long)nwf_trace_buckets[2],
          (unsigned long long)nwf_trace_buckets[3], (unsigned long long)nwf_trace_buckets[4], (unsigned long long)nwf_trace_buckets[5],
          (unsigned long long)nwf_trace_buckets[6]);
}
static int nwf_tracing(void) {
  if (nwf_trace_on < 0) {
    const char *v = getenv("GEARS_IO_NWFRAMEWORK_TRACE");
    nwf_trace_on = v != NULL && strcmp(v, "0") != 0;
    if (nwf_trace_on) {
      mach_timebase_info_data_t tb;
      mach_timebase_info(&tb);
      nwf_trace_ns_per_tick = (double)tb.numer / tb.denom;
      atexit(nwf_trace_report);
    }
  }
  return nwf_trace_on;
}
static void nwf_trace_taken(uint64_t queued_at) {
  if (!queued_at) return;
  uint64_t d = mach_absolute_time() - queued_at;
  atomic_fetch_add(&nwf_trace_q_count, 1);
  atomic_fetch_add(&nwf_trace_q_sum, d);
  uint64_t m = nwf_trace_q_max;
  while (d > m && !atomic_compare_exchange_weak(&nwf_trace_q_max, &m, d)) {}
  double us = d * nwf_trace_ns_per_tick / 1000.0;
  int b = us <= 2 ? 0 : us <= 5 ? 1 : us <= 10 ? 2 : us <= 20 ? 3 : us <= 50 ? 4 : us <= 100 ? 5 : 6;
  atomic_fetch_add(&nwf_trace_buckets[b], 1);
}
static void nwf_trace_callback(uint64_t entered_at) {
  atomic_fetch_add(&nwf_trace_cb_count, 1);
  atomic_fetch_add(&nwf_trace_cb_sum, mach_absolute_time() - entered_at);
}

/* ---- Reactors ---- */

/* The side here of the kqueue reactor on `kq`, which has registered the user event `NWF_EXTENSION_EVENT`. */
nwf_reactor_t *nwf_reactor_create(int kq) {
  nwf_reactor_t *r = calloc(1, sizeof(nwf_reactor_t));
  pthread_mutex_init(&r->lock, NULL);
  r->kq = kq;
  return r;
}

/* Called as the reactor closes, before its kqueue does: no handle is offered to it, nor its kqueue triggered, after.
 * A handle still linked, whose watcher it no longer is, is unlinked. */
void nwf_reactor_destroy(nwf_reactor_t *r) {
  pthread_mutex_lock(&r->lock);
  nwf_handle_t *h = r->head;
  while (h) {
    nwf_handle_t *next = h->next_ready;
    h->next_ready = NULL;
    h->queued_on = NULL;
    h = next;
  }
  r->head = r->tail = NULL;
  r->kq = -1;
  pthread_mutex_unlock(&r->lock);
  pthread_mutex_destroy(&r->lock);
  free(r);
}

/* Whether a handle has events, without blocking or locking. */
int nwf_reactor_ready(nwf_reactor_t *r) { return atomic_load(&r->ready); }

/* Takes up to `max` handles that have events into `out`. Once none is left, the next offer triggers the user event
 * again; the reactor drains every turn, so handles left beyond `max` are taken on the next. */
int nwf_reactor_drain(nwf_reactor_t *r, nwf_handle_t **out, int max) {
  if (!atomic_load(&r->ready)) return 0;
  pthread_mutex_lock(&r->lock);
  int n = 0;
  while (n < max && r->head) {
    nwf_handle_t *h = r->head;
    r->head = h->next_ready;
    if (!r->head) r->tail = NULL;
    h->next_ready = NULL;
    h->queued_on = NULL;
    out[n++] = h;
    if (nwf_trace_on > 0 && h->trace_queued_at) {
      atomic_fetch_add(&nwf_trace_d_count, 1);
      atomic_fetch_add(&nwf_trace_d_sum, mach_absolute_time() - h->trace_queued_at);
    }
  }
  if (!r->head) r->signalled = 0;
  atomic_store(&r->ready, r->head != NULL);
  pthread_mutex_unlock(&r->lock);
  return n;
}

/* Links `h` into `r`'s list of handles with events, once, and wakes `r` by its user event, unless it already was
 * since it last drained everything: a poll blocked in `kevent` returns, and one to come finds the event. */
static void nwf_reactor_offer(nwf_reactor_t *r, nwf_handle_t *h) {
  pthread_mutex_lock(&r->lock);
  if (h->queued_on == NULL && r->kq >= 0) {
    h->queued_on = r;
    h->next_ready = NULL;
    if (r->tail) r->tail->next_ready = h;
    else r->head = h;
    r->tail = h;
    atomic_store(&r->ready, 1);
    if (!r->signalled) {
      r->signalled = 1;
      struct kevent trigger;
      EV_SET(&trigger, NWF_EXTENSION_EVENT, EVFILT_USER, 0, NOTE_TRIGGER, 0, NULL);
      kevent(r->kq, &trigger, 1, NULL, 0, NULL);
    }
  }
  pthread_mutex_unlock(&r->lock);
}

/* ---- Handles and their events ---- */

static dispatch_queue_t nwf_queue(void) { return dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0); }

static nwf_handle_t *nwf_handle_new(int kind) {
  nwf_handle_t *h = calloc(1, sizeof(nwf_handle_t));
  pthread_mutex_init(&h->lock, NULL);
  h->kind = kind;
  atomic_store(&h->refs, 1);
  return h;
}

static void nwf_handle_retain(nwf_handle_t *h) { atomic_fetch_add(&h->refs, 1); }

static void nwf_event_free(nwf_event_t *e) {
  free(e->bytes);
  if (e->data) dispatch_release((dispatch_data_t)e->data);
  free(e);
}

/* Copies received content into the event, on the callback's thread, so that nothing of Dispatch's crosses to the
 * reactor's: it then touches no reference counts or mappings of the framework's objects. */
static void nwf_event_copy(nwf_event_t *e, dispatch_data_t content) {
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
void nwf_handle_release(nwf_handle_t *h) {
  if (atomic_fetch_sub(&h->refs, 1) != 1) return;
  pthread_mutex_lock(&h->lock);
  nwf_reactor_t *r = h->queued_on;
  pthread_mutex_unlock(&h->lock);
  if (r) {
    pthread_mutex_lock(&r->lock);
    if (h->queued_on == r) {
      nwf_handle_t **link = &r->head, *prev = NULL;
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
  nwf_event_t *e = h->head;
  while (e) {
    nwf_event_t *next = e->next;
    nwf_event_free(e);
    e = next;
  }
  if (h->connection) nw_release(h->connection);
  if (h->listener) nw_release(h->listener);
  if (h->cancelled) dispatch_release(h->cancelled);
  pthread_mutex_destroy(&h->lock);
  free(h);
}

/* Queues `e` on `h`, and wakes the watcher if it watches for events of its kind. */
static void nwf_enqueue(nwf_handle_t *target, nwf_event_t *e) {
  e->next = NULL;
  pthread_mutex_lock(&target->lock);
  if (target->tail) target->tail->next = e;
  else target->head = e;
  target->tail = e;
  nwf_reactor_t *watcher = (target->watch_mask & (1 << e->kind)) ? target->watcher : NULL;
  if (nwf_tracing() && e->kind == NWF_EV_RECEIVED) target->trace_queued_at = mach_absolute_time();
  pthread_mutex_unlock(&target->lock);
  if (watcher) nwf_reactor_offer(watcher, target);
}

static nwf_event_t *nwf_event(int kind) {
  nwf_event_t *e = calloc(1, sizeof(nwf_event_t));
  e->kind = kind;
  return e;
}

/* Sets the reactor to wake for `h`'s events of the kinds in `mask`, a bit per kind, NULL for none; events of other
 * kinds only queue, for the next drain: a send's completion wakes nobody while no write waits for one. If events are
 * already queued, that reactor is woken. */
void nwf_handle_watch(nwf_handle_t *h, nwf_reactor_t *r, int mask) {
  pthread_mutex_lock(&h->lock);
  h->watcher = r;
  h->watch_mask = mask;
  int pending = h->head != NULL;
  pthread_mutex_unlock(&h->lock);
  if (r && pending) nwf_reactor_offer(r, h);
}

/* Copies the next queued event into `*out`: 1, or 0 if none. The caller frees what it holds with `nwf_event_done`. */
int nwf_handle_next(nwf_handle_t *h, nwf_event_t *out) {
  pthread_mutex_lock(&h->lock);
  nwf_event_t *e = h->head;
  if (e) {
    h->head = e->next;
    if (!h->head) h->tail = NULL;
    if (e->kind == NWF_EV_RECEIVED && nwf_tracing()) {
      nwf_trace_taken(h->trace_queued_at);
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

void nwf_event_done(nwf_event_t *e) {
  free(e->bytes);
  e->bytes = NULL;
  if (e->data) dispatch_release((dispatch_data_t)e->data);
  e->data = NULL;
}

/* The errno of a Network.framework error; `EIO` for one of another domain. */
static int nwf_errno(nw_error_t error) {
  if (error == NULL) return 0;
  if (nw_error_get_error_domain(error) == nw_error_domain_posix) return nw_error_get_error_code(error);
  int code = nw_error_get_error_code(error);
  return code > 0 && code < 200 ? code : EIO;
}

/* ---- Connections ---- */

static nw_parameters_t nwf_tcp_parameters(void) {
  return nw_parameters_create_secure_tcp(NW_PARAMETERS_DISABLE_PROTOCOL, NW_PARAMETERS_DEFAULT_CONFIGURATION);
}

static nw_parameters_t nwf_udp_parameters(void) {
  return nw_parameters_create_secure_udp(NW_PARAMETERS_DISABLE_PROTOCOL, NW_PARAMETERS_DEFAULT_CONFIGURATION);
}

static nw_endpoint_t nwf_endpoint(const char *host, int port) {
  char text[16];
  snprintf(text, sizeof(text), "%d", port);
  return nw_endpoint_create_host(host, text);
}

/* Restricts `parameters` to the IP version of `host`, so that a listener on "::1" is not also on IPv4; but not for
 * "::", which is dual stack, taking IPv4 peers too, as an IPv6 socket on the other backends does. */
static void nwf_restrict_family(nw_parameters_t parameters, const char *host) {
  if (strcmp(host, "::") == 0) return;
  nw_protocol_stack_t stack = nw_parameters_copy_default_protocol_stack(parameters);
  nw_protocol_options_t ip = nw_protocol_stack_copy_internet_protocol(stack);
  nw_ip_options_set_version(ip, strchr(host, ':') ? nw_ip_version_6 : nw_ip_version_4);
  nw_release(ip);
  nw_release(stack);
}

/* Installs the state handler of a connection, and starts it: ready, failed and cancelled become events; so does a
 * `waiting` state with an error, as failed, since the framework would otherwise keep a refused or unreachable
 * connection waiting for a path to appear, where POSIX `connect` fails. */
static void nwf_connection_start(nwf_handle_t *h) {
  nwf_handle_retain(h); /* until the cancelled state, the framework's last callback */
  nw_connection_set_queue(h->connection, nwf_queue());
  nw_connection_set_state_changed_handler(h->connection, ^(nw_connection_state_t state, nw_error_t error) {
    if (state == nw_connection_state_ready) nwf_enqueue(h, nwf_event(NWF_EV_READY));
    else if (state == nw_connection_state_failed || (state == nw_connection_state_waiting && error != NULL)) {
      nwf_event_t *e = nwf_event(NWF_EV_FAILED);
      e->error = nwf_errno(error);
      nwf_enqueue(h, e);
    } else if (state == nw_connection_state_cancelled) {
      nwf_enqueue(h, nwf_event(NWF_EV_CANCELLED));
      nwf_handle_release(h);
    }
  });
  nw_connection_start(h->connection);
}

/* A stream connection to `host`:`port`, started. */
nwf_handle_t *nwf_connect(const char *host, int port) {
  nwf_handle_t *h = nwf_handle_new(NWF_STREAM);
  nw_endpoint_t endpoint = nwf_endpoint(host, port);
  nw_parameters_t parameters = nwf_tcp_parameters();
  h->connection = nw_connection_create(endpoint, parameters);
  nw_release(endpoint);
  nw_release(parameters);
  nwf_connection_start(h);
  return h;
}

/* Issues a receive of 1 to `max` bytes; what arrives is a `NWF_EV_RECEIVED` event. */
void nwf_receive(nwf_handle_t *h, uint32_t max) {
  nwf_handle_retain(h);
  nw_connection_receive(h->connection, 1, max,
                        ^(dispatch_data_t content, nw_content_context_t context, bool is_complete, nw_error_t error) {
                          uint64_t entered = nwf_tracing() ? mach_absolute_time() : 0;
                          nwf_event_t *e = nwf_event(NWF_EV_RECEIVED);
                          if (content) nwf_event_copy(e, content);
                          e->complete = is_complete;
                          e->final = context != NULL && nw_content_context_get_is_final(context);
                          e->error = nwf_errno(error);
                          nwf_enqueue(h, e);
                          if (entered) nwf_trace_callback(entered);
                          nwf_handle_release(h);
                        });
}

/* Issues a receive of 1 to `max` bytes for a pooled read: the `NWF_EV_RECEIVED` event holds the framework's own
 * content as `data`, uncopied, so that the reactor copies it once into a pool buffer it takes only if a pooled read
 * still waits (`nwf_data_copy`); the content is let go with the event (`nwf_event_done`). */
void nwf_receive_held(nwf_handle_t *h, uint32_t max) {
  nwf_handle_retain(h);
  nw_connection_receive(h->connection, 1, max,
                        ^(dispatch_data_t content, nw_content_context_t context, bool is_complete, nw_error_t error) {
                          uint64_t entered = nwf_tracing() ? mach_absolute_time() : 0;
                          nwf_event_t *e = nwf_event(NWF_EV_RECEIVED);
                          if (content && dispatch_data_get_size(content) > 0) {
                            dispatch_retain(content);
                            e->data = (void *)content;
                            e->len = dispatch_data_get_size(content);
                          }
                          e->complete = is_complete;
                          e->final = context != NULL && nw_content_context_get_is_final(context);
                          e->error = nwf_errno(error);
                          nwf_enqueue(h, e);
                          if (entered) nwf_trace_callback(entered);
                          nwf_handle_release(h);
                        });
}

/* Copies the content an event holds (`nwf_receive_held`) into `into`, which has room for all of it. */
void nwf_data_copy(void *data, uint8_t *into) {
  __block size_t at = 0;
  dispatch_data_apply((dispatch_data_t)data, ^bool(dispatch_data_t region, size_t offset, const void *buffer, size_t length) {
    memcpy(into + at, buffer, length);
    at += length;
    return true;
  });
}

/* Issues a receive of one datagram; it is a `NWF_EV_PACKET` event. */
void nwf_receive_message(nwf_handle_t *h) {
  nwf_handle_retain(h);
  nw_connection_receive_message(h->connection, ^(dispatch_data_t content, nw_content_context_t context,
                                                 bool is_complete, nw_error_t error) {
    nwf_event_t *e = nwf_event(NWF_EV_PACKET);
    if (content) nwf_event_copy(e, content);
    e->error = nwf_errno(error);
    nwf_enqueue(h, e);
    nwf_handle_release(h);
  });
}

/* Sends `data`, which may be NULL, in `context`; `NWF_EV_SENT` once processed, with `len` as its length. */
static void nwf_send_in(nwf_handle_t *h, dispatch_data_t data, int len, nw_content_context_t context) {
  nwf_handle_retain(h);
  pthread_mutex_lock(&h->lock);
  h->sends++;
  pthread_mutex_unlock(&h->lock);
  nw_connection_send(h->connection, data, context, true, ^(nw_error_t error) {
    nwf_event_t *e = nwf_event(NWF_EV_SENT);
    e->len = len;
    e->error = nwf_errno(error);
    pthread_mutex_lock(&h->lock);
    int cancel = --h->sends == 0 && h->cancel_pending;
    if (cancel) h->cancel_pending = 0;
    pthread_mutex_unlock(&h->lock);
    nwf_enqueue(h, e);
    if (cancel) nw_connection_cancel(h->connection); /* the cancel that waited for this send */
    nwf_handle_release(h);
  });
}

/* Sends `len` bytes, copied, as one message: bytes of a stream, or one datagram; `NWF_EV_SENT` once processed. */
void nwf_send(nwf_handle_t *h, const uint8_t *bytes, int len) {
  dispatch_data_t data = dispatch_data_create(bytes, len, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
  nwf_send_in(h, data, len, NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT);
  nw_release(data);
}

/* Ends a stream connection's sending side, after what was sent before: TCP's FIN, as `shutdown(SHUT_WR)` sends,
 * the connection staying open for receiving. `NWF_EV_SENT`, with length 0, once processed. */
void nwf_send_final(nwf_handle_t *h) {
  nwf_send_in(h, NULL, 0, NW_CONNECTION_FINAL_MESSAGE_CONTEXT);
}

/* ---- Listeners ---- */

/* Starts `h`'s listener without waiting: its ready or failed state is an event, for the op opening it. Its new
 * connections are started and become events. */
static void nwf_listener_start(nwf_handle_t *h) {
  nwf_handle_retain(h);
  nw_listener_set_queue(h->listener, nwf_queue());
  nw_listener_set_state_changed_handler(h->listener, ^(nw_listener_state_t state, nw_error_t error) {
    if (state == nw_listener_state_ready) nwf_enqueue(h, nwf_event(NWF_EV_READY));
    else if (state == nw_listener_state_failed) {
      int code = nwf_errno(error);
      nwf_event_t *e = nwf_event(NWF_EV_FAILED);
      e->error = code ? code : EIO;
      nwf_enqueue(h, e);
    } else if (state == nw_listener_state_cancelled) {
      nwf_enqueue(h, nwf_event(NWF_EV_CANCELLED));
      if (h->cancelled) dispatch_semaphore_signal(h->cancelled);
      nwf_handle_release(h);
    }
  });
  nw_listener_start(h->listener);
}

/* A stream listener on `host`:`port`, started, its ready or failed state to come as an event; NULL if it cannot be
 * made. */
nwf_handle_t *nwf_listen(const char *host, int port) {
  nwf_handle_t *h = nwf_handle_new(NWF_LISTENER);
  nw_parameters_t parameters = nwf_tcp_parameters();
  nw_parameters_set_reuse_local_address(parameters, true); /* as SO_REUSEADDR, on every backend */
  nwf_restrict_family(parameters, host);
  nw_endpoint_t local = nwf_endpoint(host, port);
  nw_parameters_set_local_endpoint(parameters, local);
  nw_release(local);
  h->listener = nw_listener_create(parameters);
  nw_release(parameters);
  if (h->listener == NULL) {
    nwf_handle_release(h);
    return NULL;
  }
  nw_listener_set_new_connection_handler(h->listener, ^(nw_connection_t connection) {
    nwf_handle_t *accepted = nwf_handle_new(NWF_STREAM);
    nw_retain(connection);
    accepted->connection = connection;
    nwf_connection_start(accepted);
    nwf_event_t *e = nwf_event(NWF_EV_ACCEPTED);
    e->accepted = accepted;
    nwf_enqueue(h, e);
  });
  nwf_listener_start(h);
  return h;
}

/* ---- Datagrams ---- */

/* A datagram socket connected to `host`:`port`, bound to `local_host`:`local_port` if `local_host` is given. */
nwf_handle_t *nwf_datagram_connect(const char *host, int port, const char *local_host, int local_port) {
  nwf_handle_t *h = nwf_handle_new(NWF_DGRAM);
  nw_endpoint_t endpoint = nwf_endpoint(host, port);
  nw_parameters_t parameters = nwf_udp_parameters();
  if (local_host) {
    nw_endpoint_t local = nwf_endpoint(local_host, local_port);
    nw_parameters_set_local_endpoint(parameters, local);
    nw_parameters_set_reuse_local_address(parameters, true);
    nw_release(local);
  }
  h->connection = nw_connection_create(endpoint, parameters);
  nw_release(endpoint);
  nw_release(parameters);
  nwf_connection_start(h);
  return h;
}

/* The bytes a kernel UDP socket's receive buffer holds (`net.inet.udp.recvspace`), the most a datagram socket queues
 * before it drops what comes. */
int nwf_udp_receive_buffer(void) {
  int space = 0;
  size_t size = sizeof(space);
  if (sysctlbyname("net.inet.udp.recvspace", &space, &size, NULL, 0) != 0 || space <= 0) space = 786896;
  return space;
}

/* ---- This host's addresses ---- */

/* Whether `host`, numeric, is an address of one of this host's interfaces: traffic to it never leaves the host. */
int nwf_is_host_address(const char *host) {
  struct in_addr v4;
  struct in6_addr v6;
  int is_v6 = strchr(host, ':') != NULL;
  if (is_v6 ? inet_pton(AF_INET6, host, &v6) != 1 : inet_pton(AF_INET, host, &v4) != 1) return 0;
  struct ifaddrs *all;
  if (getifaddrs(&all) != 0) return 0;
  int found = 0;
  for (struct ifaddrs *i = all; i && !found; i = i->ifa_next) {
    if (!i->ifa_addr) continue;
    if (!is_v6 && i->ifa_addr->sa_family == AF_INET)
      found = memcmp(&((struct sockaddr_in *)i->ifa_addr)->sin_addr, &v4, sizeof v4) == 0;
    else if (is_v6 && i->ifa_addr->sa_family == AF_INET6)
      found = memcmp(&((struct sockaddr_in6 *)i->ifa_addr)->sin6_addr, &v6, sizeof v6) == 0;
  }
  freeifaddrs(all);
  return found;
}

/* ---- Cancelling ---- */

/* How long a closed connection's sends may take to go before it is cancelled all the same, dropping what is left,
 * as when a peer stops reading: the bytes a kernel socket's close leaves to the kernel, bounded. */
#define NWF_LINGER_SECONDS 60

/* Cancels a handle's connection or listener, from any thread. A connection's outstanding receives and sends then
 * complete with errors, and its cancelled state is its last event; a listener's close waits for that state, so that
 * its port is free when this returns. The
 * caller still holds its reference, released with `nwf_handle_release`. A connection with sends outstanding is
 * cancelled once they have gone, since cancelling drops sends still queued, or after `NWF_LINGER_SECONDS`. */
static void nwf_cancel_waiting(nwf_handle_t *h, int wait) {
  if (h->connection) {
    pthread_mutex_lock(&h->lock);
    int now = h->sends == 0;
    if (!now) h->cancel_pending = 1;
    pthread_mutex_unlock(&h->lock);
    if (now) nw_connection_cancel(h->connection);
    else {
      nwf_handle_retain(h);
      dispatch_after(dispatch_time(DISPATCH_TIME_NOW, NWF_LINGER_SECONDS * NSEC_PER_SEC), nwf_queue(), ^{
        pthread_mutex_lock(&h->lock);
        int cancel = h->cancel_pending;
        h->cancel_pending = 0;
        pthread_mutex_unlock(&h->lock);
        if (cancel) nw_connection_cancel(h->connection);
        nwf_handle_release(h);
      });
    }
  }
  if (h->listener) {
    if (wait) {
      pthread_mutex_lock(&h->lock);
      if (!h->cancelled) h->cancelled = dispatch_semaphore_create(0);
      pthread_mutex_unlock(&h->lock);
    }
    nw_listener_cancel(h->listener);
    if (wait) dispatch_semaphore_wait(h->cancelled, DISPATCH_TIME_FOREVER);
  }
}

void nwf_cancel(nwf_handle_t *h) { nwf_cancel_waiting(h, 1); }

/* As `nwf_cancel`, but a listener's cancellation is not waited for: its cancelled state comes as an event. */
void nwf_cancel_async(nwf_handle_t *h) { nwf_cancel_waiting(h, 0); }

#endif /* __APPLE__ */
