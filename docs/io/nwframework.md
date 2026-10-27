# The Network.framework backend: kqueue's reactors with Network.framework on macOS

`gears.async.io.nwframework.NWFrameworkBackend` (`ioNWFramework`, in `io-backends/nwframework`) is kqueue's reactors
with Apple's Network.framework, the user-space networking stack Apple offers in place of BSD sockets (see "SwiftNIO
meets the same lock" in [`kqueue.md`](kqueue.md)), where it serves best, and kqueue's own code for the rest. It is an
experiment and is not published. It answers two questions:

- **Can the `Reactor`, `Ops` and handle semantics be kept over a completion-based framework?** Yes, except where the
  public framework has no counterpart (see "What public Network.framework cannot keep"), which the backend now leaves
  to kqueue.
- **What does Network.framework cost against kqueue on the same scenarios?** Over loopback TCP it lost every I/O
  scenario by two to seven times, each operation a hand-off to a Dispatch thread and back (see "Measured against
  kqueue over TCP"), which is why loopback now goes to kqueue. What it is for, Apple's user-space stack on a real
  interface, needs a second machine to measure.

It stays as the reference for a completion-based facility under a reactor, which io_uring's would resemble, with one
difference that matters for cost: io_uring's completions land in a ring the submitting thread reads, with no second
thread between the kernel and the loop. The interface around it is described in [`README.md`](README.md), and the
reactor pool that drives it in [`runtime.md`](runtime.md).

## What it is

- **One reactor, kqueue's.** Each reactor is a `KqueueReactor`, so pipes, FIFOs, files, timers, blocking ops, Unix
  sockets and bound datagram sockets run exactly as on `KqueueBackend`. Network.framework's ops are `Notified` ops of
  kqueue's reactor: attempted when submitted, and again when Network.framework's side notifies the reactor for them.
- **Which goes where, as Apple advises.** TN3151 ("Choosing the right networking API") recommends Network.framework
  for TCP and for UDP between two peers, and BSD sockets for what it does not support, multicast, broadcast "and
  other asymmetric designs". So `connect`, `listen` and a connected datagram socket are Network.framework's; a Unix
  domain socket (which its public C interface has no endpoint for), a bound datagram socket (which it has no socket
  for, see below), pipes, FIFOs and files are kqueue's. `NWFrameworkOps` routes each opener by what it opens, and each
  op by its handle.
- **Loopback is kqueue's.** A socket whose address is on loopback (127.0.0.0/8, `::1`) goes to kqueue, as a Unix domain
  socket does: Network.framework carries loopback over the kernel's own sockets, at several times kqueue's cost and
  with nothing gained. A listener on the wildcard address is Network.framework's, and so are the connections it
  accepts, from loopback too. `NWFrameworkBackend.Everywhere` keeps every IP socket on Network.framework, loopback's
  included, for checking and measuring Network.framework itself: its contract suites run on loopback.
- **Nothing calls back into Scala, and C does only what Scala cannot.** Network.framework delivers everything as blocks,
  which Scala Native cannot build, on Dispatch's threads, which cannot run managed code. The C side
  (`src/main/resources/scala-native/nwframework_io.c`) is therefore trampolines: each block copies its arguments,
  received bytes included, into an event, queues it on the handle, and offers the handle to the reactor watching it,
  waking that reactor by triggering a user event on its kqueue, once until the reactor drains its list. The reactor
  drains the handles with events each turn (`NWFrameworkExtension`, a `ReactorExtension` of kqueue's), and polls
  `kevent` between turns, woken by that event as by any descriptor. Everything else is Scala, on `NWFrameworkHandle`:
  the received bytes in a stash, datagrams, accepted connections, the connection's state and errors, the send window,
  and the ops waiting with the reactor to tell for each. So every completion runs on the loop thread, as `Reactor`
  promises. The one piece of state C keeps is the count of sends outstanding, so that a closed connection is cancelled
  once they have gone, whichever reactor, if any, is still watching it.
- **The reactor says what to wake it for.** `nwf_handle_watch` names the reactor watching a handle and a mask of the
  event kinds an op there waits for; the other events only queue. Without it every send's completion woke the reactor,
  a spurious wake-up per message per side.
- **A handle is kqueue's, with a foreign object.** A Network.framework connection, listener or connected datagram
  socket is a `KqueueBackend.Handle` whose `foreign` object is its `NWFrameworkHandle`, its `fd` -1; the backend's
  kinds are opaque types of its own over that handle, so that a program cannot pass one to kqueue's ops, and kqueue's
  ops refuse a foreign handle passed as a raw `Handle` (`close`).
- **Every executable that links it needs `-framework Network`** (`networkFramework` in build.sbt), and gets kqueue's
  `setjmp` override (see [`kqueue.md`](kqueue.md)) with `gears-io-kqueue`. It runs the contract suites with loopback on
  kqueue (`NWFrameworkContractSuite`, Unix, FIFO and resolve contracts included) and with every socket on
  Network.framework (`NWFrameworkEverywhereContractSuite`, `NWFrameworkFuturesSuite`), and `HigherLevelChecks` both
  ways.

The environment variables `GEARS_IO_NWFRAMEWORK_SEND_WINDOW` (bytes, default 256 KB, 0 to make every write wait for its
send), `GEARS_IO_NWFRAMEWORK_PREFETCH` (0 turns the receive prefetch off) and `GEARS_IO_NWFRAMEWORK_TRACE` (1 prints at
exit how long events waited to be taken) exist for measuring; the defaults are the measured best.

## Why kqueue's reactor, not one of its own

Until 2026-10-27 the backend had a reactor of its own, which parked on a condition variable that the callbacks
signalled, and put pipes and FIFOs on Dispatch sources so that one wait covered everything; a bound datagram socket was
a UDP listener whose flows, a connection per peer, were received from and replied to. Nothing measured chose that
design; it followed from Network.framework calling back only on Dispatch's queues. It cost three things: descriptors
waited through Dispatch, with `select`'s thread besides for a FIFO's end, which kqueue's code already does; a bound
datagram socket emulated with flows, which could not keep a kernel socket's promises (an outbound flow from a fixed
port fails with `EADDRINUSE`, so a first send to a new peer was lost; flows idle for a minute were let go, and a packet
coming while one was cancelled was lost; see "What public Network.framework cannot keep"); and no Unix domain sockets
at all. Waking a kqueue with a user event costs about what signalling a condition variable does (`EVFILT_USER` 3.00 µs
against `os_sync` 2.78 µs, in [`runtime.md`](runtime.md)), so the reactor became kqueue's, and what Network.framework
does not do well went back to kqueue's code.

## Semantics kept, and how

- **A read completes with at least one byte, or at the end of the stream.** A receive asks for 1 to `remaining` bytes,
  which go to the handle's stash, and a read takes from the stash. After a read that took bytes the next receive is
  issued at once, so the next read finds its bytes waiting and finishes at submission, which is how `trySubmit`
  finishes a read without suspending the caller.
- **A pooled read skips the stash, and takes its buffer only when bytes come.** `ops.read(handle, pool, into)` that
  waits issues a receive whose event holds the framework's own content (`nwf_receive_held`), uncopied; as the reactor
  applies it, it takes a buffer of the pool's and copies the content in, once, and hands that buffer over as a
  `Received`: one copy, where a read into the caller's buffer costs the stash copy and another into the caller's. A
  pooled read cancelled or timed out before its bytes come holds no buffer, its bytes going to the stash for the next
  read, so idle connections with time limits never drain a shared pool (they held one each until 2026-10-27, when
  the receive wrote straight into a pool buffer taken as the read waited); a pool empty when the bytes come fails the
  read, as on kqueue. A plain read after a pooled one takes what the pooled receive brought first. The receive issued
  after a pooled read that took bytes goes to the stash, which the next pooled read copies.
- **Cancelling a read runs `onCancel` at once, and the op can be performed again.** A receive once issued cannot be
  withdrawn, so a cancelled read leaves it outstanding, and its bytes land in the stash for the next read. The op's
  buffer is written only on the loop thread, when the read is delivered, so cancel does not race a callback: it takes
  the op out of the reactor's waiting map and withdraws the waiter if it has not been told; a notification already
  queued is dropped when drained, by its token.
- **A write completes once at least one byte has been written, and hands over no more than the window has room
  for.** As many of the bytes remaining as the send window has room for are copied into a `dispatch_data_t` and sent
  at submission, the buffer's position advanced, and the write completes at once; with the window full it sends
  nothing and waits for a send's completion to report the stack has processed bytes, then tries again. Completions
  pace sends rather than serialise them, as Network.framework advises and as a kernel socket's send buffer does for
  kqueue. So a write that waited and was cancelled, or timed out, sent nothing, its buffer as it was, as on kqueue;
  until 2026-10-27 a write sent everything remaining at once, however large, a cancel finding it already sent.
- **Closing sends what was written, without waiting for it.** `nw_connection_cancel` drops sends still queued, where a
  kernel socket's close sends its buffer first; so the C side, which holds its own reference to the connection until
  its cancelled state, cancels a closed connection once its last send has completed, or after
  `NWF_LINGER_SECONDS` (60) if the peer stopped reading. The close op completes at once, as a kernel socket's close
  returns; until 2026-10-27 it waited for the cancelled state, for ever if the peer never read.
- **A connect completes once connected, or fails.** Network.framework keeps a refused or unreachable connection in its
  `waiting` state for a path to appear, rather than failing it; the backend takes `waiting` with an error as the
  failure, as POSIX `connect` reports it.
- **Opening and closing are ops that complete when the framework is done.** The `listen` op completes once the
  listener is ready, or fails with the address in use. The `close` op completes, for a connection, at once (above);
  for a listener, at its cancelled state, so that a port whose listener's close op has completed can be listened on
  again at once (completing it after `nwf_cancel_async` alone failed the contract now and then with "Address already
  in use"). A listener is started and its ready or failed state comes as an event, so no opener waits on the reactor's
  thread.
- **Closing a handle fails the ops pending on it, on whichever reactor.** The handle takes its waiters and tells each
  one's reactor, where the op fails with "The handle was closed"; then it cancels the connection, whose outstanding
  receives and sends complete with errors and find no waiter. The C side is freed by the framework's last callback,
  the cancelled state.
- **A connection both sides have ended is read to its end.** `shutdownOutput` is a final send
  (`NW_CONNECTION_FINAL_MESSAGE_CONTEXT`, `nwf_send_final`), TCP's FIN. Once both sides have sent theirs and the peer
  closes, the framework moves the connection to `failed`, with POSIX `ENETDOWN`, while it still holds what the peer
  sent and its end for receives to come. A stream that was ready therefore keeps reading after `failed`, which fails
  only its writes, until a receive brings the end or fails itself. `HigherLevelChecks`' graceful close, a 256 KiB
  reply, read 128 KiB and then failed with "Network is down" before; it passed 60 runs in a row after.
- **Handles are objects, numbered in a table.** Network.framework's objects are not descriptors, so a handle holds its
  `NWFrameworkHandle` as its foreign object, whose number in a `gears.async.io.internal.HandleTable`
  the C side carries as its first field, so a drained handle is found by number (`NWFrameworkHandles.at`) and checked
  against its C pointer. A closed handle fails every op later built or submitted on it (see [`handles.md`](handles.md)).
- **Ops move between reactors.** A waiter is registered with the reactor submitting the op, and the handle keeps no
  reactor of its own, so a repeatable op may be submitted to another reactor once its previous submission is over.

## What public Network.framework cannot keep

Each of these is now kqueue's in the backend (see "What it is").

- **No Unix domain sockets.** The C headers define endpoints of type address, host, Bonjour and URL only; Swift's
  `NWEndpoint.unix` is private API.
- **No readiness.** Nothing can be tried on a connection without waiting, and no event says a connection is readable.
  The interface has no readiness either (see "Readiness is kqueue's own" in [interface-history.md](interface-history.md)).
- **No bound datagram socket.** A UDP listener receives from anyone as flows, a connection per peer, but replying to a
  peer that has not sent first needs an outbound flow from the bound address, which fails with `EADDRINUSE` from a
  fixed port (probed on macOS 27.0, with `reuse_local_address` too), losing the packet; flows hold per-peer state that
  a kernel socket does not, and a packet arriving while a flow is cancelled is dropped. Apple's answer (TN3151, and
  Quinn on the developer forums: Network.framework "can't implement all possible UDP architectures") is BSD sockets.
- **The sender of a datagram on a connected socket** is its peer as given at creation: Network.framework reports no
  per-packet sender on a connection.

## The slim C side, measured

A first design kept all the state in C, with a waiter per op registered there (commit `1fd5fd7`). Moving it to Scala
halved the C and put the semantics next to the kqueue backend's, where they can be compared; it cost about 10 µs a
round trip. Release builds, ping-pong over loopback TCP, one worker, 3 runs on the same machine (the slim build from
`bench/results/native-darwin-tcp-slim*.json`, the others from the runs in "Measured against kqueue over TCP"):

| | state in C | slim | kqueue |
|---|---|---|---|
| ping-pong, µs a round trip | 55 | 65.6 | 17 |

`concurrent`, `pipelined` and `fairness` are level between the two (on four workers, slim: `concurrent` 19.6 µs,
`pipelined` 2.34). Three things were found and fixed on the way to that: the spurious wake-up per send completion
(the watch mask, above; `concurrent` 26.5 to 19.4 µs), the reactor's thread mapping and releasing the framework's
`dispatch_data` (the callback now copies the bytes; within noise on its own), and allocation per op on the Scala side,
a waiter object, a list per notification and a boxed token per wake-up, whose collections showed in the tails. Waiters
are now fields of the handle, notifications go to the reactor under the handle's lock, and tokens queue in a ring of
longs: ping-pong 65.6 against 69.0 µs, `starvation-p99` 556 against 589.

Where the remaining 10 µs go, profiled with `sample` and `GEARS_IO_NWFRAMEWORK_TRACE=1` against the state-in-C build:

- **Not CPU.** The reactor thread runs 2.8 µs a round trip against 1.65, Dispatch's threads the same in both, with no
  hot spot: allocation, the handle's `apply` and thread-local reads, each a few samples in a thousand.
- **Wait latency.** The callback costs the same (1.0 against 1.1 µs). An event waits 6.2 µs from being queued to being
  taken, against 4.8, and only 10% of events wait under 5 µs, against 60%: each hop reaches the reactor 1.4 µs later,
  3 µs of the round trip. The rest is the reactor's work between taking an event and issuing the next operation, 1 to
  2 µs a hop.
- **Not the wake-up.** A trace point at the drain, before the Scala side looks the handle up and locks it, measures
  the same in both builds. The 1.4 µs is the way into the handle: `NWFrameworkHandles.at` and the handle's
  `synchronized`, before `nwf_handle_next`.

Tried and dropped, within noise: keeping the reading reactor as the watcher so that a read registers no change of
watcher, and taking tokens out of the waiting map without an `Option`. Open: the handle's monitor, which Scala Native
may leave inflated once contended (it is, at close, from another thread); a spin lock of an `AtomicInteger` on the
handle would tell.

## The interface changes made for it

Three changes to the interface were made for a completion-based backend, implemented by every backend and now in the
base interface with the synchronous forms removed: reads into the backend's own buffers (`Backend.pool`,
`Ops.read(handle, pool, into)`); opening and closing as ops (`listen`, `close`); and `Submitter.submit` stating that a
turn's ops may be issued together at its end. Batching is not implemented: over loopback its gain is nil for a
framework whose cost is the hop. Opening and closing as ops removed this backend's two blocking waits, and the
semantic deviation they were, at no cost to the others.

The pooled read, release builds, 3 runs, µs a round trip (`pingpong`, 64-byte messages,
`bench/results/native-pooled-reads-tcp.json`) or a message (`large`, 128 KB messages written and read at once, as
`pipelined` does, `GEARS_BENCH_LARGE` setting the size; `native-large-unix.json`, `native-large-tcp.json`):

| | kqueue, Unix socket | kqueue, TCP | NWFramework, TCP |
|---|---|---|---|
| pingpong | 8.29 | 15.5 | 73.3 |
| pingpong-pooled | 11.0 | 15.5 | 72.9 |
| large | 78.1 | 41.0 | 396 |
| large-pooled | 77.6 | 40.0 | 373 |

At 64 bytes the saved copy is nothing against a thread wake-up. (kqueue's 2.7 µs over Unix sockets did not reproduce:
8.44 against 7.87 µs in later runs with `NativePool`'s lock-free free list, so a pooled read costs a readiness backend
a few hundred nanoseconds at most.) At 128 KB NWFramework saves 23 µs a message, two copies, 6% of its cost; kqueue
saves nothing measurable, its plain read copying once as the pooled one does. The change earns its place for a backend
that owns its buffers, this one and io_uring's multishot receive, and costs the others nothing.

## Measured against kqueue over TCP

These measurements are of the backend with its own reactor (see "Why kqueue's reactor"), and over loopback, which the
backend now leaves to kqueue; `--backend nwframework-everywhere` measures Network.framework there. The gears bench on
one release build (Scala Native nightly, release-fast), kqueue and NWFramework in turn, over loopback
TCP since NWFramework has no Unix sockets, median of 3 runs, µs an operation (`bench/results/native-darwin-tcp-window.json`,
`-window-4-workers.json`, `-window-connections.json`; `connections` at a tenth of its scale and 2 runs, since 20,000
connections exhaust the ephemeral ports). This is the state-in-C build with the send window; the slim build is about
10 µs a round trip dearer (above). How to run the bench is in [`bench.md`](bench.md): `compare.sc` takes a build as
`nwframework=<path>@nwframework` with `-- --tcp`, which runs `--backend nwframework-everywhere`.

| scenario | kqueue, 1 worker | NWFramework, 1 worker | kqueue, 4 workers | NWFramework, 4 workers |
|---|---|---|---|---|
| pingpong | 17.0 | 54.7 | 17.3 | 55.6 |
| concurrent | 3.73 | 19.2 | 5.02 | 19.4 |
| connections | 59.2 | 405 | | |
| pipelined | 0.74 | 2.40 | 0.77 | 2.30 |
| blocking | 6.58 | 6.54 | 6.94 | 7.27 |
| blocking-io | 28.1 | 66.4 | 28.7 | 68.2 |
| timers | 0.21 | 0.22 | 0.21 | 0.21 |
| fairness | 1.57 | 2.42 | 1.65 | 2.34 |
| fairness-tick | 3.15 | 2.22 | 3.30 | 3.14 |
| starvation-p50 | 446 | 85.5 | 29.3 | 19.3 |
| starvation-p99 | 688 | 395 | 199 | 43.1 |
| mixed | 24.6 | 66.0 | 17.0 | 58.9 |
| mixed-compute | 24.6 | 21.7 | 34.4 | 21.0 |

- **A round trip costs 3.2 times kqueue's, a connection 7 times.** A read or write on kqueue is one system call on the
  reactor's thread, woken once by the kernel; on NWFramework it is a hand-off to a Dispatch thread, which does the
  socket call, queues the event and wakes the reactor's thread: two thread wake-ups where kqueue has one. The
  framework used as Apple intends, with no reactor and each connection's work in blocks on a serial queue
  (`bench/network-framework.c`), measures 59.9 µs a round trip on the same machine, so the backend costs nothing over
  the idiomatic use; the cost is the framework's own path over loopback.
- **The send window is the idiom, and it matters.** With every write waiting for its send's completion, `pipelined`
  was 19.1 µs, `fairness` 12.1 and ping-pong 58.7; the window brings them to 2.4, 2.4 and 54.7.
- **Blocking ops and timers cost the same**: they never touch Network.framework.
- **Responsiveness is better.** The starvation checker wakes sooner (`starvation-p50` 86 against 446 µs on one worker,
  `-p99` 43 against 199 on four), because the reactor's thread does no socket calls and parks on a condition variable
  a Dispatch thread signals, while kqueue's thread is in `kevent` and the socket calls themselves. The compute slices
  (`mixed-compute`) are not slower.
- **Four workers change nothing for NWFramework**, whose socket calls are on Dispatch's threads already; kqueue's
  `concurrent` costs more on four (5.0 against 3.7 µs), the per-process descriptor lock described in
  [`kqueue.md`](kqueue.md).
- **QoS and spinning make no difference** (2 runs): the reactor thread at the queue's QoS class or left as it is, the
  queue user-interactive, user-initiated or default, ping-pong 54.4 to 54.9 µs. Checking for events 2,000 times before
  parking, and turning the receive prefetch off, did nothing useful either. The queue is user-initiated, and spinning
  is off.
- **The machine's own LAN address is loopback too.** en0's address is routed through `lo0` and measures as `127.0.0.1`
  does on both backends. A real interface needs a second machine running `iokqueuedemos gears bench-serve --host 0.0.0.0
  --port 19876 --backend nwframework` (or `kqueue`), benched with `--tcp --host <its address> --no-server`. On a LAN
  nothing rate-limits the traffic; over the internet a round trip's time is the network's, so the backends would
  differ in CPU and in throughput at concurrency (`concurrent`, `pipelined`), not in latency. Not measured yet.

## Known limits

- **macOS only, not published.**
- **Not measured since it moved onto kqueue's reactor.** The figures above are the backend with its own reactor;
  Network.framework's cost per hop is unchanged, but the reactor's wake-up and turn are kqueue's now.
- **The slim build's 10 µs over the state-in-C one** is open, at the handle's monitor (above).
