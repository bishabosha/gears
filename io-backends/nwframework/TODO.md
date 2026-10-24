# The NWFramework backend: Network.framework as a reactor

> This log was kept while the work lived in modules of its own, and names them as they were then. They are now:
> `asyncio` is `gears.async.io` in the core project; `asyncio.unsafe` and `asyncio.gears` (the runtime) are
> `gears.async.io.internal`, except `ReactorFutures` and `TakePolicy`, which are in `gears.async.io`; `asyncio.kqueue`,
> `asyncio.nio` and `asyncio.nwframework` are `gears.async.io.kqueue`, `.nio` and `.nwframework`, in `io-backends/`;
> the contract suites and `asyncio.testkit` are `gears.async.io.testkit` (`io-testkit`); `gears-demo` and `higherlevel`
> are `io-demos`; `kqueue-demo` is `io-backends/kqueue/demos` (`ioKqueueDemos`, executable `iokqueuedemos`), its
> tests `io-demos/tests` (`ioDemosTests`); and `kqueue-demo/bench` and `kqueue-demo/ce-bench` are `bench/` and
> `bench/ce-bench`.

`asyncio.nwframework.NWFrameworkBackend` runs `asyncio`'s ops over Apple's Network.framework, the user-space networking stack
Apple offers instead of BSD sockets (see "SwiftNIO meets the same lock" in `asyncio/kqueue/TODO.md`), through its C
interface, with Dispatch for what it does not cover. It was built to answer two questions: whether the `Reactor`,
`Ops` and `Handles` semantics can be kept over a completion-based framework, and what it costs against kqueue on the
same scenarios.

## What it is

- **Sockets are Network.framework's**: `nw_connection_t` for stream and datagram connections, `nw_listener_t` for
  listeners; a bound, unconnected datagram socket is a UDP listener whose inbound flows, one `nw_connection_t` per
  peer, are received from and replied to (`nwf_datagram_bind`, `nwf_send_to`).
- **Pipes and FIFOs are Dispatch sources** (`DISPATCH_SOURCE_TYPE_READ`/`WRITE`) over non-blocking descriptors, since
  Network.framework has nothing for descriptors; regular files are read and written at submission, as they never wait.
- **Nothing calls back into Scala, and C does only what Scala cannot.** Network.framework delivers everything as
  blocks, which Scala Native cannot build, on Dispatch's threads, which it cannot run managed code on. The C side
  (`src/main/resources/scala-native/nwframework_io.c`) is therefore trampolines: each block copies its arguments into an
  event, queues it on the handle, and wakes the reactor watching the handle; the reactor's thread drains the handles
  with events each turn and parks on a condition variable between turns, bounded by its earliest timer. Everything
  else is Scala, on `NWFrameworkHandle`: the received bytes in a stash, datagrams, accepted connections, the connection's
  state and errors, the ops waiting and which reactor to wake for them (`nwf_handle_watch`), the send window. So every
  completion runs on the loop thread, as `Reactor` promises, and an op is a waiter registered on the handle, attempted
  again when the handle tells its reactor. A first cut kept all that state in C, with a waiter per op registered
  there; moving it out halved the C and put the semantics next to the kqueue backend's, where they are compared. The
  one piece of state C keeps is the count of sends outstanding, so that a closed connection is cancelled once they
  have gone (below) whichever reactor, if any, is still watching it.
- **One wake-up, consumed only by the wait.** The reactor's wake-up flag on the C side is set by `wakeup` and by a
  blocker thread or another reactor queueing something on the Scala side; `nwf_reactor_wait` consumes it. Draining
  the handles with events used to consume it too, which lost a wake-up that landed between the Scala side emptying
  its queues and that drain: the reactor parked, with no timer to bound the park, on a blocking op's result already
  queued. It showed once in thirty runs of the demo suite, in `blocking`.
- **The rest is `KqueueReactor`'s**: timers in a user-space heap (`asyncio.unsafe.TimerHeap`, now shared), blocking
  ops and `resolve` on the shared blocker pool (`asyncio.Blockers`, shared by every backend, NIO too), completions of ops finished at
  submission queued for the next turn, `trySubmit` finishing them on the caller.
- Every executable that links it needs `-framework Network` (`networkFramework` in build.sbt).

## Semantics kept, and how

- **A read completes with at least one byte, or at the end of the stream.** A receive asks for 1 to `remaining`
  bytes; what arrives goes to the handle's stash, and a read takes from the stash. After a read that took bytes the
  next receive is issued at once (`ASYNCIO_NWFRAMEWORK_PREFETCH=0` turns that off), so the next read finds its bytes
  waiting and finishes at submission, which is how `trySubmit` finishes a read without suspending the caller.
- **A read into the backend's own buffer skips the stash.** `ops.read(handle, pool, into)`, now in the base
  interface for every backend, receives straight into a buffer of the pool's on Dispatch's thread, and the reactor
  hands that buffer over as a `Received`: one copy, the framework's own, where a read into the caller's buffer costs
  the stash copy and another into the caller's. A plain read after a pooled one takes what the pooled receive
  brought first.
- **Cancelling a read runs `onCancel` at once, and the op can be performed again.** A receive once issued cannot be
  withdrawn, so a cancelled read leaves it outstanding; its bytes land in the stash, for the next read on the handle.
  The op's buffer is never written from a Dispatch thread, only on the loop thread when the read is delivered, so
  cancel need not race with a callback: it takes the op out of the reactor's waiting map and withdraws the waiter if it
  has not been notified; a notification already posted is dropped when drained, by its token.
- **A write completes once at least one byte has been written.** The bytes remaining are copied into a
  `dispatch_data_t` and sent at submission, the buffer's position advanced then, and the op completes when the send's
  completion reports the stack has processed them, so pending writes are paced, as Network.framework advises.
  Cancelling a write leaves the buffer alone, the bytes already with the stack.
- **A connect completes once connected, or fails.** Network.framework keeps a refused or unreachable connection in its
  `waiting` state, for a path to appear, rather than failing it; the backend takes a `waiting` state with an error as
  the failure, as POSIX `connect` reports it.
- **Listening and closing are ops.** A listener is asynchronous, and so is a connection's cancellation; the interface
  now has `handles.listener` with the `listening` op, whose completion carries an address in use, and the `close` op,
  which completes once the handle is closed: for a connection, at its cancelled state, for a listener, at its, and
  for a descriptor at once. Nothing waits on a semaphore any more; the earlier synchronous `listen` and `close` are
  kept on `NWFrameworkHandles` only for code with no reactor.

- **Closing a handle fails the ops pending on it, on whichever reactor.** The C side takes the handle's waiters, each
  knowing its reactor, and notifies them as closed; then it cancels the connection, whose outstanding receives and
  sends complete with errors and find no waiter. The handle is freed by Network.framework's last callback, the
  cancelled state, or for a descriptor by the last Dispatch source's cancellation handler, which also closes it.
- **Handles are table indexes**, not descriptors, since Network.framework's objects are not descriptors; a number is
  free for reuse as soon as `close` returns, and an op built with it before then acts on the new handle, as with
  descriptors.
- **Ops move between reactors.** A waiter is registered with the reactor submitting the op, and the handle keeps no
  reactor of its own, so a repeatable op may be submitted to another reactor once its previous submission is over.

## Semantics that cannot be kept with public Network.framework

- **No Unix domain sockets.** The C headers define endpoints of type address, host, Bonjour and URL only; Swift's
  `NWEndpoint.unix` is private API. So `NWFrameworkBackend` is neither a `UnixStreamBackend` nor a `UnixDatagramBackend`,
  the gears demos that need Unix sockets do not compile against it, and the bench compares it with kqueue over TCP.
- **No readiness.** There is nothing to try on a connection without waiting, and no event says a connection is
  readable: `whenReady`, `readNow` and `writeNow` have no counterpart, so it is not a `ReadinessBackend`. The
  contract tests that used readiness (`writeNow` to feed a pipe, `whenReady`, `trySubmit` finishing a read of waiting
  bytes) were moved to `ReadinessContract` and `ReadinessFuturesChecks`, which kqueue and NIO keep, and the base
  `ReactorContract` and `FuturesContract` now use ops alone, so that they run on every backend.
- **Sender of a datagram on a connected socket** is its peer, as given at creation: Network.framework reports no
  per-packet sender on a connection; a bound socket's flows report theirs.
- **A bound datagram socket is a listener with flows.** Network.framework opens a flow per peer that sends to it, each
  with a receive outstanding, so packets are taken in as they arrive, per peer, rather than from one socket buffer;
  sending to a peer that has not sent first opens an outbound flow from the bound address. Flows Network.framework
  ends on its own (a failed state) are not reclaimed.

## The slim C side, measured

Moving the state out of C cost latency at first: ping-pong 65 µs against 55 with the state in C, `concurrent` 26
against 19, `fairness-tick` 4.8 against 2.2, on the same release build method. Three things explained it, each
measured on one worker over TCP, 3 runs:

- **Every send's completion woke the reactor**, where the C side had posted only when a write waited for one; with the
  send window nothing does, so that was a spurious wake-up per message per side. The reactor now tells the C side
  which kinds of event to wake it for (`nwf_handle_watch`'s mask, from what is waiting); the rest only queue.
  `concurrent` went back from 26.5 to 19.4 µs, ping-pong from 64.9 to 63.5.
- **The reactor's thread mapped and released the framework's `dispatch_data`**; the callback now copies the bytes
  into the event on Dispatch's thread, so nothing of Dispatch's crosses over. Within noise on its own.
- **Allocation per op on the Scala side**: a waiter object per registration, a list per notification, a boxed token
  per wake-up, where the C side had allocated one record per op. The collector stops every thread for each
  collection, which showed in the tails (`starvation-p50` 247 against 86 µs). Waiters are now fields of the handle,
  notifications go straight to the reactor under the handle's lock, and tokens queue in a ring of longs. Measured
  (`native-darwin-tcp-slim*.json`, the previous build beside it in the same runs): ping-pong 65.6 against 69.0 µs,
  `fairness-tick` 3.7 against 4.2, `starvation-p50` 207 against 220, `starvation-p99` 556 against 589, on one
  worker; on four, `concurrent` 19.6, `pipelined` 2.34. So the slim backend stands at about 10 µs a round trip over
  the version with its state in C (55 µs on the same machine, 17 for kqueue), with `concurrent`, `pipelined` and
  `fairness` level. What remains between them: the handle's Scala monitor, taken twice per event and once per
  attempt, and two more C calls per op (`nwf_handle_watch` on each registration and each take). Profiled, below.

### Where the slim backend's 10 µs a round trip go

Profiled on 2026-10-21 against the state-in-C build, both release, ping-pong over loopback TCP:

- **Not CPU.** `time` gives the slim build 7 µs more user time a round trip, but `sample` finds it spread thin: the
  reactor thread runs 2.8 µs a round trip against 1.65, and Dispatch's threads the same per operation in both. No
  hot spot: the reactor's extra is allocation (`Allocator_Alloc`, `memset`), the handle's `apply`, and thread-local
  reads, each a few samples in a thousand.
- **Wait latency, measured.** `ASYNCIO_NWFRAMEWORK_TRACE=1` times each received event from the callback queueing it to
  the reactor taking it, and the callback itself, on both builds (the state-in-C one instrumented in a worktree of
  its commit): the callback costs the same, 1.0 against 1.1 µs; the wait is 4.8 µs against 6.2, 60% of events under
  5 µs against 10%. So each hop reaches the reactor 1.4 µs later, 3 µs of the round trip, and the rest is the
  reactor's work between taking the event and issuing the next operation, 1 to 2 µs a hop.
- **What did not help**: keeping the reactor that reads a handle as its watcher so that a read registers no change
  of watcher on the C side, and taking tokens out of the waiting map without an `Option` each: ping-pong 72.9 against
  72.2 µs in the same runs (a slow run of the machine; kqueue 18.5), `concurrent` 25.4 against 23.5. Neither is kept.
- **Where the 1.4 µs is**: a second trace point, at the reactor's drain of the C side before the Scala side looks
  the handle up and locks it, measures 5.3 to 5.7 µs from queueing, the same as the state-in-C build measures at its
  drain (4.8 to 5.0, a faster run of the machine). So the wake-up itself costs the same in both; the 1.4 µs is the
  Scala side's way into the handle, `NWFrameworkHandles.at` and the handle's `synchronized`, before `nwf_handle_next`.
  That is more than a thin lock should cost, and the next thing to try is the monitor: Scala Native's, once a monitor
  has been contended (the handle's is, at close, from another thread), may stay inflated; a spin lock of an
  `AtomicInteger` on the handle would tell. The rest, 1 to 2 µs a hop, is the reactor's processing after it has the
  event. The state-in-C design is the previous commit (`1fd5fd7`), and the measurements are its.

## The interface, changed for it, measured

Three changes to `Reactor`'s interfaces were made for a completion-based backend, staged as capability traits,
implemented by NWFramework, kqueue and NIO in turn, then folded into the base interface with the synchronous forms
removed: reads into the backend's own buffers (`Backend.pool`, `Ops.read(handle, pool, into)`), listening and closing
as ops (`Handles.listener`, `Ops.listening`, `Ops.close`), and `Reactor.submit` stating that a turn's ops may be
issued together at its end. The `pingpong-pooled` scenario reads each echo into a pool buffer, which this backend
receives straight into on Dispatch's thread (`native-pooled-reads-tcp.json`, release, 3 runs, µs a round trip):

| | kqueue, Unix socket | kqueue, TCP | NWFramework, TCP |
|---|---|---|---|
| pingpong | 8.29 | 15.5 | 73.3 |
| pingpong-pooled | 11.0 | 15.5 | 72.9 |

So the saved copy does not show at 64 bytes a message, which is nothing against a thread wake-up. On kqueue over Unix
sockets a first run had the pooled read 2.7 µs dearer, which a later pair of runs did not reproduce (8.48 against
8.39 µs with the pool's free list under a lock, 8.44 against 7.87 with it lock-free, a compare-and-set stack of
slot indexes tagged against ABA, which is what `NativePool` is now): the pooled read costs a readiness backend a few
hundred nanoseconds at most, the slot handed over and taken back. At a size where a copy counts the saving shows: the `large`
scenarios send 128 KB messages (`GEARS_BENCH_LARGE` sets the size) as `pipelined` does, writing and reading at
once, since a ping-pong of messages larger than a Unix socket's 8 KB buffers deadlocks both sides on their writes;
`large-pooled` reads into pool buffers the size of a message (`native-large-unix.json`, `native-large-tcp.json`,
µs a message):

| | kqueue, Unix socket | kqueue, TCP | NWFramework, TCP |
|---|---|---|---|
| large | 78.1 | 41.0 | 396 |
| large-pooled | 77.6 | 40.0 | 373 |

NWFramework saves 23 µs a message, two copies of 128 KB, 6% of its cost; kqueue saves nothing measurable, its plain
read copying once, from the kernel into the caller's buffer, as the pooled one does into the pool's. The change
earns its place for a backend that owns its buffers, this one and io_uring's multishot receive, and costs the others
nothing at this size. The table's other lesson is Network.framework at bulk over loopback: nine times kqueue's cost
a message, against three and a half at 64 bytes. Listening and closing as ops removed this
backend's two blocking waits and the semantic deviation they were, at no cost to the others. The issue-point wording
changes nothing yet: batching was not implemented, its gain over loopback being nil for a framework whose cost is
the hop.

## Measured: against kqueue over TCP

The gears bench on the same release build (`kqueue-demo/bench/build.sh WORKTREE`, Scala Native nightly, release-fast),
kqueue and NWFramework in turn, over loopback TCP since NWFramework has no Unix sockets, median of 3 runs, µs an operation
(`kqueue-demo/bench/results/native-darwin-tcp-window.json`, `-4-workers.json`, `-connections.json`; `connections` at a
tenth of its scale and 2 runs, since 20,000 connections exhaust the ephemeral ports, and after a pause for
`TIME_WAIT`). The `native-darwin-tcp*.json` results without `window` are the first cut, before the send window.

| scenario | kqueue, 1 worker | NWFramework, 1 worker | kqueue, 4 workers | NWFramework, 4 workers |
|---|---|---|---|---|
| pingpong | 17.0 | 54.7 (+221%) | 17.3 | 55.6 (+222%) |
| pingpong-fresh-ops | 16.7 | 54.6 | 17.0 | 55.7 |
| pingpong-deadline | 16.9 | 55.6 | 17.2 | 55.9 |
| concurrent | 3.73 | 19.2 (+414%) | 5.02 | 19.4 |
| concurrent-compute | 7.44 | 20.5 | 4.88 | 19.6 |
| concurrent-offload | 8.42 | 19.8 | 5.14 | 21.5 |
| connections | 59.2 | 405 (+584%) | | |
| pipelined | 0.74 | 2.40 (+224%) | 0.77 | 2.30 |
| blocking | 6.58 | 6.54 | 6.94 | 7.27 |
| blocking-io | 28.1 | 66.4 (+136%) | 28.7 | 68.2 |
| timers | 0.21 | 0.22 | 0.21 | 0.21 |
| fairness | 1.57 | 2.42 | 1.65 | 2.34 |
| fairness-tick | 3.15 | 2.22 (-30%) | 3.30 | 3.14 |
| starvation (round trips) | 7.65 | 17.7 | 6.52 | 18.7 |
| starvation-p50 | 446 | 85.5 (-81%) | 29.3 | 19.3 |
| starvation-p99 | 688 | 395 (-43%) | 199 | 43.1 (-78%) |
| starvation-max | 764 | 621 | 509 | 535 |
| mixed | 24.6 | 66.0 | 17.0 | 58.9 |
| mixed-compute | 24.6 | 21.7 | 34.4 | 21.0 |

- **Round trips cost 3.2 times what kqueue's do** (55 against 17 µs a ping-pong), and a connection 7 times. A read
  or write on kqueue is one system call on the reactor's thread, woken once by the kernel; on NWFramework it is a hand-off
  to Network.framework, whose Dispatch thread does the socket call, posts to the reactor's queue, and wakes the reactor
  thread: two thread wake-ups where kqueue has one. The framework used as Apple intends, with no reactor and every
  connection's work in its own blocks on a serial queue (`kqueue-demo/bench/network-framework.c`), measured 59.9 µs a
  round trip on this machine, so the backend costs nothing over the idiomatic usage; what costs is the framework's own
  path over loopback. Nothing in the backend's own path is close to that: a waiter is a `malloc`, a notification a
  mutex and condition variable.
- **The send window matters, and is the idiom**: Network.framework asks that completions pace sends, not serialise
  them. The first cut had every write wait for its send's completion, a hand-off and a wake-up per write; now a write
  is done at once while the handle has less than 256 KB of sends outstanding (`ASYNCIO_NWFRAMEWORK_SEND_WINDOW`, 0 for
  the old behaviour). `pipelined` went from 19.1 to 2.4 µs, `fairness` from 12.1 to 2.4, ping-pong from 58.7 to 54.7.
  A kernel socket's send buffer does the same for kqueue, which is why its `pipelined` is 0.74. It needed one more
  piece of socket semantics: `nw_connection_cancel` drops sends still queued, so closing a connection right after a
  write lost the last message, where a kernel socket sends its buffer first; the close now cancels once the last send
  has completed.
- **Blocking ops and timers cost the same**: they never touch Network.framework. `blocking` is the blocker pool's
  hand-off, shared code.
- **Responsiveness is better**: the starvation checker wakes sooner (`starvation-p50` 86 against 446 µs on one
  worker, `-p99` 43 against 199 on four), because the reactor's thread does no socket calls and parks on a condition
  variable, which a Dispatch thread signals, while kqueue's thread is in `kevent` and the socket calls themselves. The
  compute slices (`mixed-compute`) are not slower either.
- **QoS makes no difference** (NWFramework alone, 2 runs): the reactor thread at the queue's QoS class or left as it is,
  the queue user-interactive, user-initiated or default, all within noise (ping-pong 54.4 to 54.9 µs). Checking the
  queue 2,000 times before parking and turning the receive prefetch off, measured on the first cut, did nothing useful
  either. All stay at their defaults: user-initiated, thread QoS set, no spinning, prefetch on.
- **Four workers change nothing for NWFramework**: its socket calls are on Dispatch's threads already, and its round trips
  are bound by the hand-offs; kqueue's `concurrent` costs more on four workers (5.0 against 3.7 µs), the descriptor
  lock again.

- **The machine's own LAN address is loopback too**: `--host 192.168.0.186` (en0's address; macOS routes it through
  `lo0`) measured the same as `127.0.0.1` on both backends, within noise (debug build: kqueue 22 against 25 µs a
  ping-pong, NWFramework 65 against 63). A real interface needs a second machine: on it, `kqueuedemo gears bench-serve
  --host 0.0.0.0 --port 19876 --backend kqueue` (or `nwframework`), and here `kqueuedemo gears bench --tcp --host <its
  address> --no-server --backend nwframework` against the same with `kqueue`, both as `compare.py ... -- --tcp --host ...
  --no-server`. On a LAN nothing rate-limits the traffic; on the internet a round trip's time is the network's, so the
  backends would differ only in CPU and in throughput at concurrency (`concurrent`, `pipelined`), not in latency.

So, as the earlier measurement had it, Network.framework over loopback is the kernel's sockets with more threads in
the way, and as a backend it loses every I/O scenario to kqueue by two to seven times, pipelined writes by three; what
it is for, Apple's user-space stack on a real interface, needs a second machine to measure. The semantics survive,
except where the framework has no counterpart (above), and the backend is kept as the reference for a completion-based
reactor, which io_uring's would resemble, with one difference that matters for cost: io_uring's completions land in a
ring the submitting thread reads, with no second thread between the kernel and the loop.

- **Done: a listener's close op waits for the cancelled state.** `NWFrameworkHandle.closeAsync` completed a
  listener's (and a bound datagram socket's) close at once after `nwf_cancel_async`, which does not wait for
  Network.framework to cancel it, so the port could still be held when the op said it was closed: the contract's
  "a closed listener's port can be listened on again once its close op completes" failed now and then with "Address
  already in use". The close op now registers its waiter and completes on the cancelled event, as a connection's
  does; descriptors and files still close at once, having no cancelled state. Seen in the futures suites' run of
  2026-10-23; the contract suite passed five runs in a row after.

- **Done: a connection both sides have ended is read to its end.** `SocketOps.shutdownOutput` is a final send
  (`NW_CONNECTION_FINAL_MESSAGE_CONTEXT`, `nwf_send_final`), TCP's FIN. Once both sides have sent theirs and the peer
  closes, the framework moves the connection to `failed`, with POSIX `ENETDOWN`, while it still holds what the peer
  sent and its end for receives to come: a log of the state handler and the receives showed the failed state, then a
  receive with the final, complete content and no error. The handle took `failed` as the end of reading, so the
  client lost the rest: in `HigherLevelChecks`' graceful close (a 256 KiB reply), it read 128 KiB and then failed with
  "Network is down", and with the server closing outright it read all 256 KiB and failed where the end should be. A
  stream that was ready now keeps reading after `failed`, which only fails its writes, until a receive brings the end
  or fails itself; the check passes on both reactor backends, and on this one 60 runs in a row.
- **Open: the back-pressure stream over a pipe hangs now and then.** `HigherLevelChecks`' "a stream of chunks
  consumed as a gears channel, with back-pressure" waits for ever on four workers, all reactors idle, about once in
  200 runs of it (repeated 30 to a process, 2026-10-24); first seen when the graceful-close check ran before it. It
  hangs the same with this backend as it was before `shutdownOutput`, so it is not that change's. Pipes here are
  descriptors waited on through a Dispatch source with pooled reads; a wake-up lost between the writer's close and
  the reader's next pooled read is the first thing to look at.
