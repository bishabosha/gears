# The Darwin backend: Network.framework as a reactor

`asyncio.darwin.DarwinBackend` runs `asyncio`'s ops over Apple's Network.framework, the user-space networking stack
Apple offers instead of BSD sockets (see "SwiftNIO meets the same lock" in `asyncio/kqueue/TODO.md`), through its C
interface, with Dispatch for what it does not cover. It was built to answer two questions: whether the `Reactor`,
`Ops` and `Handles` semantics can be kept over a completion-based framework, and what it costs against kqueue on the
same scenarios.

## What it is

- **Sockets are Network.framework's**: `nw_connection_t` for stream and datagram connections, `nw_listener_t` for
  listeners; a bound, unconnected datagram socket is a UDP listener whose inbound flows, one `nw_connection_t` per
  peer, are received from and replied to (`dn_datagram_bind`, `dn_send_to`).
- **Pipes and FIFOs are Dispatch sources** (`DISPATCH_SOURCE_TYPE_READ`/`WRITE`) over non-blocking descriptors, since
  Network.framework has nothing for descriptors; regular files are read and written at submission, as they never wait.
- **Nothing calls back into Scala.** Network.framework delivers everything as blocks on Dispatch's threads, which
  Scala Native cannot run managed code on. The C side (`src/main/resources/scala-native/darwin_io.c`) keeps what the
  blocks deliver on the handle, bytes in a stash, datagrams as packets, accepted connections as a queue, the
  connection's state and errors, and posts a notification to the reactor whose op waits for it; the reactor's thread
  drains its queue each turn and parks on a condition variable between turns, bounded by its earliest timer. So every
  completion runs on the loop thread, as `Reactor` promises, and an op is a waiter registered on the handle, attempted
  again when notified.
- **The rest is `KqueueReactor`'s**: timers in a user-space heap (`asyncio.unsafe.TimerHeap`, now shared), blocking
  ops and `resolve` on the shared blocker pool (`asyncio.unsafe.Blockers`, now shared), completions of ops finished at
  submission queued for the next turn, `trySubmit` finishing them on the caller.
- Every executable that links it needs `-framework Network` (`networkFramework` in build.sbt).

## Semantics kept, and how

- **A read completes with at least one byte, or at the end of the stream.** A receive asks for 1 to `remaining`
  bytes; what arrives goes to the handle's stash, and a read takes from the stash. After a read that took bytes the
  next receive is issued at once (`ASYNCIO_DARWIN_PREFETCH=0` turns that off), so the next read finds its bytes
  waiting and finishes at submission, which is how `trySubmit` finishes a read without suspending the caller.
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
- **`listen` fails where it is called, and a closed listener's port is free at once.** A listener is asynchronous;
  `dn_listen` waits for its ready or failed state, so that a port in use throws `IOException` from `handles.listen`,
  as the contract has it, and `dn_close` waits for its cancelled state, so that the port can be listened on again
  when `close` returns (the futures contract's `freePort` depends on it). A connection's close does not wait.
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
  `NWEndpoint.unix` is private API. So `DarwinBackend` is neither a `UnixStreamBackend` nor a `UnixDatagramBackend`,
  the gears demos that need Unix sockets do not compile against it, and the bench compares it with kqueue over TCP.
- **No readiness.** There is nothing to try on a connection without waiting, and no event says a connection is
  readable: `whenReady`, `readNow` and `writeNow` have no counterpart, so it is not a `ReadinessBackend`. The
  contract tests that used readiness (`writeNow` to feed a pipe, `whenReady`, `trySubmit` finishing a read of waiting
  bytes) were moved to `ReadinessContract` and `ReadinessFuturesChecks`, which kqueue and NIO keep, and the base
  `ReactorContract` and `FuturesContract` now use ops alone, so that they run on every backend.
- **`listen` and a listener's `close` block for a moment**, until the listener is ready, or cancelled, where the
  contract says handle operations never block.
- **Sender of a datagram on a connected socket** is its peer, as given at creation: Network.framework reports no
  per-packet sender on a connection; a bound socket's flows report theirs.
- **A bound datagram socket is a listener with flows.** Network.framework opens a flow per peer that sends to it, each
  with a receive outstanding, so packets are taken in as they arrive, per peer, rather than from one socket buffer;
  sending to a peer that has not sent first opens an outbound flow from the bound address. Flows Network.framework
  ends on its own (a failed state) are not reclaimed.

## Measured: against kqueue over TCP

The gears bench on the same release build (`kqueue-demo/bench/build.sh WORKTREE`, Scala Native nightly, release-fast),
kqueue and Darwin in turn, over loopback TCP since Darwin has no Unix sockets, median of 3 runs, µs an operation
(`kqueue-demo/bench/results/native-darwin-tcp.json`, `-4-workers.json`, `-connections.json`; `connections` at a tenth
of its scale and 2 runs, since 20,000 connections exhaust the ephemeral ports, and after a pause for `TIME_WAIT`):

| scenario | kqueue, 1 worker | Darwin, 1 worker | kqueue, 4 workers | Darwin, 4 workers |
|---|---|---|---|---|
| pingpong | 15.9 | 58.7 (+268%) | 16.6 | 58.9 (+256%) |
| pingpong-fresh-ops | 16.5 | 58.5 | 17.0 | 59.1 |
| pingpong-deadline | 17.0 | 59.4 | 17.3 | 59.7 |
| concurrent | 3.67 | 20.1 (+448%) | 4.71 | 20.1 |
| concurrent-compute | 7.48 | 21.2 | 4.62 | 21.4 |
| concurrent-offload | 8.34 | 20.8 | 5.04 | 22.1 |
| connections | 61.0 | 413 (+577%) | | |
| pipelined | 0.75 | 19.1 (+2445%) | 0.74 | 22.3 |
| blocking | 6.45 | 6.34 | 6.91 | 7.16 |
| blocking-io | 28.0 | 68.6 (+145%) | 28.7 | 69.7 |
| timers | 0.21 | 0.21 | 0.21 | 0.21 |
| fairness | 1.59 | 12.1 | 1.66 | 14.3 |
| fairness-tick | 3.18 | 1.09 (-66%) | 3.32 | 1.21 |
| starvation (round trips) | 7.54 | 18.2 | 6.62 | 18.9 |
| starvation-p50 | 422 | 131 (-69%) | 31.0 | 37.4 |
| starvation-p99 | 641 | 437 (-32%) | 142 | 131 |
| starvation-max | 749 | 722 | 384 | 561 |
| mixed | 24.1 | 66.7 | 17.1 | 61.3 |
| mixed-compute | 24.1 | 22.0 | 34.3 | 21.0 |

- **Round trips cost 3.5 times what kqueue's do** (59 against 16 µs a ping-pong), and a connection 7 times. A read
  or write on kqueue is one system call on the reactor's thread, woken once by the kernel; on Darwin it is a hand-off
  to Network.framework, whose Dispatch thread does the socket call, posts to the reactor's queue, and wakes the reactor
  thread: two thread wake-ups where kqueue has one, each about what the earlier C measurement found
  (`network-framework.c`: 60 against 27 µs a round trip). Nothing in the backend's own path is close to that: a
  waiter is a `malloc`, a notification a mutex and condition variable.
- **Pipelined writes cost 25 times**: every write waits for its send's completion, as the framework advises, where
  kqueue's write returns once the kernel took the bytes. A window of outstanding sends per handle would bring this down
  (a write finishing at once while fewer than N sends are outstanding, which is what a kernel socket buffer is) and is
  the one evident improvement left; it was not tried, since round trips, not throughput, are what the backend is slow
  at.
- **Blocking ops and timers cost the same**: they never touch Network.framework. `blocking` is the blocker pool's
  hand-off, shared code.
- **Responsiveness is better**: the ticker and the starvation checker wake sooner (`fairness-tick` 1.1 against
  3.2 µs, `starvation-p50` 131 against 422 µs on one worker), because the reactor's thread does no socket calls and
  parks on a condition variable, which a Dispatch thread signals, while kqueue's thread is in `kevent` and the socket
  calls themselves; on four workers kqueue catches up. The compute slices (`mixed-compute`) are not slower either.
- **Knobs make little difference** (Darwin alone, 2 runs): checking the queue 2,000 times before parking
  (`ASYNCIO_DARWIN_SPIN=2000`) takes `pipelined` from 19 to 13 µs and `starvation-p99` from 437 to 423, and costs
  `fairness-tick` (1.1 to 8.6 µs); no receive prefetch (`ASYNCIO_DARWIN_PREFETCH=0`) changes nothing beyond noise.
  Both stay off by default.
- **Four workers change nothing for Darwin**: its socket calls are on Dispatch's threads already, and its round trips
  are bound by the hand-offs; kqueue's `concurrent` costs more on four workers (4.7 against 3.7 µs), the descriptor
  lock again.

So, as the earlier measurement had it, Network.framework over loopback is the kernel's sockets with more threads in
the way, and as a backend it loses every I/O scenario to kqueue by a factor of two and a half to seven; what it is for,
Apple's user-space stack on a real interface, needs a second machine to measure. The semantics survive, except where
the framework has no counterpart (above), and the backend is kept as the reference for a completion-based reactor,
which io_uring's would resemble.
