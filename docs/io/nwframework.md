# The Network.framework backend: a completion-based reactor on macOS

`gears.async.io.nwframework.NWFrameworkBackend` (`ioNWFramework`, in `io-backends/nwframework`) runs the interface's
ops over Apple's Network.framework, the user-space networking stack Apple offers in place of BSD sockets (see "SwiftNIO
meets the same lock" in [`kqueue.md`](kqueue.md)), through its C interface, with Dispatch for what Network.framework
does not cover. It is an experiment and is not published. It was built to answer two questions:

- **Can the `Reactor`, `Ops` and handle semantics be kept over a completion-based framework?** Yes, except where the
  public framework has no counterpart: Unix domain sockets, readiness, and the sender of a datagram on a connected
  socket (see "What public Network.framework cannot keep").
- **What does it cost against kqueue on the same scenarios?** Over loopback TCP it loses every I/O scenario by two to
  seven times, because each operation is a hand-off to a Dispatch thread and back; it wakes sooner for a starved
  task, and timers and blocking ops cost the same. What it is for, Apple's user-space stack on a real interface, needs
  a second machine to measure (see "Measured against kqueue over TCP").

It stays as the reference for a completion-based reactor, which io_uring's would resemble, with one difference that
matters for cost: io_uring's completions land in a ring the submitting thread reads, with no second thread between the
kernel and the loop. The interface around it is described in [`README.md`](README.md), and the reactor pool that drives
it in [`runtime.md`](runtime.md).

## What it is

- **IP sockets are Network.framework's**: `nw_connection_t` for stream and datagram connections, `nw_listener_t` for
  listeners. A bound, unconnected datagram socket is a UDP listener whose flows, one `nw_connection_t` per peer, are
  received from and replied to (`nwf_datagram_bind`, `nwf_flow_open`).
- **Pipes and FIFOs are Dispatch sources** (`DISPATCH_SOURCE_TYPE_READ` and `WRITE`) over non-blocking descriptors,
  since Network.framework has nothing for descriptors. A regular file's sequential reads and writes are done at
  submission, as they never wait; its open, positional ops and sync run on the shared blocker pool, as
  [`files.md`](files.md) describes.
- **Nothing calls back into Scala, and C does only what Scala cannot.** Network.framework delivers everything as blocks,
  which Scala Native cannot build, on Dispatch's threads, which cannot run managed code. The C side
  (`src/main/resources/scala-native/nwframework_io.c`) is therefore trampolines: each block copies its arguments,
  received bytes included, into an event, queues it on the handle, and wakes the reactor watching the handle. The
  reactor's thread drains the handles with events each turn (`nwf_reactor_drain`, `nwf_handle_next`) and parks on a
  condition variable between turns, bounded by its earliest timer (`nwf_reactor_wait`). Everything else is Scala, on
  `NWFrameworkHandle`: the received bytes in a stash, datagrams, accepted connections, the connection's state and
  errors, the send window, and the ops waiting with the reactor to tell for each. So every completion runs on the
  loop thread, as `Reactor` promises, and an op is a waiter registered on the handle, attempted again when the handle
  tells its reactor. The one piece of state C keeps is the count of sends outstanding, so that a closed connection is
  cancelled once they have gone, whichever reactor, if any, is still watching it.
- **The reactor says what to wake it for.** `nwf_handle_watch` names the reactor watching a handle and a mask of the
  event kinds an op there waits for; the other events only queue. Without it every send's completion woke the reactor,
  a spurious wake-up per message per side.
- **One wake-up, consumed only by the wait.** The reactor's wake-up flag is set by `nwf_reactor_wake` (`wakeup`, a
  blocker thread finishing, another reactor queueing a notification) and consumed only by `nwf_reactor_wait`. When the
  drain also consumed it, a wake-up landing between the Scala side emptying its queues and the drain was lost, and the
  reactor parked, with no timer to bound the park, on a blocking op's result already queued: once in thirty runs of
  the demo suite.
- **The rest is shared with `KqueueReactor`**: timers in a user-space heap (`gears.async.io.internal.TimerHeap`),
  blocking ops and `resolve` on the shared blocker pool (`gears.async.io.internal.Blockers`), completions of ops
  finished at submission queued for the next turn, and `trySubmit` finishing them on the caller.
- **Every executable that links it needs `-framework Network`** (`networkFramework` in build.sbt). It runs the base
  contract suites (`NWFrameworkContractSuite`, `NWFrameworkFuturesSuite`) and `HigherLevelChecks`. It does not get the
  kqueue backend's `setjmp` override (see [`kqueue.md`](kqueue.md)), which only programs linking `gears-io-kqueue` have.

The environment variables `GEARS_IO_NWFRAMEWORK_SEND_WINDOW` (bytes, default 256 KB, 0 to make every write wait for its
send), `GEARS_IO_NWFRAMEWORK_PREFETCH` (0 turns the receive prefetch off), `GEARS_IO_NWFRAMEWORK_SPIN` (checks before
parking, default 0) and `GEARS_IO_NWFRAMEWORK_TRACE` (1 prints at exit how long events waited to be taken) exist for
measuring; the defaults are the measured best.

## Semantics kept, and how

- **A read completes with at least one byte, or at the end of the stream.** A receive asks for 1 to `remaining` bytes,
  which go to the handle's stash, and a read takes from the stash. After a read that took bytes the next receive is
  issued at once, so the next read finds its bytes waiting and finishes at submission, which is how `trySubmit`
  finishes a read without suspending the caller.
- **A pooled read skips the stash.** `ops.read(handle, pool, into)` receives straight into a buffer of the pool's on
  Dispatch's thread (`nwf_receive_into`), and the reactor hands that buffer over as a `Received`: one copy, the
  framework's own, where a read into the caller's buffer costs the stash copy and another into the caller's. A plain
  read after a pooled one takes what the pooled receive brought first.
- **Cancelling a read runs `onCancel` at once, and the op can be performed again.** A receive once issued cannot be
  withdrawn, so a cancelled read leaves it outstanding, and its bytes land in the stash for the next read. The op's
  buffer is written only on the loop thread, when the read is delivered, so cancel does not race a callback: it takes
  the op out of the reactor's waiting map and withdraws the waiter if it has not been told; a notification already
  queued is dropped when drained, by its token.
- **A write completes once at least one byte has been written.** The bytes remaining are copied into a
  `dispatch_data_t` and sent at submission, and the buffer's position advanced. While the handle has less than the send
  window outstanding the write completes at once; beyond it, when a send's completion reports the stack has processed
  the bytes. Completions pace sends rather than serialise them, as Network.framework advises and as a kernel socket's
  send buffer does for kqueue. Cancelling a write leaves the buffer alone, the bytes being with the stack.
- **Closing sends what was written.** `nw_connection_cancel` drops sends still queued, where a kernel socket's close
  sends its buffer first; so the C side cancels a closed connection once its last send has completed.
- **A connect completes once connected, or fails.** Network.framework keeps a refused or unreachable connection in its
  `waiting` state for a path to appear, rather than failing it; the backend takes `waiting` with an error as the
  failure, as POSIX `connect` reports it.
- **Opening and closing are ops that complete when the framework is done.** The `listen` op completes once the
  listener is ready, or fails with the address in use. The `close` op completes, for a connection, at its cancelled
  state; for a listener or a bound datagram socket, at its cancelled state too, so that a port whose listener's close
  op has completed can be listened on again at once (completing it after `nwf_cancel_async` alone failed the contract
  now and then with "Address already in use"); for a descriptor or a file, at once. The synchronous
  `NWFrameworkHandles.listen` and `close`, which wait on a semaphore, are kept only for code with no reactor.
- **Closing a handle fails the ops pending on it, on whichever reactor.** The handle takes its waiters and tells each
  one's reactor, where the op fails with "The handle was closed"; then it cancels the connection, whose outstanding
  receives and sends complete with errors and find no waiter. The C side is freed by the framework's last callback,
  the cancelled state, or for a descriptor by the last Dispatch source's cancellation handler, which also closes it.
- **A connection both sides have ended is read to its end.** `shutdownOutput` is a final send
  (`NW_CONNECTION_FINAL_MESSAGE_CONTEXT`, `nwf_send_final`), TCP's FIN. Once both sides have sent theirs and the peer
  closes, the framework moves the connection to `failed`, with POSIX `ENETDOWN`, while it still holds what the peer
  sent and its end for receives to come. A stream that was ready therefore keeps reading after `failed`, which fails
  only its writes, until a receive brings the end or fails itself. `HigherLevelChecks`' graceful close, a 256 KiB
  reply, read 128 KiB and then failed with "Network is down" before; it passed 60 runs in a row after.
- **Handles are objects, numbered in a table.** Network.framework's objects are not descriptors, so a
  `NWFrameworkBackend.Handle` holds its `NWFrameworkHandle`, whose number in a `gears.async.io.internal.HandleTable`
  the C side carries as its first field, so a drained handle is found by number (`NWFrameworkHandles.at`) and checked
  against its C pointer. A closed handle fails every op later built or submitted on it (see [`handles.md`](handles.md)).
- **Ops move between reactors.** A waiter is registered with the reactor submitting the op, and the handle keeps no
  reactor of its own, so a repeatable op may be submitted to another reactor once its previous submission is over.

## What public Network.framework cannot keep

- **No Unix domain sockets.** The C headers define endpoints of type address, host, Bonjour and URL only; Swift's
  `NWEndpoint.unix` is private API. So the backend has neither `UnixStreams` nor `UnixDatagrams`, the gears demos that
  need Unix sockets do not compile against it, and the bench compares it with kqueue over TCP.
- **No readiness.** Nothing can be tried on a connection without waiting, and no event says a connection is readable:
  `ready`, `readNow` and `writeNow` have no counterpart, so it is not a `Readiness` backend. The contract tests that used
  readiness moved to `ReadinessContract` and `ReadinessFuturesChecks`, which kqueue and NIO run, and the base
  `ReactorContract` and `FuturesContract` use ops alone, so that every backend runs them.
- **The sender of a datagram on a connected socket** is its peer as given at creation: Network.framework reports no
  per-packet sender on a connection. A bound socket's flows report theirs.
- **A bound datagram socket is a listener with flows.** Network.framework opens a flow per peer that sends to it, each
  with a receive outstanding, so packets are taken in per peer as they arrive rather than from one socket buffer;
  sending to a peer that has not sent first opens an outbound flow from the bound address. Flows the framework ends on
  its own (a failed state) are not reclaimed.

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

The gears bench on one release build (Scala Native nightly, release-fast), kqueue and NWFramework in turn, over loopback
TCP since NWFramework has no Unix sockets, median of 3 runs, µs an operation (`bench/results/native-darwin-tcp-window.json`,
`-window-4-workers.json`, `-window-connections.json`; `connections` at a tenth of its scale and 2 runs, since 20,000
connections exhaust the ephemeral ports). This is the state-in-C build with the send window; the slim build is about
10 µs a round trip dearer (above). How to run the bench is in [`bench.md`](bench.md): `compare.sc` takes a build as
`nwframework=<path>@nwframework` with `-- --tcp`.

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

- **macOS only, not published**, and no Unix sockets or readiness (above).
- **Flows of a bound datagram socket that the framework fails on its own are not reclaimed.**
- **The back-pressure stream over a pipe hangs now and then.** `HigherLevelChecks`' "a stream of chunks consumed as a
  gears channel, with back-pressure" waits for ever on four workers, all reactors idle, about once in 200 runs (30 to a
  process). It hangs the same with and without the final-send change, so that is not its cause. Pipes here are
  descriptors waited on through a Dispatch source with pooled reads; a wake-up lost between the writer's close and the
  reader's next pooled read is the first thing to look at. Open.
- **The slim build's 10 µs over the state-in-C one** is open, at the handle's monitor (above).

## Skywalk as a backend without Dispatch: a design note, not built

Network.framework is a user-space stack over Skywalk, the kernel's user-space data path, with Dispatch in between. This
note records what it would take to go to Skywalk directly, with no Dispatch and no Network.framework, as a backend
driven by the kqueue loop. It was written from the xnu sources (`apple-oss-distributions/xnu`, `main`) and the machine
itself (Darwin 27.0.0). Nothing of it has been built.

### What Skywalk gives a process

- **A channel is a descriptor over a mapped arena.** `os_channel_create_extended` opens a channel to a port of a nexus,
  `os_channel_get_fd` gives its descriptor, and the TX and RX rings, their slots and the packet pool live in memory the
  kernel maps into the process (`bsd/skywalk/channel/os_channel.h`). `os_channel_sync` is the system call that hands
  the kernel what TX has and takes what RX has. The descriptor takes `EVFILT_READ` and `EVFILT_WRITE`
  (`filt_chread`, `filt_chwrite` in `channel.c`), so `KqueueLoop` can wait on it as on a socket.
- **The library ships; its headers do not.** `libsystem_kernel.dylib` exports 139 `os_channel_*`, `os_nexus_*` and
  `os_packet_*` wrappers, listed in the SDK's `libsystem_kernel.tbd`, with source in xnu's
  `libsyscall/wrappers/skywalk/`. The headers are `PRIVATE` and stripped from the SDK, so Scala Native would bind them
  with `@extern` against vendored copies at the tag matching the kernel.
- **A flowswitch port sees layer 3.** The flowswitch demuxes packets to flows by 5-tuple, reserves ports, reassembles
  IP, offloads checksums, segments and aggregates TCP on receive; a user port receives packets with the link header
  stripped. The TCP state machine is not in the kernel: the stack is Network.framework's, in user space. A backend
  without Network.framework must bring its own TCP and UDP over IP.
- **The machine has it attached.** `ifconfig -v en0` shows a netif and a flowswitch nexus, and the macOS default
  (`SKYWALK_NETWORKING_MAC_OS`) enables them for Ethernet and cellular. `lo0` carries neither, so loopback traffic
  cannot take this path; a bench needs two interfaces or a feth pair.

### Getting a flow without entitlements

Registering nexuses (`com.apple.private.skywalk.register-user-pipe`, `-kernel-pipe`, `-net-if`, `-flow-switch`),
low-latency channels and observing take private entitlements, which a self-signed binary does not get. So there is no
direct flow add (`NXCFG_CMD_FLOW_ADD` on the system flowswitch needs the controller that owns it, the kernel's: EPERM,
entitled or not), and no user pipe nexus, the natural stand-in for Unix sockets and pipes, which stay on kqueue.

The route an ordinary process takes is NECP, as Network.framework's stack does (`bsd/net/necp_client.c`): `necp_open`,
then `necp_client_action` with `NECP_CLIENT_ACTION_ADD` and the client's parameters, and `NECP_CLIENT_ACTION_ADD_FLOW`
naming the flowswitch netagent; the kernel adds the flow under its own controller and returns the nexus instance, port
and key (`NECP_CLIENT_RESULT_NEXUS_INSTANCE`, `_PORT`, `_KEY`); the process opens a channel to that instance and port
with the key on the channel attributes (`os_channel_attr_set_key`), which `__channel_open` matches against the flow's
binding. The only entitlement checked on that path is for `NECP_CLIENT_PARAMETER_FLAG_INTERPOSE`, which a backend does
not need. The parameter TLVs and the order of the calls are written down nowhere: they are private, versioned per
release, and the reference is `necp_client.c` for the running kernel, with Network.framework's binary to trace where
the sources leave a gap. Whether each flow gets its own port and channel, or flows share a port
(`NECP_CLIENT_RESULT_NEXUS_PORT_FLOW_INDEX`), is to be settled the same way.

### Mapping onto the reactor

A readiness backend, designed as `KqueueBackend` is rather than as this one, with `Readiness` on channel descriptors
falling out of it.

- **Loop.** Channel descriptors join the reactor's kqueue. A turn drains the RX ring of each channel that signalled,
  runs the protocol state machines, fills TX rings, and calls `os_channel_sync` once per channel touched at the turn's
  end: the batching `Submitter.submit` allows. `TimerHeap` serves the protocol timers, and `trySubmit` finishes a read
  from bytes already in the receive buffer, as kqueue's does.
- **Handles** name flows, not descriptors, as here.
- **Ops.** `receive` and `send` map onto UDP flows, a packet per slot, with IP and UDP headers written by the backend;
  `read` and `write` sit on a TCP send and receive buffer in the backend; `accept` needs a listener flow and a child
  flow per completed handshake; `connect` is the SYN exchange, `listen` the flow assignment completing, and `close`
  sends FIN and deletes the flow once the state machine is done. `resolve` and `blocking` stay on the blocker pool.
  The pooled read can be zero copy: the slot's packet buffer goes out as the `Received`, and `release` frees the
  packet (`os_channel_packet_free`); a read into the caller's buffer costs one copy, as kqueue's `recv` does.
- **Memory and threads.** The arena is outside the collector, read and written through pointers. A channel is driven
  by one thread at a time, which one reactor per submission gives.

### Order of work

1. **Feasibility, standalone**: a UDP echo over a NECP-assigned flow on `en0`, driven by `kevent` on the channel
   descriptor, in a Scala Native binary with the vendored headers. It proves the NECP handshake and the ring layout for
   the running kernel, and is the step most likely to fail.
2. **UDP ops**: `receive`, `send`, `timer`, `resolve` and `blocking` over Skywalk, the rest on kqueue sockets, so that
   the contract suites run against it.
3. **TCP**: a small stack in Scala, or lwIP vendored in C, single-threaded on its raw API with no OS layer, which fits
   the per-turn model. A TCP with congestion control, retransmission and SACK is the largest item by far, and decides
   whether the backend is worth having.
4. **Bench** against kqueue and this backend over two interfaces. The round trip to beat removes the kernel socket
   layer and Network.framework's Dispatch hop; whether a user-space TCP in Scala Native beats the kernel's socket path
   is open until measured.

### Risks

- **Private ABI**: the NECP and Skywalk structures change between releases with no compatibility promise; a kernel
  update can break the backend until the vendored headers are refreshed.
- **Not loopback, not Unix sockets, not pipes**: the gears demos that need them cannot run on it alone.
- **A TCP of one's own** is a protocol implementation to maintain, with its own conformance and security surface.
