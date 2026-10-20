# NIO backend and gears on the JVM

`asyncio-jvm/nio` (sbt `jvmAsyncioNio`, package `asyncio.nio`) implements `asyncio.Backend` on the JVM, as
`asyncio-native/loop` does with kqueue on Scala Native. `asyncio-jvm/gears` (sbt `jvmAsyncioGears`) is the JVM's
`ReactorFutures`, so that gears code calling `perform` is the same on both platforms; only the given backend differs.

## Capabilities, in the types

Every backend has `ops`, `handles` and `perform`, over IP sockets, pipes and regular files (`Backend`, `Handles`,
`Ops`). What only some have is a capability mixed into the backend, with its own handles and ops, so asking a backend
for one it lacks fails to compile. Running ops on reactors is one of them: a backend may instead perform each op itself.

| Capability | Traits | kqueue | NIO |
| --- | --- | --- | --- |
| Reactors (`open`, `submit`, attachments) | `ReactorBackend` | yes | yes |
| Readiness and waitless use (`whenReady`, `readNow`, `writeNow`) | `ReadinessBackend`, `ReadinessOps`, `ReadinessHandles` | yes | yes |
| Blocking ops (`blocking`) | `BlockingBackend`, `BlockingOps` | yes | yes |
| Unix stream sockets | `UnixStreamBackend`, `UnixStreamHandles` | yes | yes (`UnixDomainSocketAddress`, JDK 16+) |
| Unix datagram sockets | `UnixDatagramBackend`, `UnixDatagramHandles`, `UnixDatagramOps` | yes | no: the JDK has no Unix datagram channels |
| FIFOs | `FifoBackend`, `FifoHandles` (`openFifo`) | yes | no: the JDK can only block on one |

Addresses are typed by family: `IpAddress` (IPv4, IPv6) and `UnixAddress`; `send` takes an address of its socket's
family, or none for a connected socket's peer. `openFile` opens only regular files, on both backends; a FIFO is
`openFifo`'s. A program states what it needs by the backend it asks for, `(using UnixStreamBackend)` for instance, and
`ReactorFutures.run` hands it that backend with its own type, so `r.handles.connect(UnixAddress(path))` compiles where
the backend has Unix streams and not elsewhere.

## The NIO backend

- **Reactor:** `NioReactor` drives one `Selector`, on the thread that runs it. A command is tried as soon as it is
  submitted (unless it is a `whenReady`), and waits on its channel's key otherwise. Interest ops follow the commands
  pending on each key, brought up to date once per turn, just before the select, so a read finishing and the next read
  submitted from its completion change nothing. Keys the select finds ready are collected, then dispatched after it
  returns, so completions never run inside the selector. Timers are a heap on the loop thread, as in `KqueueReactor`,
  and bound how long a select blocks. `wakeup` coalesces into one `Selector.wakeup` per select.
- **Handles:** `NioHandles` is a process-wide table of entries by handle, so that, like descriptors, a handle means the
  same on every reactor and thread, and an op may move between reactors. Freed numbers are reused; a reactor keeps
  nothing by number, only on the channel's key in its selector, so a reused number never meets stale state.
- **Closing:** a channel registered with a selector releases its descriptor only once that selector has seen its key
  cancelled, so closing a handle wakes every reactor that registered it; without that, a reactor blocked with nothing
  else to do kept a pipe's write end open, and its reader never saw the end (found by `ReactorFuturesSuite` with two
  reactors; checked by the contract's "closing a handle that a reactor blocked on another thread has used").
- **Regular files** are `FileChannel`s, always ready, as POSIX reports one; a command on one that is not done is
  performed again next turn.
- **Blocking ops and `resolve`** run on a blocker pool of daemon platform threads, which a cancel interrupts, with the
  kqueue backend's rules for `onCancel` and slots. `resolve` uses `InetAddress.getAllByName`. They were virtual threads
  at first, but one started from a virtual thread inherits its scheduler: under `Carriers.Reactors` a blocking op then
  took one of the pool's workers, and the cancel demo hung, its reactor closing on the only worker while waiting for an
  interrupted sleep that needed that worker to wake (found running `gears cancel` on the JVM).
- **Differences:** an IPv6 address comes back in the JDK's long form (`0:0:0:0:0:0:0:1`, not `::1`). A connected
  datagram socket cannot send an empty packet: the JDK's `DatagramChannel` sends nothing for an empty buffer once
  connected, through `write` or `send` to its peer, and reports it sent, so NIO's `send` fails instead (an unconnected
  socket's `send(handle, buf, to)` sends one). No spinning before
  blocking, no readiness guesses (every read is tried first), and nothing measured yet.

## Gears on the JVM: `ReactorFutures.run(workers, policy, carriers)`

Gears suspends a future on the JVM by blocking its virtual thread, so `perform` is the same call as on Scala Native;
what differs is what carries the futures, `Carriers`:

- **`Carriers.Jdk`, the default:** gears' own virtual threads, on the JDK's scheduler, with `workers` reactor threads
  beside them (`ReactorThreads`), found through an inheritable thread local the runner sets (`OpRouter.ambient`).
  `perform` hands its op to a reactor thread, picked by the caller's thread so a future's ops stay together, which
  submits it and runs its completion, resuming the future; a cancel is handed over the same way. There is no fast path,
  since only a reactor's own thread may try an op: every op costs a hand-off and a wake-up each way. Needs nothing of
  the JDK beyond its public interface.
- **`Carriers.Reactors`, opted into:** virtual threads whose scheduler is a `ReactorPool`, so the pool's workers carry
  them and drive the reactors, as on Scala Native: `perform` finds the worker by the caller's carrier, and may finish an
  op at once; a completion's resume is a continuation queued on that worker, under the pool's `TakePolicy`. The JDK
  keeps a virtual thread's scheduler and its carrier to itself, so this uses `ThreadBuilders.VirtualThreadBuilder
  (Executor)` and `Thread.currentCarrierThread` by reflection, and needs `--add-opens java.base/java.lang=ALL-UNNAMED`;
  without it, `run` says so. Its gears support is `FactoryVThreadSupport`, gears' `VThreadSupport` with the threads
  from the pool's factory.

`perform` finds its reactors through `OpRouter` in both cases (`ReactorAsync`): on a worker, that worker's reactor,
with the fast path; anywhere else, a hand-off to a driver's thread, with a waiter whose state is locked, since the caller
and the completion are on different threads. A program on Scala Native performing from a thread outside its pool now
hands its ops over the same way, where it used to be refused.

Checked by `ReactorFuturesSuite`, over NIO, in each mode, on one reactor and on several: timers, a pipe streamed between
two futures, clients and a server echoing over Unix sockets, cancelling a future waiting on an op, failures.

## Next

1. [x] **A shared demo.** The gears demos are `gears-demo` (sbt `gearsDemoJVM`, `gearsDemoNative`), written against
   `asyncio` alone with buffers from `asyncio-testkit`'s `BufferArena`, and their helpers in `DemoIO`. kqueue-demo
   keeps the plain demos, `move-bench` (kqueue's own) and the Native `Main`, so `kqueuedemo gears <command>` is as it
   was; the JVM's `Main` (`sbt "gearsDemoJVM/run <command>"`) has the commands NIO can run, the FIFO and Unix datagram
   demos not compiling against it. `-Dasyncio.gears.carriers=reactors` runs them on the reactor pool
   (`Carriers.default`). `kqueueDemoTests` runs the gears checks on the JVM too, in both modes (`JvmGears*Suite`,
   `JvmReactorsGears*Suite`), skipping what NIO lacks. Running them found three things: a race in the cancel demo, which
   assumed a future's read reached the reactor within one turn, true only on a pool's worker; NIO's blocking ops on
   virtual threads, which a future carried by the pool made run on the pool itself (see above); and the JDK's empty
   datagrams on a connected socket (see the differences).
2. [x] **Measured: the JVM's three ways against kqueue.** The bench scenarios, `GearsBench` over NIO with each carrier
   mode, and `JdkBench`, the same scenarios with the JDK alone (blocking Unix socket channels on virtual threads, in
   `StructuredTaskScope`s, a preview API in JDK 27, timers on a `ScheduledThreadPoolExecutor`), with kqueue on Scala
   Native as the reference. `gears-demo/bench/build-jvm.sh` packages them for `kqueue-demo/bench/compare.py`; each JVM
   run warms up first (`--warmup`). Median of 5 runs, µs/op, JDK 27, macOS arm64, load average 3 to 5
   (`kqueue-demo/bench/results/jvm-backends*.json`):

   | scenario | nio-jdk | nio-reactors | jdk-vthreads | kqueue |
   | --- | --- | --- | --- | --- |
   | pingpong | 48.8 | 8.7 | 9.0 | 7.0 |
   | pingpong-deadline | 49.2 | 10.0 | 12.3 | 6.9 |
   | concurrent | 5.5 | 3.9 | 11.8 | 1.8 |
   | concurrent-compute | 5.3 | 7.0 | 12.1 | 6.2 |
   | connections | 127 | 79 | 39 | 21 |
   | pipelined | 20.4 | 1.26 | 1.09 | 0.67 |
   | blocking | 9.5 | 7.4 | 5.2 | 5.1 |
   | blocking-io | 67 | 15.9 | 15.0 | 13.0 |
   | timers | 0.18 | 0.16 | 0.30 | 0.16 |
   | fairness | 21.3 | 2.6 | 1.7 | 1.4 |
   | mixed | 65 | 16.3 | 10.2 | 22.7 |

   One worker each side; the JDK's virtual thread scheduler as it comes, a carrier per core.
   - **`Carriers.Jdk` pays two thread hand-offs per op**, to the reactor's thread and back to a carrier: ping-pong at 49
     µs, against 9 for the other two. It does well only where many ops are in flight at once (`concurrent`), batching
     the hand-offs, and it is the one to improve if it stays the default: a reactor thread could try an op as it is
     handed over, as it does, but the caller cannot; the cost is the waking of both threads.
   - **`Carriers.Reactors` is within 25% of kqueue on a connection's latency** (8.7 against 7.0 µs) and level with the
     JDK's own virtual threads, with timers as cheap as kqueue's. It is slowest at `connections` (79 µs, twice the
     JDK's), where opening, registering and closing NIO channels through the handle table costs more than the
     blocking channels' own path: a place to look.
   - **The JDK alone is as fast on one connection**, its blocking reads parking a virtual thread on the JDK's own
     poller, but slower with many at once (`concurrent` 11.8 µs) and on timers.
   - With the scheduler held to two carriers (`-Djdk.virtualThreadScheduler.parallelism=2`, as many threads as the
     pools have), the JDK alone gets faster with many connections (`concurrent` 11.8 to 3.8 µs, `concurrent-compute`
     4.6) and `Carriers.Jdk` too (5.5 to 3.4): more carriers contend for macOS's per-process descriptor lock, as the
     kqueue pool's workers do (see the kqueue TODO). The rest is unchanged.
   - `fairness` and `mixed` favour the JDK's scheduler, whose computation and yields run on other carriers, in
     parallel, where a pool's worker shares one thread between them.
   - **At four workers, `Carriers.Reactors` slows down** (ping-pong 8.7 to 23 µs, `fairness-tick` 5 to 28, `mixed` 16
     to 40) where kqueue does not (7.1 µs): a continuation resumed by one worker's reactor should stay on it under the
     affine policy, but something moves or wakes it elsewhere. Not looked into yet.
3. [x] **`Carriers.Jdk` optimised: ops tried on the caller first, and wake-ups only when the reactor is blocked.**
   Median of 5, µs/op, one worker (`kqueue-demo/bench/results/jvm-jdk-carriers-try-first.json`):

   | scenario | Jdk before | Jdk after | Reactors before | Reactors after | jdk-vthreads |
   | --- | --- | --- | --- | --- | --- |
   | pingpong | 48.8 | 33.1 | 7.8 | 7.7 | 9.1 |
   | concurrent | 5.3 | 7.0 | 3.6 | 2.5 | 11.7 |
   | connections | 127 | 94 | 82 | 82 | 39 |
   | pipelined | 20.1 | 1.42 | 1.28 | 0.52 | 1.30 |
   | blocking | 9.5 | 9.5 | 7.1 | 6.0 | 5.1 |
   | blocking-io | 67 | 50 | 15.7 | 14.9 | 14.8 |
   | fairness | 20.7 | 1.20 | 1.94 | 1.99 | 1.68 |
   | mixed | 64 | 41 | 16.5 | 17.7 | 10.2 |

   - **`Backend.tryNow`** performs an op on the calling thread, touching no reactor, when it finishes at once; NIO
     tries any command that is tried first, since a non-blocking channel may be read and written from any thread.
     `perform` from a caller on no worker tries it before handing the op over, so a write that fits, or a read of data
     already waiting, costs neither hand-off, and a run of 32 such gives up the thread once (`Thread.yield`). A command
     that was just tried and would have blocked is not tried again by the reactor (`triedNow`). Most of the gain.
   - **`NioReactor.wakeup` wakes the selector only when the loop is blocked in it**, or about to be: otherwise it sets
     a flag the loop checks after saying it is about to block, so a task handed over, or a promise resolved, while the
     loop runs costs no write to the selector's pipe, nor the read that drains it.
   - **Spinning before blocking did not pay**, tried four ways: `selectNow` polls on the reactor thread before blocking
     (ping-pong 33 to 23 µs, but each poll a system call contending for the descriptor lock: `blocking` 9.5 to 18-25);
     polls only while a command waits on a channel (no gain: by the time a read is handed over, the previous one has
     finished, so nothing is armed); and waiting on memory for the next op after running completions, busy or
     yielding, which made whatever the reactor had just woken wait for the spin to end (`blocking` up to 86 µs),
     apparently scheduled on the waking thread's core by macOS, where a system call's return would have let it run.
   - **What it still costs**: an op that has to wait is handed to the reactor's thread, which a blocked selector must
     be woken to take, and comes back by waking the caller: ping-pong 33 µs against the JDK's 9, whose poller takes
     registrations from the caller's own thread. Only a backend that registers with `kqueue`/`epoll` from the caller,
     through the foreign function API, would remove the first. **And it slows** `concurrent` (5.3 to 7.0 µs): the
     futures' carriers now make the system calls themselves, a carrier per core contending for macOS's per-process
     descriptor lock where the reactor's one thread made them all before. Linux has no such lock.
   - **A race in gears' virtual thread suspension, found here and fixed** in `VThreadSupport` and its copy,
     `FactoryVThreadSupport`. A synchronous `resume` (a future's completion resuming one that awaits it) cleared the
     label's result, then waited for the body's next suspension to set one; a `resumeAsync` clearing it again before
     the waiting thread looked left that thread waiting, woken by every later suspension of the body. Under
     `Carriers.Reactors` that was a task on the worker for every op of the bench's main future after `pipelined`, so the
     worker never blocked (`blocking` 7 to 14 µs). Results are now counted, and a `resume` waits for one newer than the
     count it saw. `Carriers.Reactors` gained most: `pipelined` 1.28 to 0.52 µs, `concurrent` 3.6 to 2.5.
4. [x] **The floor of a `Selector` design, measured.** `jdk-bench --selector` (`gearsexample.SelectorIo`, the
   `jdk-selector` build of `build-jvm.sh`) runs the JDK-only scenarios with non-blocking channels and a selector thread
   a side: an operation tried on the virtual thread first, and one that would block handing a registration to the
   selector's thread and parking (`LockSupport.park`) until that thread unparks it, the selector woken only when
   blocked. No futures, completions or reactor: the least any design on `Selector` with virtual threads pays. Median
   of 5, µs/op (`kqueue-demo/bench/results/jvm-selector-floor.json`):

   | scenario | nio-jdk | Selector floor | JDK blocking channels | nio-reactors |
   | --- | --- | --- | --- | --- |
   | pingpong | 34.3 | 17.1 | 8.8 | 7.6 |
   | pingpong-deadline | 37.9 | 22.5 | 11.7 | 7.3 |
   | blocking-io | 49.5 | 23.4 | 14.8 | 15.0 |
   | connections | 94 | 56 | 39 | 83 |
   | blocking | 9.8 | 5.1 | 5.1 | 5.9 |
   | concurrent | 7.7 | 10.3 | 11.6 | 2.5 |
   | pipelined | 1.36 | 1.46 | 1.04 | 0.53 |
   | mixed | 41 | 27 | 10 | 18 |

   - **`Selector` itself costs about twice the JDK's own poller** on a connection's latency (17 against 9 µs a round
     trip): the hand-off to the selector's thread, and waking it, which the JDK's poller avoids by taking registrations
     from the caller's thread. No design on `Selector` gets below that.
   - **`Carriers.Jdk` costs about twice the floor again** (34 µs): gears' suspension (a `VThreadSupport` label and
     suspension, each a lock and condition, and the listener machinery around `await`), the waiter, the hand-off's
     closure, and the reactor's bookkeeping, on both sides of each round trip. `blocking` shows the same: 9.8 against
     5.1, a blocking op going through the reactor and the blocker pool where the floor forks a subtask.
   - **With many connections, ours beats the floor** (`concurrent` 7.7 against 10.3): the reactor's batching of interest
     changes and completions per turn, against a registration per wait.
5. [x] **`Carriers.Jdk` at the floor: callers park themselves, and run blocking ops themselves.** A caller on no
   pool worker, which may block anyway, now waits for a pending op by parking its own thread (`LockSupport.park`):
   the completion leaves the outcome in the waiter and unparks it, with no gears `await`, label or listener; the waiter
   is still linked to the caller's group, so cancelling the caller cancels the op on its reactor. And a blocking op
   runs on the caller (`Backend.blockingHere`, which NIO implements with its blocker pool's rules: skipped if cancelled
   first, interrupted if during, its result then not written), rather than going to a reactor and its blocker pool.
   Median of 5, µs/op, load average 5 to 8 (`kqueue-demo/bench/results/jvm-jdk-carriers-park.json`):

   | scenario | Jdk before | Jdk after | Selector floor | JDK blocking channels |
   | --- | --- | --- | --- | --- |
   | pingpong | 33.6 | 18.0 | 19.3 | 9.2 |
   | pingpong-deadline | 37.1 | 19.2 | 22.4 | 12.3 |
   | blocking-io | 50.0 | 18.5 | 25.8 | 15.2 |
   | blocking | 9.7 | 0.14 | 5.8 | 5.4 |
   | connections | 94 | 86 | 57 | 39 |
   | concurrent | 7.4 | 7.5 | 10.0 | 11.4 |
   | mixed | 41 | 34 | 27 | 11.5 |

   `Carriers.Jdk` is now at the `Selector` floor on a connection's latency, below it wherever blocking ops are
   involved (a call, where the floor forks a subtask), and still ahead with many connections. What is left to the
   JDK's own blocking channels is `Selector`'s (see 4). A blocking op running on the caller no longer belongs to a
   reactor, so closing the reactor no longer interrupts it; the caller's own cancellation, which a program's end
   brings, does (the cancel demo's checks still pass).


6. [x] **Measured: cats-effect on the JVM.** `kqueue-demo/ce-bench` is now a cross project, the same scenarios on the
   JVM (`ceBenchJVM`, packaged by `build-jvm.sh` as `cats-effect`, run `@raw`) with `--warmup`. On the JVM, fs2 3.14
   does TCP through cats-effect's selector poller, but Unix sockets as blocking calls on its blocking pool, so it was
   measured both ways. fs2 listens with the JDK's default backlog of 50, which `concurrent`'s 64 connections overflow,
   so its clients retry a refused connect. Median of 5, µs/op, JDK 27, one worker each side; over TCP without
   `connections`, which ran out of ephemeral ports (`kqueue-demo/bench/results/jvm-vs-cats-effect.json`,
   `jvm-vs-cats-effect-tcp.json`):

   | scenario | nio-jdk | nio-reactors | jdk-vthreads | cats-effect | nio-jdk TCP | nio-reactors TCP | cats-effect TCP |
   | --- | --- | --- | --- | --- | --- | --- | --- |
   | pingpong | 17.7 | 9.7 | 10.4 | 52.2 | 33.5 | 19.4 | 29.6 |
   | pingpong-deadline | 20.2 | 9.4 | 13.7 | 64.7 | 34.9 | 18.5 | 34.7 |
   | concurrent | 5.2 | 2.0 | 10.6 | 19.7 | 7.1 | 4.6 | 18.8 |
   | connections | 55 | 48 | 24 | 411 | | | |
   | pipelined | 0.84 | 0.46 | 1.05 | 20.9 | 1.13 | 0.81 | 1.47 |
   | blocking-io | 18.2 | 16.7 | 18.3 | 53.6 | 34.1 | 31.6 | 35.7 |
   | timers | 0.17 | 0.14 | 0.26 | 0.45 | 0.18 | 0.15 | 0.42 |
   | fairness (tick) | 0.86 (19.8) | 1.60 (3.2) | 1.78 (1.4) | 22.5 (0.21) | 1.20 (23.1) | 1.87 (3.7) | 1.53 (54.1) |
   | mixed (compute) | 21.2 (26.1) | 16.1 (32.1) | 11.1 (21.8) | 1921 (15.8) | 39.5 (26.2) | 25.0 (43.7) | 969 (15.9) |

   - Over Unix sockets, cats-effect's blocking-pool path is 3 to 25 times slower than NIO; that is fs2's JVM Unix
     sockets, not cats-effect's runtime.
   - Over TCP, on its selector, cats-effect is level with `Carriers.Jdk` on one connection's latency, 1.5 times
     `Carriers.Reactors`, and 2.6 to 4 times slower with many connections. Its `blocking` (0.05 µs) runs on the
     calling thread and is not comparable, as on Native.
   - As on Native, its `mixed` starves the ping-pong behind a compute fiber that only `cede`s (about 1 to 2 ms a round
     trip), its compute side the fastest for it.
