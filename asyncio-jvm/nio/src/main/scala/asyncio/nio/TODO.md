# NIO backend and gears on the JVM

`asyncio-jvm/nio` (sbt `jvmAsyncioNio`, package `asyncio.nio`) implements `asyncio.Backend` on the JVM, as
`asyncio-native/loop` does with kqueue on Scala Native. `asyncio-jvm/gears` (sbt `jvmAsyncioGears`) is the JVM's
`ReactorFutures`, so that gears code calling `perform` is the same on both platforms; only the given backend differs.

## Capabilities, in the types

Every backend has `ops`, `handles` and `perform`, over IP sockets, pipes and regular files (`Backend`, `Handles`,
`Ops`). What only some have is a capability mixed into the backend, with its own handles and ops, so asking a backend
for one it lacks fails to compile. Running ops on reactors is one of them: a backend may instead perform each op
itself, as the JVM's `VThreadBackend` did until it was retired (item 14).

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
  `perform` first tries the op on the caller's own thread (`ReactorBackend.tryNow`); one that would block, while few
  ops are waiting, is tried again after a yield, up to 16 times, then waited for on the handle's own selector in the
  caller's thread (`ReactorBackend.waitHere`, cancelled with `Selector.wakeup`); otherwise, or with many ops waiting,
  it is handed to a reactor thread, picked by the caller's thread so a future's ops stay together, which submits it
  and runs its completion, resuming the future; a cancel is handed over the same way (items 3, 5, 11 and 12). A
  blocking op runs on the caller, and a timer parks it until its time. Needs nothing of the JDK beyond its public
  interface.
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
7. [x] **Readiness and blocking ops as capabilities, and a backend with no reactor.** The base `Backend` is now `ops`,
   `handles` and `perform(op)(using Async)`. Reactors, readiness, and blocking ops are capabilities like the others
   (see the table above): `ReactorBackend` (`open`, attachments, `tryNow`, `blockingHere`; its `perform` is
   `ReactorAsync.performOnReactor`), `ReadinessBackend` (`ReadinessOps.whenReady`, `ReadinessHandles.readNow`/
   `writeNow`) and `BlockingBackend` (`BlockingOps.blocking`). kqueue and NIO have all three. `ReactorAsync.perform`
   is `b.perform(op)`, so demos call it as before; `submit` takes a `ReactorBackend`. `GearsCancel` asks for all
   three, `GearsFileRead` for readiness, and `GearsBench` skips the scenarios a backend cannot run: `pingpong-deadline`
   and `timers` submit timers to a reactor, `blocking` and `blocking-io` perform blocking ops.

   `VThreadBackend` (`asyncio.nio`, beside NIO, whose address handling and handle table, `HandleTable`, it shares)
   has no reactor: `perform` refuses a platform thread, then makes the JDK's blocking call on the caller's virtual
   thread, which the JDK parks on its own poller. The op is the caller group's `Cancellable` while it runs, so
   cancelling the caller interrupts the thread; the interrupt is cleared before `perform` returns. The JDK closes a
   channel whose blocking call is interrupted, so a cancelled read or write leaves its channel closed, unlike on a
   reactor; a cancelled timer just stops sleeping. `ReactorFutures.run` on the JVM starts a program over a backend with
   no reactors as a future of gears' own, since `Async.blocking` runs its body on the calling thread, a platform
   thread in a test, which `perform` found. Checked by `VThreadBackendSuite`. Benched as `vthread-backend`
   (`-Dasyncio.backend=vthreads`). Median of 5, µs/op, Unix sockets (`kqueue-demo/bench/results/jvm-vthread-backend.json`):

   | scenario | nio-jdk | nio-reactors | vthread-backend | jdk-vthreads |
   | --- | --- | --- | --- | --- |
   | pingpong | 17.8 | 9.8 | 16.6 | 10.6 |
   | concurrent | 5.4 | 2.3 | 10.7 | 10.6 |
   | concurrent-offload | 6.4 | 6.7 | 11.8 | 11.9 |
   | connections | 54 | 48 | 50 | 25 |
   | pipelined | 0.84 | 0.45 | 1.08 | 1.13 |
   | fairness (tick) | 0.93 (21.1) | 1.51 (3.0) | 1.55 (1.7) | 1.81 (1.4) |
   | mixed (compute) | 21.4 (26.2) | 16.9 (33.6) | 18.9 (25.2) | 11.2 (21.8) |

   (Superseded by item 10: the backend now waits on selectors, and a cancel leaves the channel open.) It matches the
   JDK alone wherever many ops are in flight (`concurrent`, `pipelined`, `fairness`), so the JDK's
   poller does the work there either way. On one connection it is 6 µs a round trip slower (16.6 against 10.6), and
   twice as slow at `connections`, which is gears' cost over a bare virtual thread, not yet looked into: per op, the
   caller-group link and unlink and the op's state; per connection, a gears future on the server and the handle
   table.
8. [x] **Measured: waiting on a channel's own selector, on the waiting virtual thread (`SelectorHereIo`).** The JDK can
   only cancel a blocking channel call by interrupting it, which closes the channel, so a backend that must keep the
   handle open has to wait on readiness itself, and with public API only that means a `Selector`. Here each channel
   has a selector for each direction, made on its first wait, and the virtual thread that would block selects on it
   itself: no selector thread to hand a wait to or wake. On a virtual thread, `select` makes one non-blocking `kevent`,
   parks on the JDK's poller only if nothing is ready (`Poller.pollSelector`, the JDK's own; no pinning), and makes
   one more. A wait is cancelled with `Selector.wakeup()`: checked with a single-file program, the waiter returns, is
   not interrupted, the channel stays open, and a later read gets the data. `jdk-bench --selector-here`, the
   `jdk-selector-here` build. Median of 5, µs/op, Unix sockets, load average 4 to 7
   (`kqueue-demo/bench/results/jvm-selector-here.json`):

   | scenario | nio-jdk | selector-here | Selector floor | JDK blocking channels | nio-reactors |
   | --- | --- | --- | --- | --- | --- |
   | pingpong | 17.9 | 6.8 | 16.4 | 10.5 | 9.9 |
   | pingpong-deadline | 20.4 | 9.1 | 21.1 | 13.6 | 9.5 |
   | concurrent | 5.3 | 16.6 | 8.5 | 10.8 | 2.2 |
   | connections | 55 | 30 | 34 | 25 | 48 |
   | pipelined | 0.85 | 1.06 | 0.95 | 1.06 | 0.45 |
   | blocking-io | 18.1 | 16.0 | 24.3 | 18.5 | 16.5 |
   | mixed | 21.9 | 7.2 | 17.6 | 11.1 | 16.7 |

   - **Fastest on one connection of every JVM way measured**, below the JDK's own blocking channels (6.8 against
     10.5 µs). Likely, the non-blocking `kevent` that `select` makes first often finds the echo already there, so the
     thread does not park: spinning once before blocking, which the blocking channels do not do. Not confirmed.
   - **Slowest with many connections** (`concurrent` 16.6, three times NIO's 5.3). Not the descriptor lock between
     carriers: held to two carriers (`-Djdk.virtualThreadScheduler.parallelism=2`), the JDK's blocking channels drop
     from 10.7 to 3.6 µs, this only from 16.3 to 15.2. Likely the cost of each wait, a kqueue nested in the JDK
     poller's, which its poller thread handles for every waiting connection, where NIO's reactor handles them all in
     one `select`. Not profiled yet.
9. [x] **`SelectorHereIo` profiled and tuned, public API only.** JFR on `concurrent` (`jfr view native-methods`, and
   `jdk.NativeMethodSample` by thread and caller): against the JDK's blocking channels, the virtual threads spend their
   extra time in the two `kevent`s `select` makes on the channel's selector (105 samples, none for blocking channels),
   and in registering the selector's descriptor with the JDK's poller (78, against 52 for the socket's own), almost
   all of it system time: 23 of the process's 25 CPU seconds, as the carriers' system calls contend for macOS's
   per-process descriptor lock (see the kqueue TODO). Two changes, each found by sweeping it:
   - **Yield before waiting** (`-Dgears.selectorHere.yields`, now 32 by default): a read that would block yields and
     tries again, up to 32 times, before it selects. Other virtual threads run meanwhile and the data often arrives, so
     most reads never wait. With nothing else to run a yield returns at once, so this spins briefly, like the kqueue
     pool's spin before blocking. 1 to 3 yields made things worse; from 16 on the scenarios level off.
   - **Two carriers** (`-Djdk.virtualThreadScheduler.parallelism=2`, as many threads as one worker each side), so
     fewer threads contend in the kernel. This is the JDK's process-wide setting, for whoever deploys, not something a
     library can choose; without yields it barely helped (`concurrent` 16.3 to 15.0).

   Median of 5, µs/op, Unix sockets, 32 yields, load average up to 14 by the end
   (`kqueue-demo/bench/results/jvm-selector-here-tuned.json`):

   | scenario | nio-jdk | nio-reactors | selector-here | selector-here, 2 carriers | JDK blocking | JDK blocking, 2 carriers |
   | --- | --- | --- | --- | --- | --- | --- |
   | pingpong | 17.6 | 9.7 | 4.8 | 3.0 | 10.5 | 10.2 |
   | pingpong-deadline | 19.7 | 9.5 | 8.2 | 6.0 | 13.6 | 13.2 |
   | concurrent | 5.4 | 2.2 | 6.6 | 1.45 | 10.5 | 3.4 |
   | concurrent-offload | 6.3 | 6.5 | 8.1 | 3.8 | 11.5 | 5.2 |
   | connections | 55 | 47 | 22 | 21 | 24 | 24 |
   | pipelined | 0.84 | 0.48 | 1.15 | 0.91 | 0.90 | 1.06 |
   | blocking-io | 17.9 | 16.4 | 13.2 | 8.9 | 18.2 | 16.5 |
   | fairness (tick) | 0.92 (20.1) | 1.54 (3.1) | 1.56 (1.3) | 0.69 (0.14) | 1.78 (1.3) | 0.41 (0.16) |
   | mixed (compute) | 20.7 (26.1) | 16.8 (33.6) | 4.6 (21.9) | 1.5 (16.9) | 11.1 (21.7) | 10.2 (20.4) |

   On two carriers it is the fastest of every JVM way measured, and below kqueue on Scala Native, wherever latency
   counts (ping-pong 3.0 against 7.0; `mixed` 1.5), and level with or ahead of the reactor with many connections
   (`concurrent` 1.45 against 2.2). Only `pipelined`, a stream of writes, stays the reactor's (0.48 against 0.91).
   The spinning costs nothing overall: whole-process CPU, start-up and warm-up included, with two carriers, against
   the JDK's blocking channels on two and NIO's reactors: ping-pong 1.06 s against 1.27 and 2.34; `concurrent` 1.14
   against 1.91 and 1.91; `mixed` 0.91 against 2.06 and 2.92. On one carrier, `mixed` crawls: every reader's yield
   gives the computation a whole slice.

   Next: build it into a backend, with cancellation by `wakeup` and the handle left open, and look into the 6 µs a
   round trip gears adds over a bare virtual thread (item 7).
10. [x] **`VThreadBackend` on selectors, cancelled with `Selector.wakeup`, and a starvation scenario.** The backend now
    runs over `NioHandles`' non-blocking channels, so a handle works with either backend, and has readiness
    (`whenReady`, `readNow`, `writeNow`). An op tries its operation on the caller's virtual thread, yields and tries
    again up to 8 times (`-Dasyncio.vthread.yields`), then waits on a selector of the handle's own for that direction,
    in the virtual thread itself. Cancelling sets a flag and calls `Selector.wakeup()`, or unparks a timer: nothing is
    interrupted and the handle stays open. Selectors come from a pool (`SelectorPool`), go back once the handle is
    closed and no op waits on them, and a `selectNow` first releases the closed channel's descriptor. A timer of no time
    yields. Checked by `VThreadBackendSuite` (16): cancelling a read on a pipe and on a socket leaves it usable, closing
    a handle wakes its waiter, `whenReady`, refused connections.

    Tuning, at the JDK's default carrier count (two carriers is the JDK's process-wide setting, and would cap an
    application's compute): busy spinning before yielding (`Thread.onSpinWait`) gave ping-pong 1.6 µs, but holds the
    carrier, and starved work handed to other virtual threads (`concurrent-offload` 26 against 10); it was dropped
    for fairness, as was spinning on accept. A yield puts the thread behind every other one ready to run. Pooling
    selectors made no difference to `connections`; yielding before accepting did (110 to 72 µs), so a listener waits
    at once.

    **`starvation`**, after cats-effect's CPU starvation check (`CpuStarvationCheck`: a fiber sleeps
    `cpuStarvationCheckInterval`, a second by default, and warns when it wakes more than 10% late): `concurrent`'s round
    trips, while a compute task per core works in slices, yielding between them, and a checker sleeps 1 ms over and
    over, recording how late it wakes; `starvation-p50`, `-p99` and `-max` are that lateness in µs, which includes the
    sleep's own overhead (about 0.3 ms even unloaded). In `GearsBench`, `JdkBench` and `CeBench`.

    Median of 5, µs/op, Unix sockets, load average 5 to 7 (`kqueue-demo/bench/results/jvm-vthread-backend-selector.json`):

    | scenario | nio-jdk | nio-reactors | vthread-backend | jdk-vthreads |
    | --- | --- | --- | --- | --- |
    | pingpong | 18.0 | 10.1 | 5.5 | 10.6 |
    | concurrent | 5.4 | 2.1 | 7.0 | 10.8 |
    | concurrent-offload | 6.3 | 6.6 | 10.3 | 12.0 |
    | connections | 56 | 47 | 75 | 25 |
    | pipelined | 0.86 | 0.44 | 1.77 | 1.08 |
    | fairness (tick) | 0.94 (22.4) | 1.68 (3.4) | 2.20 (1.9) | 1.70 (1.5) |
    | mixed (compute) | 21.4 (26.1) | 16.8 (33.5) | 6.8 (26.7) | 11.2 (21.9) |
    | starvation | 8.1 | 6.0 | 8.6 | 11.9 |
    | starvation p50 / p99 / max | 282 / 4383 / 10113 | 356 / 494 / 1928 | 316 / 1180 / 1751 | 266 / 1891 / 8991 |

    Over TCP, against cats-effect on its selector (`jvm-vthread-backend-vs-cats-effect-tcp.json`):

    | scenario | nio-reactors | vthread-backend | cats-effect |
    | --- | --- | --- | --- |
    | pingpong | 19.6 | 26.0 | 25.0 |
    | concurrent | 4.6 | 10.2 | 7.1 |
    | pipelined | 0.77 | 2.75 | 1.49 |
    | fairness (tick) | 1.84 (3.7) | 1.82 (2.0) | 1.52 (54.2) |
    | mixed (compute) | 26.4 (43.6) | 30.3 (27.7) | 1016 (16.7) |
    | starvation | 9.4 | 10.4 | 14.6 |
    | starvation p50 / p99 / max | 230 / 741 / 2772 | 302 / 1344 / 2074 | 755 / 1381 / 2770 |

    - **Over Unix sockets, the lowest latency and worst-case lateness:** ping-pong 5.5 against the reactor's 10.1;
      `mixed` 6.8 against 16.8; the worst wake-up 1.75 ms, the best of the four, where `Carriers.Jdk` and the JDK's
      blocking channels go to 9 to 10 ms. The reactor keeps the best p99 (0.49 ms against 1.18).
    - **Behind the reactor with many connections or a stream** (`concurrent` 7.0 against 2.1, `pipelined` 1.77
      against 0.44) and the slowest at `connections` (75): every virtual thread makes its own system calls, which on
      macOS contend for the per-process descriptor lock, where the reactor makes them from one thread.
    - **Over TCP its single-connection advantage is gone** (ping-pong 26 against the reactor's 19.6), and it trails on
      `concurrent` and `pipelined`; cats-effect sits between the two except at `mixed`, where its fibers starve.
    - Not done: Linux, where the descriptor lock should not apply; why TCP loses the advantage.
11. [x] **NIO's public-API path, `Carriers.Jdk`, brought up with what `VThreadBackend` found, fairly.** Three steps
    before an op is handed to a reactor, each of which yields or parks and never spins (see item 10 for why), and all
    in `ReactorAsync.performElsewhere`, so a backend only provides the pieces:
    - **Yield and try again** (`-Dasyncio.gears.retriesHere`, 16): an op `tryNow` could not finish is tried again
      after a `Thread.yield`, up to 16 times. Only for an op the backend can try on the caller at all
      (`ReactorBackend.triesNow`: NIO's commands tried first, not a timer, a `whenReady` or a blocking op; without
      that, a ticker's zero-length timers yielded 16 times each, `fairness-tick` 20 to 57 µs), and only while fewer
      than `retryBelow` ops are waiting, on a reactor or on their callers: with many, each turn of a reactor serves
      many at once, which tries on every caller's thread only slow (`concurrent` 5.4 to 14.7 ungated). 8 retries
      were worse than none over TCP (41 against 34 µs), where the loopback round trip takes about 20 µs, so the
      first few yields find nothing; 16 catch it (22).
    - **Wait on the handle's own selector, in the caller's thread** (`ReactorBackend.waitHere`, NIO's
      `SelectorWait`, cancelled with `Selector.wakeup`), under the same gate, instead of handing the op over: no thread
      is woken to register the wait, none to resume the caller. Worth little over Unix sockets, where the retries
      catch the echo, and 4.5 µs a round trip over TCP (24.9 to 20.4, the reactor pool at 19.6).
    - **A zero-length timer finishes on the caller**, after a yield (`NioBackend.tryNow`), as on a reactor its
      completion runs a turn later: `fairness-tick` 25 to 1.5 µs, below the reactor pool's 3.2.

    Two things measured and dropped: the reactor on a virtual thread, so that it would take turns inside the
    scheduler instead of competing with its carriers for the cores (`starvation-p99` 4.8 to 6.4 ms: its own wake-up
    then queues behind the futures' too); and spinning, as in item 10.

    **Where the worst-case lateness comes from.** `starvation`'s p99 for `Carriers.Jdk` was 4.4 ms before any of
    this and 6 ms after, against the reactor pool's 0.5 and `VThreadBackend`'s 1.5, and it tracks how many ops cross
    the reactor thread: with the gate opened (`retryBelow=1000`, every op waiting on its own selector) it is 1.6 ms,
    `VThreadBackend`'s. The reactor is one platform thread running every completion while the carriers saturate the
    cores, so when the OS preempts it everything behind it waits, and the maxima of 9 to 10 ms are scheduling quanta;
    a wait on the handle's own selector is woken by the JDK's poller instead. Opening the gate costs `concurrent` (5.5
    to 7.1 µs): more threads in the kernel on macOS's descriptor lock, the balance to strike. The gate, swept with 64
    connections (median of 3, µs/op; lateness in µs):

    | `retryBelow` | concurrent | concurrent-offload | pipelined | starvation p99 | max |
    | --- | --- | --- | --- | --- | --- |
    | 4 | 5.39 | 6.50 | 1.70 | 5161 | 15653 |
    | 16 | 5.37 | 6.38 | 1.83 | 4086 | 10706 |
    | 32 | 5.52 | 6.37 | 1.64 | 4556 | 7067 |
    | 64 | 6.46 | 9.86 | 2.17 | 1916 | 5657 |
    | 128 | 7.52 | 10.44 | 1.88 | 1502 | 2088 |

    32 is the default: nothing lost, the worst case halved. From 64 on, every connection waits on its own thread,
    and the p99 falls to the 1.5 ms of `VThreadBackend` for 20 to 40% on `concurrent` and 50% on
    `concurrent-offload`; on Linux, without the descriptor lock, the price may be lower, to be measured.

    **The result.** Median of 5, µs/op, Unix sockets, load average about 8
    (`kqueue-demo/bench/results/jvm-nio-jdk-tuned.json`; `nio-jdk` at the start of the day from item 10's table):

    | scenario | nio-jdk before | nio-jdk | nio-reactors | vthread-backend | jdk-vthreads |
    | --- | --- | --- | --- | --- | --- |
    | pingpong | 18.0 | 5.9 | 9.9 | 5.4 | 10.6 |
    | pingpong-deadline | 20.5 | 8.4 | 9.4 | | 13.6 |
    | concurrent | 5.4 | 5.8 | 2.1 | 6.8 | 10.7 |
    | concurrent-offload | 6.3 | 6.3 | 6.5 | 10.3 | 11.7 |
    | connections | 56 | 66 | 47 | 74 | 24 |
    | pipelined | 0.86 | 1.30 | 0.43 | 1.53 | 1.07 |
    | blocking-io | 18.3 | 5.9 | 16.5 | | 18.3 |
    | timers | 0.18 | 0.17 | 0.14 | | 0.27 |
    | fairness (tick) | 0.94 (22.4) | 1.70 (1.5) | 1.50 (3.0) | 2.23 (1.9) | 1.73 (1.4) |
    | mixed (compute) | 21.4 (26.1) | 8.5 (24.8) | 16.6 (33.3) | 6.8 (26.8) | 11.1 (21.8) |
    | starvation | 8.1 | 7.7 | 3.9 | 8.5 | 13.2 |
    | starvation p50 / p99 / max | 282 / 4383 / 10113 | 301 / 3155 / 9199 | 413 / 476 / 2676 | 308 / 1353 / 2485 | 268 / 2678 / 9458 |

    - **On one connection, three times faster than it was and ahead of the reactor pool**, which needs `--add-opens`
      (ping-pong 5.9 against 9.9, `mixed` 8.5 against 16.6, `blocking-io` 5.9 against 16.5: the read after a
      blocking op now finds the echo by retrying); `fairness-tick` 22 to 1.5 µs.
    - **With many connections, within 10% of where it was** (`concurrent` 5.8, the gate holding), the reactor pool
      still 2.7 times faster there and three times on `pipelined`, which the retries slow (0.86 to 1.30: a reader
      that tries eagerly reads smaller pieces) and `connections` (56 to 66).
    - **Worst-case lateness** 3.2 ms p99, down from 4.4, the reactor pool's 0.5 out of reach while completions cross
      a platform thread the OS can preempt (see above).

    Over TCP, against cats-effect on its selector (`jvm-nio-jdk-tuned-tcp.json`; `nio-jdk` before from item 6):

    | scenario | nio-jdk before | nio-jdk | nio-reactors | vthread-backend | cats-effect |
    | --- | --- | --- | --- | --- | --- |
    | pingpong | 33.5 | 25.7 | 19.4 | 25.3 | 25.0 |
    | pingpong-deadline | 34.9 | 33.8 | 18.6 | | 24.9 |
    | concurrent | 7.1 | 7.1 | 4.4 | 10.1 | 6.8 |
    | pipelined | 1.13 | 2.19 | 0.75 | 2.36 | 1.40 |
    | blocking-io | 34.1 | 26.5 | 31.1 | | 25.6 |
    | fairness (tick) | 1.20 (23.1) | 1.64 (1.8) | 1.81 (3.6) | 1.81 (2.0) | 1.46 (52.0) |
    | mixed (compute) | 39.5 (26.2) | 29.1 (25.9) | 25.7 (43.5) | 29.7 (27.6) | 997 (16.3) |
    | starvation p50 / p99 / max | | 264 / 4774 / 10778 | 216 / 710 / 2439 | 301 / 1407 / 2266 | 689 / 1370 / 2595 |

    Over TCP it is level with cats-effect and `VThreadBackend` on one connection (25.7 against 25.0 and 25.3), a
    quarter faster than it was, and the reactor pool keeps a third (19.4): the loopback round trip is about 20 µs,
    so the retries find nothing and every read pays the wait on its own selector, which costs more than the reactor
    pool's one `select` for all. `pingpong-deadline` did not move (33.8): the read now finishes on the caller, but
    arming and cancelling its timer are still two hand-offs to the reactor each round trip; a timer that the
    caller's own wait could carry, as a `select` timeout, is the next step there. `pipelined` lost, as over Unix
    sockets.

    Not done: Linux, where the per-process descriptor lock does not apply, so the gate and the retries may both sit
    elsewhere; a deadline carried by the caller's wait; and the hand-off path's lateness, which only moving ops off
    the reactor thread has cut so far.

12. [x] **The hand-off's worst-case lateness closed: a timer parks the caller.** Item 11 left `Carriers.Jdk`'s
    `starvation` p99 at 3.2 ms against the reactor pool's 0.5 and `VThreadBackend`'s 1.4, and traced it to ops
    crossing the reactor thread. The scenario's checker is a timer, and timers always crossed it: `triesNow` is
    false for them, so neither the retries nor the wait on the caller applied, and the one event the scenario times
    waited for a platform thread the OS keeps from the cores while the carriers saturate them. Now `waitHere` for a
    timer is a `TimerWait`, which parks the caller's own virtual thread until the time has passed (`parkNanos`) and
    is cancelled with an unpark, as `VThreadBackend`'s timers are; `WaitHere.await` says whether the wait finished
    the op, since there is nothing to try afterwards. A timer contends for nothing, so it waits on the caller
    whatever the gate says, which now applies only to ops whose tries are system calls on a handle (`triesNow`).
    Checked in both carrier modes: a timer cancelled while parked stops waiting, and may be performed again.

    With that, the gate makes no difference to the p99 (32 to 128: 1.7 to 2.2 ms, median of 3 at the bench's
    length), nor does holding the scheduler to nine carriers to leave a core for the reactor's thread (1.8), and
    `concurrent` no longer pays for a wider gate (5.3 to 5.5 µs across the range): the timer traffic through the
    reactor was part of what the gate sweep in item 11 charged to the gate. At that length a run has about a
    thousand samples, so the p99 is a handful of events and the max one; three times longer, the tail resolves
    (median of 5, lateness in µs; `kqueue-demo/bench/results/jvm-timers-on-caller-starvation.json`):

        | lateness, µs | nio-jdk | vthread-backend | nio-reactors | jdk-vthreads |
    | --- | --- | --- | --- | --- |
    | p50 | 516 | 519 | 309 | 514 |
    | p99 | 1114 | 1653 | 434 | 2260 |
    | max | 6851 | 2902 | 2172 | 11332 |

    The p99 is now below `VThreadBackend`'s and 2.6 times the reactor pool's, from 3.2 ms this morning. The max, one
    event in three thousand, is still two to three times the other gears backends' (6.9 against 2.2 and 2.9 ms),
    though under the JDK's own blocking channels (11.3): the reactor's thread is one more thread for the OS to keep
    from a core, and only moving every op off it would remove that. The timer scenarios are unchanged
    (`jvm-timers-on-caller.json`):

        | scenario | nio-jdk | vthread-backend | nio-reactors | jdk-vthreads |
    | --- | --- | --- | --- | --- |
    | pingpong | 5.75 | 5.17 | 9.72 | 10.51 |
    | pingpong-deadline | 8.51 | | 9.15 | 13.45 |
    | concurrent | 5.26 | 6.24 | 2.23 | 10.64 |
    | concurrent-offload | 6.47 | 10.14 | 6.60 | 11.89 |
    | timers | 0.18 | | 0.14 | 0.29 |
    | fairness (tick) | 1.91 (1.47) | 2.18 (1.67) | 1.45 (2.89) | 1.79 (1.33) |
    | mixed (compute) | 8.10 (25.1) | 6.79 (27.2) | 16.67 (33.3) | 10.99 (21.7) |

    Not done: `submit`'s timers still go to the reactor, so `pingpong-deadline`'s arm and cancel each round trip are
    two hand-offs (33.8 µs over TCP, item 11); and Linux.

13. [ ] **Tried and dropped: the waiters driving one shared selector in turn.** The blend of `VThreadBackend` (no
    thread of the backend's own, so nothing for the OS to keep from a core) and `Carriers.Jdk` (one `select` for all
    under load) that public API allows: ops the gate would hand to a reactor, or all of them, instead waited on a
    selector the waiters share, whichever of them arriving while no one selects becoming the leader, which selects
    for all, wakes the owners of the ready keys, and hands the role to a parked follower once its own op is ready;
    cancelled with `wakeup` or an unpark, the key's interest kept as what its waiters want, under their lock (a
    read-modify-write of `interestOps` from the leader and a waiter arming the other direction lost the latter, and
    deadlocked the echo check: everyone waiting, no one sending). Correct in every mode (the gears suites pass with
    it off, for the overflow, and for all), and slower in every one, median of 3, µs/op, lateness in µs, `starvation`
    three times long:

    | mode | concurrent | concurrent-offload | pipelined | starvation | p99 | max |
    | --- | --- | --- | --- | --- | --- | --- |
    | off (item 12) | 5.37 | 6.45 | 1.22 | 8.4 | 2117 | 14086 |
    | overflow | 13.2 | 16.9 | 1.63 | 31.9 | 4798 | 14194 |
    | all | 14.7 | 17.0 | 1.35 | 45.3 | 3648 | 10808 |

    Each wait costs more hops than the hand-off it replaces: a follower arming while the leader selects must wake it
    (a write, and the poller's hop to a virtual thread), the leader selects again (two `kevent`s), and the follower
    is woken by one hop more; and the leader is woken for every arm, so nothing is batched. Batching the arms and
    waking the leader only when it is blocked would be the reactor's thread again, as a virtual thread, which item 11
    measured as worse than a platform one. So the blend stays as items 11 and 12 left it: a wait on the handle's own
    selector while few ops wait, a timer parked on the caller always, the reactor beyond. The code was removed.

14. [x] **`VThreadBackend` retired.** Everything it pioneered runs in NIO's `Carriers.Jdk` path now: the wait on the
    handle's own selector, the timer parked on the caller, cancellation by `wakeup` or an unpark (`WaitsHere.scala`,
    where its waits moved). What it still had over NIO was a smaller maximum lateness under compute saturation (2.9
    against 6.9 ms, one event in three thousand), for the absence of a reactor's thread; what it lacked was the
    reactor, blocking ops and `submit`, and 20 to 60% on `concurrent` and `concurrent-offload`. A second backend with
    that much in common with the first was not worth that. Its checks that NIO's suite lacked are in
    `ReactorFuturesSuite`: a read cancelled on a socket leaving the connection usable, `whenReady`, a refused
    connection and a one-shot op performed twice in all four carrier modes; closing a handle waking its waiter under
    `Carriers.Jdk` only at first, where the waiter is on the handle's own selector, since an op pending on a reactor
    stayed pending (item 15 ended that). The bench's
    `-Dasyncio.backend` switch and the `vthread-backend` build went with it; its results stay cited in items 7 to 13.

15. [x] **Closing a handle ends the ops pending on it.** Found by item 14's check: `Selectable.close` woke the reactors
    the channel was registered with, so that their selectors dropped the cancelled key and released the descriptor,
    but the command attached to that key was never completed, and a `perform` on it in `Carriers.Reactors` waited
    for ever; kqueue the same one step earlier, its pending command kept by descriptor number. `Reactor.close` had
    only ever said that handles belong to the caller. Now `Handles.close` says what the JDK does for a thread blocked
    on a channel that is closed: the ops pending on the handle, on whichever reactor, fail with an `IOException`,
    their completions getting `onFailure` from that reactor's loop, a `perform` waiting on one throwing it.
    - **NIO:** `Selectable.close` hands each registered reactor its entry (`NioReactor.closed`, a queue like the
      resolved promises', with the wake-up), and the loop fails the entry's pending commands with
      `ClosedChannelException`. They are found through the reactor's own map from entry to waiting commands, not the
      selector's key: the select that the wake-up ends deregisters a closed channel's key before the loop gets there,
      which the first version found out on a reactor blocked on another thread.
    - **kqueue:** `KqueueHandles.close` ends the descriptor's generation in `DescriptorTable` while the number is
      still its, closes, then tells every open reactor (`KqueueReactor.descriptorClosed`, a registry kept by `open`
      and `close`), with the generation that ended: a slot that knows a later generation knows a reused number and is
      left alone. The reactor queues the close, fails the slot's pending commands as commands that failed when
      submitted, so their completions run from the next turn and never inside a submit, and forgets the slot; and
      `currentSlot`, meeting a changed generation with commands still pending, does the same, which was the orphaning
      when a number was reused before the close was seen. Every close in `KqueueHandles` goes through `close`, the
      error paths after an open included, so generations never go stale.
    - **Untangled on the way:** `DescriptorHooks` is gone, and with it the rule that a descriptor had to be closed on
      its reactor's thread. It was a thread-local list of listeners that `PosixSockets.close` called, so that the
      closing thread's reactor, and only that one, forgot the descriptor: the system-call layer knew about reactors,
      and a close elsewhere was seen only as a generation change at the next submit. Now `PosixSockets.close` is a
      close, `DescriptorTable` is written by `KqueueHandles.close` alone, and the reactors are told by it, from any
      thread, one way.
    - **Checks:** two in `ReactorContract`, on the test's own reactor and on one blocked on another thread, both
      backends; the gears check back in all four carrier modes; and the cancel demo, which had encoded the old
      semantics ("cancelling after close"), now shows the failure instead.

16. [ ] **Open: an intermittent hang of the pipe check on four workers.** Once, in a full run of the gears suite on the
    day of item 15, `ReactorCarriersOnFourSuite`'s "two futures stream bytes through a pipe" never finished: the
    writer's and the reader's virtual threads both parked in `performHere` waiting for their ops, every reactor idle in
    `select`, one of the two inside `UnlinkedAwaitListener.run`, that is, being resumed. Both waiting on one pipe at
    once means a completion or a resume was lost between workers. Run alone, the suite passed three times out of
    three afterwards, and every other full run of the day passed. Not reproduced, so not understood; the pool's resume
    path across workers is where the dump points. Thread dump kept with the day's scratch files.

17. [x] **The gears checks shared across backends.** `ReactorFuturesSuite` was NIO's alone, so the checks ported
    from `VThreadBackend` (item 14) ran on NIO's four carrier modes and nowhere else. Its body is now
    `asyncio.gears.FuturesContract`, in `asyncio`'s shared tests beside `ReactorContract`: twelve checks through
    `perform` alone, abstract over the backend and over `running`, which starts a program the platform's way. NIO's
    suite instantiates it in each carrier mode on one and several reactors (49 with its own pool check), kqueue's on
    a pool of one and of four workers (`KqueueFuturesSuite`, 24, beside the pool's two). The refused-connection
    check finds its free port by listening with the backend and letting go, since `java.net.ServerSocket` is the
    JDK's; `jvmAsyncioGears` and `nativeAsyncioGears` now depend on the tests below them (`test->test`).

