# NIO backend and gears on the JVM

`asyncio-jvm/nio` (sbt `jvmAsyncioNio`, package `asyncio.nio`) implements `asyncio.Backend` on the JVM, as
`asyncio-native/loop` does with kqueue on Scala Native. `asyncio-jvm/gears` (sbt `jvmAsyncioGears`) is the JVM's
`ReactorFutures`, so that gears code calling `perform` is the same on both platforms; only the given backend differs.

## Capabilities, in the types

Every backend has IP sockets, pipes and regular files (`Backend`, `Handles`, `Ops`). What only some have is a
capability mixed into the backend, with its own handles and ops, so asking a backend for one it lacks fails to compile:

| Capability | Traits | kqueue | NIO |
| --- | --- | --- | --- |
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
- **Blocking ops and `resolve`** each run on a virtual thread of their own, which a cancel interrupts, with the kqueue
  backend's rules for `onCancel` and slots. `resolve` uses `InetAddress.getAllByName`.
- **Differences:** an IPv6 address comes back in the JDK's long form (`0:0:0:0:0:0:0:1`, not `::1`). No spinning before
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

1. [ ] **A shared demo.** The gears demos now only need `asyncio` and the capabilities they ask for, apart from buffers
   (`NativeBuffer`, over a `Zone`, on Scala Native, where the kernel needs pointer-backed buffers) and `Main`'s choice
   of backend. With buffers allocated through the backend, they can move into a cross-built project with a `Main` per
   platform; the FIFO and Unix datagram demos would then not compile against NIO, as intended.
2. [ ] **Measure** the two JVM modes against each other and against kqueue with the same bench, once (1) lets it run.
