# gears.async.io: the interface, and what sits on either side of it

The interface and its runtime are part of the gears core project; the backends, the contract suites and the demos are
projects of their own:

| where | holds | depends on |
| --- | --- | --- |
| `gears.async.io` (core, `shared/`) | the interface: `Submitter`, `Reactor`, `Backend` and its kinds and capabilities, `Io` and its kinds, `Ops`, `Handle`, `Slot`, addresses, `BufferPool`, `TakePolicy` | the rest of gears |
| `gears.async.io` (core, `jvm/` and `native/`) | the runner, `ReactorFutures`, one per platform | the runtime |
| `gears.async.io.internal` (core, `jvm-native/`, `jvm/`, `native/`) | the runtime (`ReactorPool`, `ReactorWorker`, `RunQueue`, `ReactorScheduler`, `Pools`, `VirtualCarriers`) and what the backends share (`Blockers`, `TimerHeap`, `HandleTable`; on Native, `NativePool` and the Posix sockets, files and resolver) | the interface |
| `ioKqueue`, `ioNWFramework`, `ioNio` (`io-backends/`) | the backends, in `gears.async.io.kqueue`, `.nwframework` and `.nio` | core |
| `ioTestkit` (`io-testkit/`) | the contract suites every backend runs, and `BufferArena` | core, munit |
| `ioDemos` (`io-demos/`) | the gears demos and `higherlevel`, the higher level the interface is for | the backends |

The interface lives in `shared/`, so it compiles on every platform gears does, Scala.js included; the runtime and the
runners only on the JVM and Scala Native, which have threads to drive reactors with. Nothing in the interface refers
to the runtime: the runner, in the interface's package, starts it.

What the build does not enforce: what the interface hides from programs it hides by package (`private[io]`:
`CallerBackend.performHere`, `Slot.set`), which `gears.async.io.internal` and every backend's package are subpackages
of. That keeps a program from performing a backend's ops but through an `Io`, and a backend outside this repository
would have to be written in a package under `gears.async.io` too. A handle is an object of its backend's own class
(`Backend.Handle`, `KqueueBackend.Handle` and the others), whose constructor is private to the backend's package, so a
program can neither forge one nor pass one backend's handle to another, whose `Handle` is a different type.

## The concepts, and why they are orthogonal

Three axes, chosen independently of one another:

- **Capability**: what a backend can do, as traits mixed into a backend and asked for by intersection: `IpSockets`,
  `Files`, `Pipes`, `Resolving`, `Readiness`, `Blocking`, `UnixStreams`, `UnixDatagrams`, `Fifos`. Each is a
  `Backend` narrowed to its own `Ops`; the base `Ops` is read, write, the pooled read, timer and close, and opens
  nothing. Every handle is opened by an op of the capability that has it (`connect`, `listen`, `datagram`, `pipe`,
  `openFifo`, `open`), which writes it into a slot as it completes and leaves the slot empty, with nothing open, if it
  fails or is cancelled: opening may wait, for a connection, a bind or the disk, and some environments, such as
  Network.framework, only open asynchronously. A program asks for a backend with one as `SocketBackend`, which is
  `Backend & IpSockets`, and likewise for the others.
- **Kind**: how a backend's ops run. A `ReactorBackend` submits them with completions on reactors it opens, and a
  program over it runs on a pool of workers driving those reactors; a `CallerBackend` does each op on the calling
  thread, which may block. A backend is one kind, with any set of capabilities.
- **`Io`**: where a program's ops run. An `Io[B]` has a backend, `backend: B`, and adds only `perform`: it has no
  ops or handles of its own, and it is not a `Backend`. Its kinds are `OnPool`, on a pool of workers driving a
  reactor backend's reactors, which the runtime implements and the runner gives the program it starts, and
  `OnCaller`, over a caller backend, which anyone may make. The type names the backend, and is covariant in it, so
  a program over a `SocketBackend & FileBackend` is given an `Io` over that backend, which a helper asking for
  `Io[IpSockets]` takes. `Io` is also the program's gears `AsyncOperations`.

What each axis does not know: a capability says nothing about threads, kinds or `Io`; a kind says nothing about
sockets or files; an `Io` says nothing about sockets or files either, and reaches them only through its backend. The
runtime is reached through one contract, `OnPool`, which says what a pool offers a program (`perform` and `yield` on
its backend's ops) and which `gears.async.io.internal.Pools.bound` implements; `yield` is `OnPool`'s alone. A program
performs ops and nothing else: ops in flight together are futures performing them (`Future(io.perform(op))`), raced,
awaited and cancelled as futures are. There was an `OnPool.submit`, an op started with a completion of the caller's
own, until 2026-10-25: nothing but a bench and its own tests used it, and its submissions' membership of their group
was half the cost of a timer. An op as a passive `Future` of its own, `performAsync`, without spawning one, is a
direction to explore.

## A program

```scala
def echo(using io: Io[IpSockets])(client: io.Handle)(using Async): Unit = {
  val buf = ...
  val read = io.backend.ops.read(client, buf) // ops are the lowest level: a mutable record the caller owns, reused
  val write = io.backend.ops.write(client, buf)
  while { buf.clear(); io.perform(read); buf.position() > 0 } do {
    buf.flip()
    while buf.hasRemaining() do io.perform(write)
  }
}

ReactorFutures.run(using NioOnCaller) { ... echo(client) ... }
```

`ReactorFutures.run` dispatches once by the backend's kind and returns what the program returns, as `Async.blocking`
does; the run that takes `workers` and a `policy` exists only for a `ReactorBackend`. Inside, gears is gears:
`Future`, `Async.group`, `sleep`, `withTimeout` and `Timer` work, the last three on the backend's timers, since the
`Io` is the `AsyncOperations`; a future performing an op is cancelled with its group, and its op with it, which the
group's end waits for. The runner gives the program its `Io` as
a context parameter, alongside `Async.Spawn`, which the program names where it uses it (`io ?=>`); code over a
program, such as the higher level, takes it as one too, named `io` (`using io: Io[IpSockets]`). An op's type and a
handle's are path-dependent on it (`io.Op`, `io.Handle`), so `perform` is a method of `io`, and a method taking a
handle takes the `Io` first (`def echo(using io: Io[IpSockets])(client: io.Handle)`). Time limits are an `Int` of
milliseconds, the op's own unit.

## What the interface fixes, and what it leaves

Fixed, as semantics every backend must keep, and stated on `Io.perform` where a wrapper reads them: a cancelled op
leaves its handle usable and its buffer the caller's once `perform` returns, which waits until the kind has stopped
the op; a time limit on a submission withdraws the op as a cancel would and fails it with
`TimeoutException`; a reactor backend's op is performed only where a worker can run it, which the type of `Io`
guarantees; readiness is an op that completes, level-triggered, with the handle then used without waiting; a
blocking op's body is skipped if cancelled before it starts and interrupted if running, and its result is never
written after a cancel.

Left to a higher level, on purpose: ops, `perform` and slots are mutable records the caller owns and reuses, the
lowest level there is. A result-returning or streaming API wraps that mutability; the interface only makes sure it
can be written once over every backend.

## The higher level

`higherlevel`, in `ioDemos`, is the API that wraps the lowest level: written over the interface's public contract
alone, in a package outside `gears.async.io` so that it sees only what a program sees, and run on every backend.
`HigherLevel` takes the `Io` as a context parameter, and has result-returning openers (`connect`, `listen`, `datagram`, `pipe`, `openFifo`), `read`,
`writeAll`, `accept` and `resolve`, and `chunks`, a stream over one repeatable pooled read consumed as a gears channel,
with the pool's window and the channel's backlog as back-pressure, failure rethrown to the consumer, and chunks read
ahead released when the consumer stops early. `HigherLevelChecks` runs over every backend in the demos' tests; the
demos open their sockets, pipes and FIFOs through it, the whois demo resolves and reads through it, the key-value and
server-socket demos accept through it, and the stream server closes through `closeGracefully`. The stream demos read
their one frame each way with `FrameReader`, which limits each read to what the frame still needs, so `chunks`, whose
reads fill whatever arrives, is exercised by `HigherLevelChecks` rather than by a demo.

What the wrapper shows the interface costs it: a read's byte count is a buffer-position delta, and there are two slot
types since a `Handle` is an unboxed `Int`, which are the mutability a higher level wraps; and a stream spawns a
future to feed its channel, as a `Future` is the gears way to turn a loop of ops into a channel.

## Design notes

- [`handles.md`](handles.md): handles outlive the I/O object they named, since numbers are reused; a generation in the
  handle, a number that only grows, or a reference object that cannot be forged, each checked as the program runs, and
  handles tied to gears scopes, enforced by capture checking.
- [`files.md`](files.md): file I/O in the reactor, what macOS offers for it, and the shape the file ops take.
- [`interface-history.md`](interface-history.md): why the interface is this shape: each decision, the alternative it
  replaced, and why.
- [`runtime.md`](runtime.md): the pool the reactor backends run on: workers, run queues, take policies, tasks moving
  between threads, fairness, failures, and what macOS's per-process locks mean for scaling.
- [`kqueue.md`](kqueue.md): the kqueue backend on Scala Native: what each op costs in system calls, how it got there,
  and how it compares with cats-effect.
- [`jvm.md`](jvm.md): the JVM's backends, `NioOnCaller` and `NioBackend` on the pool: why each waits as it does, what
  was measured, and what was tried and dropped.
- [`nwframework.md`](nwframework.md): Network.framework as a reactor backend, an experiment kept as the reference for
  a completion-based one, and Skywalk as a possible backend without Dispatch.
- [`scala-native.md`](scala-native.md): costs only Scala Native can remove, the changes proposed to it, and the
  workarounds meanwhile (the `setjmp` override, the `Unblocked` bindings).
- [`bench.md`](bench.md): the bench scenarios, the scripts that build and compare builds, and how to measure well.
