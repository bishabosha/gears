# asyncio: the interface, and what sits on either side of it

Three modules, each depending only on the one before it:

| module | holds | depends on |
| --- | --- | --- |
| `asyncio` | the interface: `Submitter`, `Reactor`, `Backend` and its kinds and capabilities, `Io` and `Pool`, `Ops`, `Handles`, `Slot`, addresses, `BufferPool` | the gears library |
| `asyncio-unsafe` | implementation the backends share: `Blockers`, `TimerHeap`, `HandleTable`, `Pooled` | `asyncio` |
| `asyncio-gears` | the runtime: `ReactorPool`, `ReactorWorker`, `RunQueue`, `TakePolicy`, `ReactorScheduler`, `Pools` | `asyncio` |
| `higherlevel` | the higher level the interface is for: result-returning methods and a stream of chunks over `Io` | `asyncio` |

The backends (`asyncio-native/loop`, `asyncio-native/nwframework`, `asyncio-jvm/nio`) depend on the first two; the
platform runners (`asyncio-native/gears`, `asyncio-jvm/gears`) on the runtime, and on the backends only in their
tests. A reference from the interface to anything below it, or from a runner to a backend, does not compile. The
Native implementation the two Native backends share (`NativePool`, the Posix sockets and files) is kept in the loop
module's `asyncio.unsafe` package, so the Network.framework backend depends on the loop module for it.

What the build does not enforce: what the interface hides from programs it hides by package (`private[asyncio]`:
`bind`, `Pool`, `Slot.set`, `Handle.wrap`), and every module of this repository lives in a package under `asyncio`.
That keeps a program from performing an op where it cannot run, and a backend outside the repository would have to be
written in a package under `asyncio` too.

## The concepts, and why they are orthogonal

Three axes, chosen independently of one another:

- **Capability**: what a backend can do, as traits mixed into a backend and into its `Io` alike, and asked for by
  intersection: `IpSockets`, `Files`, `Pipes`, `Resolving`, `Readiness`, `Blocking`, `UnixStreams`,
  `UnixDatagrams`, `Fifos`. Each is its `Ops` and `Handles` over `Facilities`, what a backend and an `Io` have in
  common; the base `Ops` is read, write, the pooled read, timer and close, and the base `Handles` opens nothing. A
  program asks for a backend with one as `SocketBackend`, which is `Backend & IpSockets`, and likewise for the
  others; a helper over a program's `Io` asks for the capability itself, `Io & IpSockets`.
- **Kind**: how a backend's ops run. A `ReactorBackend` submits them with completions on reactors it opens, and a
  program over it runs on a pool of workers driving those reactors; a `CallerBackend` does each op on the calling
  thread, which may block. A backend is one kind, with any set of capabilities.
- **`Io`**: what a program performs with. A backend's facilities bound to where the program runs: `OnPool` for a
  reactor backend, made only by the runner (`ReactorBackend.bind`), or the caller backend itself (`OnCaller`). It is
  not a `Backend`: the two share their capabilities and nothing else. Each capability narrows a backend's `Io` to
  itself, so a program over a `SocketBackend & FileBackend` is given an `Io & IpSockets & Files`. `Io` is also the
  program's gears `AsyncOperations`.

What each axis does not know: a capability says nothing about threads, kinds or backends; a kind says nothing about
sockets or files; an `Io` carries the capabilities and adds only `perform`. What a capability does know is the `Io`
axis: each refines `Facilities.Io` to carry itself, which is what lets a program over a `SocketBackend & FileBackend`
be given an `Io` with both. The
runtime is reached through one contract, `Pool`, bound once to a backend's `Op` (`Pool.Of[Op]`), which says what a
pool offers a program (`perform`, `submit`, `yield` on that backend's ops) and which `asyncio.gears.Pools` implements.
`submit` and `yield` are `OnPool`'s alone: a program written over `Io` runs on either kind, one that submits with its
own completion on reactor backends only.

## A program

```scala
def echo(client: Handle)(using Async, r: Io & IpSockets): Unit = {
  val buf = ...
  val read = r.ops.read(client, buf) // ops are the lowest level: a mutable record the caller owns, reused
  val write = r.ops.write(client, buf)
  while { buf.clear(); r.perform(read); buf.position() > 0 } do {
    buf.flip()
    while buf.hasRemaining() do r.perform(write)
  }
}

ReactorFutures.run(using NioOnCaller) { r => ... echo(client)(using summon[Async], r) ... }
```

`ReactorFutures.run` dispatches once by the backend's kind and returns what the program returns, as `Async.blocking`
does; the run that takes `workers` and a `policy` exists only for a `ReactorBackend`. Inside, gears is gears:
`Future`, `Async.group`, `sleep`, `withTimeout` and `Timer` work, the last three on the backend's timers, since the
`Io` is the `AsyncOperations`; a `Submission` is a `Cancellable` linked to the group it was submitted in, so the
scope's end cancels it and waits for its completion, as it does a `Future`. The `Io` is an explicit parameter
where gears passes `Async` as `using`, because an op's type is path-dependent on it (`r.Op`), so `perform` has to be
a method of `r`; time limits are an `Int` of milliseconds, the op's own unit.

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

The module `higherlevel` is the API the goal says would wrap the lowest level: written over the interface's public
contract alone, in a package outside `asyncio` so that it sees only what a program sees, and run on every backend.
`HigherLevel` has result-returning `read`, `writeAll`, `accept` and `resolve`, and `chunks`, a stream over one
repeatable pooled read consumed as a gears channel, with the pool's window and the channel's backlog as
back-pressure, failure rethrown to the consumer, and chunks read ahead released when the consumer stops early.
`HigherLevelChecks` runs in every backend's contract suite; the whois demo resolves and reads through it, the socket
demo takes its response as a stream of chunks, and the key-value and server-socket demos accept through it.

What the wrapper shows the interface costs it: a read's byte count is a buffer-position delta, and there are two slot
types since a `Handle` is an unboxed `Int`, which are the mutability a higher level wraps; and a stream spawns a
future to feed its channel, as a `Future` is the gears way to turn a loop of ops into a channel.
