# asyncio: the interface, and what sits on either side of it

Three modules, each depending only on the one before it:

| module | holds | depends on |
| --- | --- | --- |
| `asyncio` | the interface: `Submitter`, `Reactor`, `Backend` and its kinds and capabilities, `Io` and `Pool`, `Ops`, `Handles`, `Slot`, addresses, `BufferPool` | the gears library |
| `asyncio-unsafe` | implementation the backends share: `Blockers`, `TimerHeap`, `HandleTable`, `Pooled` | `asyncio` |
| `asyncio-gears` | the runtime: `ReactorPool`, `ReactorWorker`, `RunQueue`, `TakePolicy`, `ReactorScheduler`, `Pools` | `asyncio` |

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

- **Capability**: what a backend can do, as traits a program asks for by intersection: `SocketBackend`,
  `FileBackend`, `PipeBackend`, `ResolveBackend`, `ReadinessBackend`, `BlockingBackend`, `UnixStreamBackend`,
  `UnixDatagramBackend`, `FifoBackend`. Each is its `Ops` and `Handles`; the base `Ops` is read, write, the pooled
  read, timer and close, and the base `Handles` opens nothing.
- **Kind**: how a backend's ops run. A `ReactorBackend` submits them with completions on reactors it opens, and a
  program over it runs on a pool of workers driving those reactors; a `CallerBackend` does each op on the calling
  thread, which may block. A backend is one kind, with any set of capabilities.
- **`Io`**: what a program performs with. A backend bound to where the program runs: `OnPool` for a reactor backend,
  made only by the runner (`ReactorBackend.bind`), or the caller backend itself (`OnCaller`). Each capability narrows
  a backend's `Io` to itself, so a program over a `SocketBackend & FileBackend` is given an
  `Io & SocketBackend & FileBackend`. `Io` is also the program's gears `AsyncOperations`.

What each axis does not know: a capability says nothing about threads; a kind says nothing about sockets or files;
an `Io` carries both and adds only `perform`. What a capability does know is the `Io` axis: each refines `Backend.Io`
to carry itself, which is what lets a program over a `SocketBackend & FileBackend` be given an `Io` with both. The
runtime is reached through one contract, `Pool`, bound once to a backend's `Op` (`Pool.Of[Op]`), which says what a
pool offers a program (`perform`, `submit`, `yield` on that backend's ops) and which `asyncio.gears.Pools` implements.
`submit` and `yield` are `OnPool`'s alone: a program written over `Io` runs on either kind, one that submits with its
own completion on reactor backends only.

## A program

```scala
def echo(client: Handle)(using Async, r: Io & SocketBackend): Unit = {
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
scope's end cancels it and waits for its completion, as it does a `Future`.

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

## Review against the goals of the restructuring

Reviewed independently, against the goal's wording, by a reader given the code and the goal and not this document's
claims (the findings are recorded in `asyncio-jvm/nio/TODO.md`, item 28). What that review found, and what was done:

- *Orthogonal concepts*: the three axes above, with nothing shared between them but `Op` and the `Io` refinement each
  capability carries. Found and fixed: the Native runner matched on kqueue's reactor to print its counters and the
  build let it depend on the kqueue backend, so the runner depended on one backend and the Network.framework backend
  on the runner (a reactor now prints its own counters, and the runner sees backends in its tests only);
  `BlockingBackend` was documented as a reactor backend's capability while `NioOnCaller` has it (it now promises the
  same on either kind: the body runs where it may block, with the time limit and cancellation interrupting it);
  `Pool` took the backend on every call to line up types (bound once, `Pool.Of[Op]`). Left: `Io extends Backend`, so
  an `Io` may be passed where a `Backend` is wanted, which the runner refuses when run, not when compiled.
- *Implementation apart from the interface*: the direction of dependency is enforced by the build. Found and fixed:
  `Handle.Table`, a table of I/O objects two backends used, was in the interface (now `asyncio.unsafe.HandleTable`).
  The bodies left in the interface are defaults the contract itself states: `perform(op)` is `perform` with no limit,
  `sleep` is a timer op, a completion's `onFailure` throws and its `onCancel` does nothing, `Reactor.scoped` opens and
  closes. Shared implementation is traits and classes in `asyncio-unsafe` (`Pooled`, `HandleTable`) and the runtime's
  own module. Hiding is by package, as said above.
- *Idiomatic to gears*: programs are context functions over `Async.Spawn` and `AsyncOperations`; ops are performed
  as methods on the value gears hands the program; gears' own timing functions run on the backend's timers; the
  pool's `yield` is the scheduler's. Found and fixed: `ReactorFutures.run` returned `Unit` where gears' entry points
  return the body's value, so every result left the program through a `var` (it returns the value); a `Submission`
  was outside structured concurrency, cancelled only by hand or by closing the reactor (it is a `Cancellable` in the
  caller's group). Left, as the lowest level: a submission is not an `Async.Source`, so selecting an op against a
  channel takes a `Future` per op; the `Io` is an explicit parameter where gears passes capabilities as `using`; time
  limits are milliseconds in an `Int`.
- *The lowest level stays the lowest*: ops, `perform` and slots are untouched in meaning; no op but `blocking` takes a
  closure, and that one's closure is the op. Found and fixed: the guarantee a wrapper relies on, that a cancelled op
  is out of use once `perform` returns, was stated here and not on `perform` (it is on `perform`). The hook a
  streaming API will want next is a source view of an op's completion, which the runtime has (`Pools.OpWaiter`) and
  the interface does not yet expose.
