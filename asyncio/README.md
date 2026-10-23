# asyncio: the interface, and what sits on either side of it

Three modules, each depending only on the one before it:

| module | holds | depends on |
| --- | --- | --- |
| `asyncio` | the interface: `Submitter`, `Reactor`, `Backend` and its kinds and capabilities, `Io` and `Pool`, `Ops`, `Handles`, `Slot`, addresses, `BufferPool` | the gears library |
| `asyncio-unsafe` | implementation the backends share: `Blockers`, `TimerHeap`, `Pooled` | `asyncio` |
| `asyncio-gears` | the runtime: `ReactorPool`, `ReactorWorker`, `RunQueue`, `TakePolicy`, `ReactorScheduler`, `Pools` | `asyncio` |

The backends (`asyncio-native/loop`, `asyncio-native/nwframework`, `asyncio-jvm/nio`) depend on the first two; the
platform runners (`asyncio-native/gears`, `asyncio-jvm/gears`) on the backends and the runtime. A reference from the
interface to anything below it does not compile.

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
an `Io` carries both and adds only `perform`. The runtime is reached through one contract, `Pool`, which says what a
pool offers a program (`perform`, `submit`, `yield` on a reactor backend's ops) and which `asyncio.gears.Pools`
implements.

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

`ReactorFutures.run` dispatches once by the backend's kind; the run that takes `workers` and a `policy` exists only
for a `ReactorBackend`. Inside, gears is gears: `Future`, `Async.group`, `sleep`, `withTimeout` and `Timer` work, the
last three on the backend's timers, since the `Io` is the `AsyncOperations`.

## What the interface fixes, and what it leaves

Fixed, as semantics every backend must keep: a cancelled op leaves its handle usable and its buffer the caller's once
`perform` returns; a time limit on a submission withdraws the op as a cancel would and fails it with
`TimeoutException`; a reactor backend's op is performed only where a worker can run it, which the type of `Io`
guarantees; readiness is an op that completes, level-triggered, with the handle then used without waiting; a
blocking op's body is skipped if cancelled before it starts and interrupted if running, and its result is never
written after a cancel.

Left to a higher level, on purpose: ops, `perform` and slots are mutable records the caller owns and reuses, the
lowest level there is. A result-returning or streaming API wraps that mutability; the interface only makes sure it
can be written once over every backend.

## Review against the goals of the restructuring

- *Orthogonal concepts*: the three axes above, with nothing shared between them but `Op`. Removed on the way:
  `CompletionBackend`, the top-level `perform` and `submit`, the caller-side hooks on `ReactorBackend`, `Carriers`,
  `Backend.start` and `Runner`, `whenReady` with its closure, and the capability lists each NIO backend repeated.
- *Implementation apart from the interface*: enforced by the build. The interface module imports the gears library
  and the JDK. The only bodies in it are defaults the contract itself states (`perform(op)` is `perform` with no
  limit; `sleep` is a timer op). Shared implementation is traits in `asyncio-unsafe` (`Pooled`) and the runtime's
  own module.
- *Idiomatic to gears*: programs are context functions over `Async.Spawn` and `AsyncOperations`; ops are performed
  as methods on the value gears hands the program; gears' own timing functions run on the backend's timers; the
  pool's `yield` is the scheduler's.
- *The lowest level stays the lowest*: ops, `perform` and slots are untouched in meaning; no op but `blocking` takes a
  closure, and that one's closure is the op.
