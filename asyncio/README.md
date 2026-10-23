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

## The goal, as criteria, and where each stands

The goal of the restructuring, in its own words: *improve the orthogonality of concepts, separate implementation from
interface, but traits are ok to share implementation; make it more idiomatic to gears, but bear in mind that ops,
perform and slots is the lowest level detail, and that a higher level API would eventually wrap this mutability for
either result returning methods or streaming APIs.* Each clause below is given a criterion that can be checked
without trusting this document, how to check it, and whether it is met or, where it is not, why the gap is accepted
against the goal's wording. The checks that can be mechanical are `asyncio/criteria.sh`, run from the repository's
root: it greps the sources and the build for each and fails if any fails, so the status below is a measurement, not
a reading. The criteria were also checked twice by readers given the code and the goal and not this document's
claims: a review that found the gaps fixed in item 28 of `asyncio-jvm/nio/TODO.md`, and a verification of these
criteria (item 29) that ran each check, judged the two comparative clauses against the state before the goal (commit
`5a5734b`) and found orthogonality *improved* and programs *more idiomatic*, and asked for the four things item 29
lists, all done.

### Orthogonality of concepts

*Criterion*: a capability trait mentions no kind or thread; a kind mentions no capability; a runner mentions no
backend; a program reaches the runtime through `Pool` alone. *Check*: the first five checks of `criteria.sh`: the
interface names nothing below it, no capability trait names a kind, a pool, a worker or a thread, no kind names a
capability, neither the runtime nor a runner names a backend, and `Pooled` forwards to `Pool` alone; and the build,
where a runner depends on a backend in `Test` only. *Status*: met. Each capability refines `Backend.Io` to carry itself, which is the one thing
the capability axis knows of the `Io` axis, and is what lets a program over `SocketBackend & FileBackend` be given an
`Io` with both. **Accepted**: `Io extends Backend`, since an `Io` must be a `SocketBackend` for a helper over
`Io & SocketBackend` to take it; what that allowed, an `Io` on a pool passed to the runner as a backend, is refused
when compiling (`ReactorFutures.run` takes `NotGiven[backend.type <:< OnPool]`, checked by a test), so nothing is
left to the run.

### Implementation apart from the interface

*Criterion*: the interface module depends on the gears library alone; every body in it is a default the contract
itself states, or a utility over the contract. *Check*: `criteria.sh`, which reads the module's dependencies off
the build and lists every def with a right-hand side in the interface against the allowed set. *Status*: met. The bodies are `perform(op)` (`perform`
with no limit), `sleep` (a timer op), `Completion.onFailure` (throws) and `onCancel` (nothing), `Submitter.submit` and
`trySubmit` with no limit, `Reactor.scoped` (open, use, close), `Slot`, `Handle.Opt` and `Handle.Slot` (the records
ops write), which the goal's *traits are ok to share implementation* covers, and nothing else: `Handle.Table` moved
out to `asyncio.unsafe.HandleTable`, the pool's machinery is `asyncio.gears.Pools` behind `Pool`, the forwarding to it
is `asyncio.unsafe.Pooled`. `Io`, `OnPool`, `OnCaller`, `Submission` and `Pool` are in the interface because they are
interface: contracts a program or the runtime is written against, with no bodies. **Accepted**: hiding is by package,
not module, as said above, since Scala has no module-private; the build fixes the direction of dependency and the
package fixes who may call `bind`, `Slot.set` and `Handle.wrap`.

### Idiomatic to gears

*Criterion*: a program is written with gears' own vocabulary and nothing of the backend's beyond `perform`: a
context function over `Async.Spawn` and `AsyncOperations`, returning its value; `Future`, `Async.group`, `sleep`,
`withTimeout`, `Timer` and cancellation mean what they mean in gears; anything the program may wait on or cancel is a
gears `Cancellable` and an `Async.Source`, so it composes in `select` with a channel or a timer. *Check*:
`FuturesContract`, the one suite every backend runs, which uses only those, has `sleep` and `withTimeout` run on the
backend's timers, has cancelling a future cancel its op, has a scope's end cancel a submission made in it and wait
for its completion, races a submission against a timer in `Async.select`, and takes every result from the value
`running` returns; `criteria.sh` checks those tests are present and that the contract holds no result variables.
*Status*: met. `ReactorFutures.run` returns the program's value as `Async.blocking` does; `Submission` is a
`Cancellable` in the caller's group, as a `Future` is, and a `Source` of the op's outcome, as a `Future` is, so
`OnPool.submit(op)` with no completion of the caller's own is the source view of an op. **Accepted**, as the lowest
level the goal names: the `Io` is an explicit parameter where gears passes `Async` as `using`, because an op's type
is path-dependent on it (`r.Op`), so `perform` has to be a method of `r` and a program names it on every line,
unlike an `Async`; time limits are an `Int` of milliseconds, the op's own unit.

### The lowest level stays the lowest

*Criterion*: ops, `perform` and slots keep their meaning as mutable records the caller owns and reuses; no op but
`blocking` takes a closure; the guarantee a wrapper relies on is stated where it reads it; and a higher level of
both kinds the goal names can be written over the interface's public contract alone, runs on every backend, and is
used by real programs. *Check*: `criteria.sh`: the closures in `Ops.scala`, the guarantee on `Io.perform`, the
module `higherlevel` depending on the interface alone and its source naming nothing below it, its checks mixed into
every backend's contract suite, and the demos that use it. The module (`higherlevel.HigherLevel`, outside the
`asyncio` package so it sees only what a program sees) has result-returning `read`, `writeAll`, `accept` and
`resolve`, and `chunks`, a stream over one repeatable pooled read consumed as a gears channel, with the pool's window
and the channel's backlog as back-pressure, failure rethrown to the consumer and chunks read ahead released when the
consumer stops early; `HigherLevelChecks` runs in every backend's contract suite; the whois demo resolves and reads
through it and the socket demo takes its response as a stream of chunks. *Status*: met. `whenReady` with
its closure became `ready`, an op that completes; `Io.perform` states that once it returns or throws the op is out of
use everywhere, its buffers and slots the caller's and its handle as open as it was. What the wrapper shows the
interface costs it: a read's byte count is a buffer-position delta, and there are two slot types since a `Handle` is
an unboxed `Int`, which are the mutability the goal says a higher level wraps; and a stream spawns a future to feed
its channel, as a `Future` is the gears way to turn a loop of ops into a channel.
