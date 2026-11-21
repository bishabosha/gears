# Flows, sinks and servers over submission queues

Status: experiment, on the branch `experiment/reactor`. Nothing here is on the main branch, and nothing has tests of
its own yet. Measurements are of 2026-11-19, on one macOS machine, Scala Native
`0.5.13-20261114-143517b-SNAPSHOT`.

## Where it stands

This document was written as the experiment went, and much of it is of things since taken out. What is on the branch
now:

- `Shards` (`io.shards`, `io.onShard(n)`) and `SubmissionQueues` (`io.performQueue`), each a capability of its own.
- `Flow`, written as a generator (`Flow { ... emit(x) ... }`) or made of stages, with its combinators; `Sink`.
- `Server.serve(handler)`, a handler that is run in a future for each connection, pinned to a shard where there are
  shards, what it sends written once a turn; and `Server.serveFlow(pipeline)`, a pipeline of stages that is stepped,
  by a machine over the submission queue where there is one. `mapAsync` is the stage that waits.
- In the core, `Async.park`, a wait with no listener that a cancel still ends, and a future begun with fewer frames
  under it.

Taken out, with the sections on them left below as the record of why: the reactor, flows as continuations of one
future, and the three ways of serving that were made of it. Nothing it did was the fastest on any backend once
futures were pinned and parked cheaply and a connection's streams were stages; on Node.js it had hazards of its own.
What only it used went with it: the hooks that said whether a flow was a continuation and whether an op waited
through `Async`. `Io.startSubmitted` and what implements it are left, unused.

Its measurements below are of code that is no longer there.

## Which stages are stepped

A stage is stepped if it never waits. Those are `map`, `filter`, `withFilter`, `collect`, `mapConcat`, `tap`,
`process`, `mapStateful`, `mapUsingEmit`, `groupThrough`/`groupBy`, and, since they were given a step each, the ones
that keep something between elements (`drop`, `dropWhile`, `scan`, `zipWithIndex`, `changes`, `intersperse`,
`grouped`, which pushes what it has left at the end) and the ones that want no more (`take`, `takeWhile`). `mapAsync`
is the one that waits and is stepped all the same, and `mapPar` is `mapAsync` keeping the order: what is done
first waits its turn in a place kept for it. Work of either still under way when its connection is closed, by a
failure or by a stage that wants no more, is cancelled.

A stage that wants no more ends the step after it, drops what still comes, and tells what steps the pipeline
(`Drive.stop`): the machine reads no more of that connection and closes it when what was made has been written. In a
key's pipeline under `groupThrough` it ends the key, whose later elements are dropped, and leaves the rest.

What is not stepped is what runs something beside something else (`zip`, `merge`, `++`, `flatMap`) and a
`Flow { ... }` of code. One type is kept for both, as Ox has one: `serveFlow` no longer refuses a pipeline that is
not of stages alone but runs that connection in a future, as `serve` would, so that which a pipeline is decides how
fast it is served and not whether. A test runs every stage both ways and compares.

## What is guaranteed at the end

A server ends in one of two ways. `close()` is the gentle one: no more connections are accepted, those there are
are served until their peers end, and what they made is written. A cancel of the future that called `serve` or
`serveFlow` is the other, and what follows is about that, and about a connection that fails.

**What is run, whatever was cancelled.**

- Every accepted connection is closed, once, and the listener is closed. These are performed under a group that
  nothing cancels, by the machine as by a connection's own future.
- `onComplete(atLast)` on a flow is run once when the flow has ended, however it did: at its last element, at a
  failure, at a cancel, or because a stage after it wants no more (`take`). It is where a stage closes what it
  opened. Run, it is a `finally`; stepped, it is `Step.abandon`, which whatever steps the pipeline calls as it gives
  the pipeline up, and which every step passes on. It is run on the pipeline's thread and must not wait.
- `serve` and `serveFlow` return only when all of that is done and no work of a stage's (`mapAsync`, `mapPar`) is
  still running: that work is in a group of its connection's, cancelled as the connection is closed.

**What is not run.** After a cancel no stage is pushed anything more, though bytes had been read for it. What a
pipeline had made and not yet written is dropped: a cancel does not flush.

**Ops submitted already.** The machine has ops on its submission queue at any moment, and a cancel finds each in
one of three states:

- *Submitted and not started.* It is never started: `SubmissionQueue.withdraw` hands each back as failed with a
  `CancellationException`.
- *In flight.* `performQueue`, cancelled as it waits, gives up every op in flight before it throws, as `perform`
  gives up its one: each is cancelled, unless a thread of the backend has it already, which is then waited for, and
  handed back. So a write in flight may have written some, all or none of its bytes.
- *Done, and not yet looked at.* Its bytes were read, or written; the machine no longer acts on it.

So after a cancel every op the queue was given is accounted for and none is in use, which is what lets the
connections' buffers be let go. A close that was itself cut short is asked for again: the one op that is always
performed.

Tests, on every backend, served both ways: a server cancelled with three connections with a minute's work under
way and one idle (the work cancelled, each `onComplete` run once, every peer sees the end, the listener refuses);
`onComplete` at an end, a failure and a `take`; and the same cancel under each strategy of `serve`.

Not guaranteed, and to be decided: there is no time limit on the gentle close; `onComplete` cannot wait, so a
resource whose release is itself an op has nowhere to do it; and elements lent to a stage (a read's buffer) are
still lent by convention.

## Failures

One failure ends the flow unless something recovers it, as in ZIO's streams, fs2 and Ox: after a stage has thrown,
what the stages around it keep is in no known state. A failure is an exception; what is to be handled element by
element is made a value by the stage's own function.

A failure is a third thing a step is told, beside an element and the end: `Step.fail(thrown)`, in place of the end,
passed on by every step that makes nothing else of it. The step with nothing after it throws it on as `Unrecovered`
to whatever steps the pipeline, which fails what the pipeline was for. A stage that throws as it is pushed an
element throws *up*, through the stages before it; what steps the pipeline catches that and tells the first step, so
every step hears of it, and a step that must tell the stages before it from those after notes what the step after
it threw.

- `recover(handler)`: a failure of the stages before it becomes one last element and the end. Not one of a stage
  after it, and not a cancel.
- `onError(see)`: shown such a failure, which goes on as it was.
- `onComplete(atLast)`: as before, run at a failure too.

**Scope.** What fails is the nearest pipeline: a key's under `groupThrough`, then the connection's. A key's failure
that its own pipeline recovers ends that key and leaves the others; one that it does not is the whole flow's, where
a `recover` after the `groupThrough` may still take it. A failure before the keys is told to each key's pipeline and
then to what follows them. Work of `mapAsync` that throws is its stage's failure, in its key. A connection's
failure is not its server's other connections'.

**A stage that is code: `direct`.**

```scala
requests.direct[Response] { requests =>
  val session = open()
  try for request <- requests do emit(session.answer(request))
  catch case _: Malformed => emit(Response.BadRequest)
  finally session.close()
}
```

The body is written as a `Flow { ... }` is, and its `try` is the stage's error handling: a failure of the flow it is
given is thrown from its loop over that flow; what it throws is the stage's failure, for the stages after; what a
stage after it throws is kept from its `catch` (its `finally` is still run). It may wait, perform ops, keep what it
likes and leave early.

It is a stage, so a pipeline with it in is still stepped. Stepped, the body is a future beside the pipeline
(`Drive.start`): pushed elements go to it through a queue, with the source held when it is sixty-four behind; each
`emit` hands the element back to be pushed on where the stages are called, and the body goes on when it has been;
it is cancelled with its connection, or its key. Under a pipeline that is itself run (a key's, in a flow that is
run) what runs the pipeline waits for the body instead (`Drive.waits`), so nothing is beside anything. That is a
hand-over an element each way, which the stages that are functions have not: those for what must be quick, this
for what is easier said as code.

Tests: every stage, these among them, run and stepped and compared, what is thrown as well as what is emitted; on
each backend, served both ways, a `direct` stage that performs an op for each element, recovers a failure of the
stage before it in its own `catch`, and has its `finally` run when the server is cancelled; and a failure of one
key's `mapAsync` recovered in that key while the other key goes on.

Not done: the server still throws the first connection's failure when it returns, where it should hand each to
something that reports it and keep its own failures for itself; `orElse` and `retry`.

**Scala Native skips a `finally`** whose `catch` calls a method that throws: `try A catch { case _ => f() }
finally C`, with `f` throwing, does not run `C`. It is the platform's, with no gears in it, from 0.4.17 to 0.5.12 and
the nightly, on Scala 2.13 and 3; the JVM and Scala.js run it. A handler that throws by itself, the call in a `try`
of its own, and the `try`/`catch` written inside a `try`/`finally` all run it. It was met here because a cancel is
an `IllegalStateException`, which the test's handler caught, and its `emit` threw again. A twelve-line program and
what does and does not set it off are in `scala-native-finally-repro`, beside this repository.

The machine's figure with all of this in is two hundredths behind what it was (kqueue, six rounds: 2.57-2.73
against 2.52-2.65 on one worker, 0.81-0.84 against 0.79-0.84 on four), the cost being the `try` each element of a
key now passes.

## Failures as values

The other way to say a failure, and the one to prefer: as a value, a `Result` (lampepfl/steps), with nothing
thrown and nothing caught. A flow that may fail is a `Flow[Result[A, E]]`: an `Err` is an element like any other, and
the error has a type.

- `failFast`: the flow as far as its first `Err`, which is its last element. One failure ends the flow, as before;
  here it is the stages that may make an `Err` that do it, and a flow of results made otherwise goes on past one.
- `flatMapOk(f)`, for a stage that may fail, which ends at the first `Err` it makes; `mapOk`, `mapErr`, `tapErr`.
- `handleErr(f)`: the values, an `Err` replaced by what `f` makes of it. What recovers.
- `attempt`: from a flow that throws to a flow of results, what was thrown a last `Err`.
- `runResult()` and `toListResult()`: run it for a `Result`.
- `directResult[B, E] { elements => ... }`: a stage that is code whose body is a `Result.task`: `.ok` unwraps, `raise`
  fails, and the stage emits `Ok` of what the body emits and then the `Err` it raised, if it did. `directOk` is that
  over the values of a flow of results, ended by the first `Err` that comes.

```scala
requests.directResult[Response, Malformed] { requests =>
  val session = open()
  try for request <- requests do emit(session.answer(parse(request).ok))
  finally session.close()
}.handleErr(Response.badRequest)
```

They are all stages, made of `map`, `Until` and `direct`, so stepped, and what ends at an `Err` in a key's pipeline
ends that key. Cleanup is still a `finally`, with no `catch` beside it: that is run however the stage ends, a cancel
among the ways, and on Scala Native too, where it is a `catch` whose handler calls something that throws that loses
the `finally` (scala-native#5367). A test serves such a stage both ways on every backend and cancels the server with
the stage waiting; its `finally` is run on all of them.

Thrown failures are still what the section above says, for what does throw and for Ox's names. `.ok` and `raise` in
a loop over a flow leave the loop as `boundary.break` does, which across the loop's function is a `Break` thrown by
the language: nothing of ours throws or catches for it.

## Against the same server written by hand

The multiplexed server with no flow in it, over `perform` (`flow-mux-hand-*`): `loop`, a future for each
connection that reads, answers every frame that came and writes, in a `while`; `pinned`, that with a connection's
ops on one shard; `queue`, a future for each shard performing a submission queue, the machine written out for this
one server. Same clients, same bytes. Microseconds a request, four rounds, each in a process of its own (JVM: one
process, the last six of ten repeats):

| | kqueue, 1 worker | kqueue, 4 | Node.js | JVM pool, 4 |
|---|---|---|---|---|
| by hand, a queue for each shard | 2.41-2.47 | 0.76-0.78 | 3.83-3.88 | 0.26-0.39 |
| by hand, a loop for each connection, pinned | 2.54-2.58 | 0.81-0.85 | no shards | 0.26-0.36 |
| by hand, a loop for each connection | 2.57-2.67 | 0.80-0.86 | 4.08-4.25 | 0.24-0.41 |
| flow of stages, the machine | 2.56-2.59 | 0.82 | 3.99-4.21 | 0.27-0.33 |
| flow of stages, a future for each connection | 2.67-2.74 | 0.88-0.92 | 4.37-4.47 | 0.26-0.36 |
| handler with a flow for each stream, on futures | 3.47-3.99 | 1.45-1.59 | 5.14-5.23 | 0.43-0.55 |

So flows are not faster than loops. The loop and the machine are the same within the noise; the same queue by
hand is a twentieth ahead of the machine, which is what the stages cost (a frame object and its copy, a table
lookup for the stream, the calls); and stages run by a future are a twentieth behind the loop for the same reason.
What was a third or more slower all along was not flows against loops but a future and a channel for each stream
against one loop for the connection.

What the loop is here is the easy case for a loop: one connection's streams answered in the order they came, by
work that never waits. A stream that has to wait for something holds the whole loop, and then the loop needs what
`mapAsync` gives the pipeline, and is no longer a few lines.

Two runs, one on each platform, came back hundreds of times slower, one of them longer than the time limit it was
run under, so the clock had jumped, the machine asleep most likely; 240 further runs on Node.js had none.

## A pipeline as a program, tried

`Pipe` (`FlowVm.scala`) is the same pipeline as instructions: an array of opcodes and their functions, made once
and shared by every connection, with what a stage keeps in registers, an array for each connection. One-to-one
instructions run in a loop; `process`, `mapConcat` and a group enter the loop again at the instruction after.
`Server.serveProgram` has the machine step it. It has `map`, `filter`, `mapConcat`, `scan`, `process`, `mapWith`
and `groupThrough`, and no stage that waits or stops: enough to compare.

Since there is one program, a stage cannot capture what is its connection's, a cache say: that is made for each
connection (`Pipe.local`) and handed to the instructions that ask for it. It is a different thing to write.

Measured against the chain of steps, each scenario in a process of its own, four rounds (JVM: one process, the last
four of eight repeats), microseconds:

| | native kqueue | Node.js | JVM pool |
|---|---|---|---|
| an element through three stages: steps | 0.029 | 0.021-0.027 | 0.016-0.032 |
| the same: program | 0.033 | 0.030-0.034 | 0.008 |
| a connection begun, with sixteen keys: steps | 1.53 | 1.78-1.82 | 0.50-0.54 |
| the same: program | 1.41 | 1.78-1.87 | 0.52-0.56 |
| multiplexed server, a request: steps | 2.75-2.82 (1 worker), 0.84-0.87 (4) | 4.33-4.48 | 0.29-0.39 |
| the same: program | 2.73-2.80, 0.85-0.90 | 4.39-4.48 | 0.36-0.39 |

An element costs more as an instruction than as a call where nothing compiles the loop to the program (a seventh
more on Scala Native, two fifths on Node.js) and less on the JVM, which does. Beginning a connection is a twelfth
cheaper on Scala Native and the same elsewhere: most of it is the keys' table and the stages' own state, which both
have. And the server, where an element's passage is some tens of nanoseconds of a request's microseconds, cannot
tell them apart. So it buys nothing here that is measured, at the price of a second way to write a pipeline.

What it was hoped for and is not measured: with the program in hand the ops a pipeline causes could be rewritten
before they are submitted, a write and the read after it linked, several writes made one vectored write. On kqueue
and NIO there is nothing to rewrite them into; that is a question for an io_uring backend, and the place for it is
the submission queue, which sees the ops whichever way the pipeline is written.

## Ox's flows

Ox's `Flow` (SoftwareMill, 1.0.3) is the same thing made independently: a stage that is run with what to call for
each element (`FlowStage.run(emit: FlowEmit[T])`), made with `Flow.usingEmit`, run with `runForeach`, its `groupBy`
taking what makes a key's flow of the flow it is given. Ours is `foreach(each)`, `Flow { ... emit ... }`,
`groupThrough`. So the two are kept compatible in two ways.

**Names.** Where both have an operator ours has Ox's name and shape, beside its own: `usingEmit`, `fromValues`,
`fromIterable`, `fromIterator`, `mapPar` (in order, as Ox's is), `mapParUnordered`, `mapStateful`, `mapUsingEmit`,
`groupBy(parallelism, key)(child)`, `concat`, `runForeach`, `runToList`, `runDrain`, `runFold`, `runLastOption`,
with `map`, `filter`, `collect`, `mapConcat`, `take`, `takeWhile`, `drop`, `scan`, `grouped`, `intersperse`, `tap`,
`zip`, `zipWithIndex`, `merge`, `flatMap` already the same. A pipeline written with these reads the same for either,
and a test runs one such against both and compares what comes out. What differs: ours take `using Async` to run, and
`f` in `mapPar` may itself wait; `groupBy`'s `parallelism` is not a bound here, a key's pipeline being a step and not
a thread; Ox has many more operators (time, retries, I/O, reactive streams) that ours has not.

**Each as the other** (`io-ox`, package `gears.async.io.oxflow`, JVM). Nothing is between them but the call for
each element: an Ox flow blocks the thread it is run on, a gears flow suspends the future it is run in, and on the
JVM a future is a virtual thread, for which those are one.

- Whole flows: `oxFlow.toGears`, `gearsFlow.toOx`, and `gearsFlow.toOxOnAnyThread` for a caller with no `Async`.
  `toOx` runs with the `Async` of the future that asked when it is run on that future's thread, and with one of
  whatever thread it is on when an Ox operator (`buffer`, `mapPar`, `merge`) runs its upstream in a fork.
- Stages: `gearsFlow.throughOx(pipe: OxFlow[A] => OxFlow[B]): Flow[B]` puts stages of Ox's in a gears flow, and
  `oxFlow.throughGears(pipe: Flow[A] => Flow[B]): OxFlow[B]` puts stages of gears' in an Ox flow, which needs no
  `Async` of its caller's. They nest, and either side may leave early.

Limits: an Ox flow is opaque, so a pipeline with one in it is run, not stepped, and `serveFlow`'s machine refuses
it; and a gears flow run on Ox's threads cannot perform an op of a pool backend, which is performed on the pool's
own.

**Other platforms.** Ox is published for the JVM only: there is no `core` for Scala Native or Scala.js at 1.0.3. On
those the names are what is kept, so that code written against the shared operators moves when Ox does.

## What it is for

A server that multiplexes many streams over one connection (HTTP/2 is the model) wants three things that a future
for each stream does not give cheaply:

- one wait for whatever a turn of the backend brings, not one for each op;
- state shared by a connection's streams with no lock;
- every stream's answer of a turn written with one write.

The lower layers give these but are not what a program should be written against. This design is the layer a
program is written against, and what it rests on.

## The layers

| Layer | What it is | Who uses it |
|---|---|---|
| `Server`, `Connection`, `Flow`, `Sink` | streams and their combinators, a listener served on every shard | programs |
| `Reactor`, its `Outbox` and `Signal` | flows of direct-style code run by one future over one queue | the layer above, and code that needs a hot loop of its own |
| `SubmissionQueues`: `performQueue`, shards, a queue's signal | many ops performed with one wait | the reactor |
| ops begun and ended in two parts (`beginOn`, `endOn`) | each kind of op without its wait | the queue |

### Submission queues

`io.performQueue(ops, completed)` enters every op submitted, gives it its time limit and tries it at once. What is
done, or failed, is added to the completion queue with its tag and its failure; the rest are in flight and are settled
by whichever thread of the backend finds what they waited for. The call returns as soon as something has completed and
otherwise waits once. A queue may be of a shard, one for each worker of a pool, which pins where its ops are listened
for and its future resumed. A queue can be signalled from any thread, which has the call return with nothing
completed; that is what an awaited future and a handed-over connection use.

### Reactor

One future runs many flows. A flow is a continuation of that future, with an `Async` of its own:

- an op performed on `reactor.io` is tried at once, and a flow whose op has to wait is set aside; the reactor performs
  the queue and resumes each flow whose op is done;
- an `await` sets the flow aside, not the reactor; whoever completes the source signals the queue;
- `Signal` is where flows of one reactor wait for one another, on its thread;
- `Outbox` gathers what flows put for one handle and writes it with one op when no flow has more to do;
- `submit` hands the reactor a flow from another thread; `keepOpen` and `close` bound its life.

`reactor.io` has the type of the `Io` it was made of (`Io[io.ops.type]`), so handles and ops of the one are the
other's.

### Flow and Sink

A flow is the code that produces it, and `emit` is how that code hands an element on:

```scala
def lines(bytes: Flow[ByteBuffer]): Flow[String] = Flow:
  val line = new StringBuilder
  for chunk <- bytes do
    while chunk.hasRemaining() do
      val next = chunk.get().toChar
      if next == '\n' then { emit(line.result()); line.clear() } else line += next
```

```scala
trait Flow[+A] { def foreach(each: A => Unit)(using Async): Unit }
trait Sink[-A] { def send(a: A)(using Async): Unit; def flush()(using Async): Unit }
```

- A flow is pushed. Running it (`foreach`, and so `for ... do`, `fold`, `toList`, `to` a sink) runs its code on the
  caller's own stack, and an `emit` is a call of what consumes the element, returning when that has done with it.
  Nothing is read until a flow is run, and a consumer that is slow, or waits, holds its producer where it is: that
  is all the backpressure there is on the way in.
- `map`, `filter`, `flatMap`, `collect`, `take`, `takeWhile`, `drop`, `scan`, `zipWithIndex`, `tap`, `++` are each a
  flow that runs the one before it and emits. `for a <- flow if p(a) yield f(a)` is a flow. `through` takes a
  function from a flow to a flow, which is all a decoder is. None has a runtime of its own; all work under any
  `Async`.
- The functions given to `map`, `filter`, `fold` and the like are inlined, as is the body of `Flow { ... }`: an
  element costs one call for each stage it passes. Three stages and a fold over a thousand elements, nanoseconds an
  element, not inlined and inlined: Scala Native 44 and 13, Node.js 42 and 19, the JVM 36 and 1; the same with the
  library's iterators 37, 31 and 9.
- What `Flow { ... }` and the combinators make is one class (`Flow.Made`) holding a function: a place where a flow
  is made adds a function to the program, not a class.
- The stack is as deep as the pipeline has stages, whatever the number of elements: a flow runs in a loop, not a
  call inside a call. What does grow it is flows made of flows without end, a `map` in a loop, `++` folded over a
  list. Measured, a chain of `map`s runs out of stack past some 1,000 stages on Node.js, 3,000 on the JVM and 10,000
  on Scala Native, where it is not an exception but the end of the program.
- So a long pipeline is cut. A flow knows how many it is made of (`depth`), and every `Flow.Segment` of them, 128,
  what is above runs beside its consumer, not under it: in a flow of a reactor's as another flow of the reactor's,
  begun from its loop, and elsewhere as a future. Each element is handed across and the producer goes on only when
  the consumer has done with it, so that order, lent elements, failures and leaving early are as they were. No stack
  holds more than a segment. `Flow.concat` joins any number of flows with no cut at all.
- A hand-over costs what waking a future does: a thousand stages, seven cuts, cost an element 28 microseconds on
  Scala Native, 37 on Node.js and 16 on the JVM, some 2 to 5 for each cut. A pipeline under 128 stages pays nothing.
- Not right yet: on Node.js a pipeline of 2,000 stages (15 cuts) runs, and one of 20,000 (156 cuts) runs too but
  leaves Node unable to suspend afterwards (`SuspendError: trying to suspend JS frames` in whatever runs next). Each
  cut there is a stack of its own inside the last, and somewhere between the two is more than it has. The cause was
  not found. And a flow that runs itself from its own body is not cut at all.
- A flow left early (`take`, `headOption`) has its producer leave where it was, through its `finally` blocks, as a
  `break` does.
- `groupBy` deals elements by key into a flow for each key; `foreachPar` runs a flow for each element and joins
  them. In a flow of a reactor's they use its signals and forks; elsewhere channels and futures.
- A sink takes an element at once and waits only when too much is taken and unwritten. A connection's sink is its
  outbox, so a send is whole, in order, and written with the rest of the turn's.

It was first written the other way round, a flow pulled an element at a time (`pull(): A | End`). A decoder then had
to be a state machine that remembered where it was between pulls; as a generator it is a loop. The bench's server
measured the same either way (kqueue 3.50 against 3.40 with one worker and 1.13 against 1.13 with four, Node.js 5.47
against 5.47, all within their ranges).

### Server and Connection

```scala
Server.tcp(address).serve { connection =>
  frames(connection.bytes).groupBy(_.stream).foreachPar { (stream, requests) =>
    for request <- requests do connection.send(answer(stream, request))
  }
}
```

`serve` starts a reactor for each shard. A future of its own accepts and hands each connection to a shard's reactor in
turn (`submit`): that is the hand-off from one shard to another. The handler runs there as a flow. When it ends, what
it sent is written and the connection closed. `close`, from any thread, stops the accepting and has `serve` return
once the connections there are have been served.

## Decisions

1. **Pushed, and written as a generator.** A flow is direct-style code that emits; what consumes it is called, not
   scheduled. Backpressure is the call not having returned. The cost is that two flows cannot be run in step with
   one another (a zip) without a flow of their own for one of them.
2. **Lent elements.** A connection's bytes are its read buffer, good until what consumes it returns. What crosses to
   another flow (`groupBy`) must be owned, so a decoder that feeds one copies. In the bench that is an array for each
   frame.
3. **A flow does not require a reactor.** Every combinator works under any `Async`; `groupBy`, `foreachPar` and a
   shared sink are flows of the reactor's when there is one, and channels, futures and a locked buffer when there is
   not. The reactor is how those are made cheap, not a second world.
4. **Accepting is one future, not one for each shard.** It is the simplest hand-off and the one that works on every
   backend. A listener for each shard would save the hand-off where the platform can share a port.
5. **A handler's failure is its connection's.** The connection is closed, the others go on, and `serve` throws the
   first failure when it returns.

## What was measured

The session cache of the bench, speaking Redis's protocol, with 16 streams to a connection and 64 connections;
microseconds a request, medians. "No streams" is the same work with one flow a connection and no multiplexing.

| | each stream its own write | outbox, by hand on the reactor | `Server`/`Flow`/`Sink` | no streams |
|---|---:|---:|---:|---:|
| kqueue, 1 worker (7 runs) | 4.62 | 2.83 | 2.92 | 2.49 |
| kqueue, 4 workers (7 runs) | 1.74 | 0.99 | 0.91 | 0.74 |
| Node.js (4 runs) | 7.66 | 4.52 | 4.59 | 3.51 |
| NIO pool, 1 worker (3 runs) | 3.39 | 2.11 | 2.26 | 0.77 |
| NIO pool, 4 workers (3 runs) | 1.66 | 0.61 | 0.58 | 0.26 |

- The high-level layer costs nothing measurable over the reactor used by hand: within 3% on one kqueue worker and on
  Node.js, within the ranges on four workers, 7% on one NIO worker.
- Gathering a turn's writes is where the gain is: 35 to 65% less than each stream writing for itself.
- Multiplexing itself costs 15 to 30% over no streams on kqueue and Node.js, and two to three times on the JVM, where
  a flow is a virtual thread and each hand-over between flows is a park and an unpark.

### Against the same abstractions with no reactor

`Server.tcp(..., reactors = false)` is the same to its handler but made of futures alone: a future for each
connection, a future that writes what a connection's flows put in a buffer under a lock, `groupBy` a channel for each
key and `foreachPar` a future for each element. The handler is the same code, except that the cache its streams share
has to be locked. For this comparison each stream has keys of its own, since futures answer streams in no order;
that is a little more work for the client than in the table above, so these are not to be read against it.

| | on reactors | futures alone |
|---|---:|---:|
| kqueue, 1 worker (7 runs) | 3.44 (3.20-3.82) | 4.21 (3.88-4.45) |
| kqueue, 4 workers (7 runs) | 1.12 (1.11-1.42) | 1.62 (1.60-1.70) |
| Node.js (4 runs) | 5.84 (5.33-6.06) | 11.90 (11.01-12.29) |
| NIO pool, 1 worker (3 runs) | 2.36 (2.30-2.37) | 1.95 (1.93-2.01) |
| NIO pool, 4 workers (3 runs) | 0.60 (0.58-0.61) | 0.61 (0.60-0.61) |

- On Scala Native the reactor is worth 18% with one worker and 31% with four; on Node.js it halves the cost.
- On the JVM it is worth nothing, and with one worker costs 21%: a flow there is a virtual thread, as a future is,
  and the reactor adds a hand-over to each.
- With futures a connection's streams are answered in no order and their shared state needs a lock; on a reactor
  they are answered in the order their frames came, on one thread.

### A reactor with no queue for its ops

`Reactor.unqueued` performs a flow's op as `Io.perform` does, with the flow's `Async`: the op's own wait sets the flow
aside, and whoever settles it signals the reactor. Flows, signals, the outbox and the order of a connection's streams
are as before; what is gone is the queue's part, ops begun without a wait and listened for on the reactor's shard.
The reactor's future still waits on its queue's signal, which stands for whatever an `Io` without queues would give
it to wait on. Not on Node.js, whose ops do not wait through `Async`.

| | futures | reactor, no queue | reactor |
|---|---:|---:|---:|
| kqueue, 1 worker, 16 streams (7 runs) | 3.98 | 2.95 | 3.07 |
| kqueue, 4 workers, 16 streams | 1.86 (1.74-2.00) | 1.66 (1.42-1.72) | 1.08 (0.97-1.13) |
| kqueue, 1 worker, echo, a flow a connection | 7.46 | 7.46 | 7.24 |
| kqueue, 1 worker, realistic | 9.99 | 9.93 | 9.50 |
| NIO pool, 1 worker, 16 streams (3 runs) | 1.94 | 2.54 | 2.33 |
| NIO pool, 4 workers, 16 streams | 0.61 | 0.67 | 0.61 |

- With one worker the reactor gains all it gains without the queue: the gain is the flows on one thread and the
  writes gathered, not how the ops are waited for.
- With four workers most of it is lost: an op is listened for by whichever worker the reactor was on, not its
  shard's, so its completion crosses threads. What the queue gives a reactor on a pool is the shard.
- For a flow a connection the queue is worth 3 to 4% on one worker, and nothing can be told apart on four.

### A reactor over the least an `Io` can give

`Reactor.pinned` uses no submission queue. It asks one thing of the `Io`: `performPinned(shard, op, timeout)`, the op
performed as ever but begun on a named shard's thread, so listened for and settled there. The reactor's future waits
on a source of its own, which whoever settles a flow's op, completes what a flow awaits, or hands it a flow, fires.

| 16 streams a connection | futures | reactor, no queue | pinned, no queue | reactor with queue |
|---|---:|---:|---:|---:|
| kqueue, 1 worker (7 runs) | 4.03 | 3.07 | 3.06 (2.95-3.23) | 3.09 (2.96-3.18) |
| kqueue, 4 workers (7 runs) | 1.56 | 1.34 | 0.97 (0.94-1.02) | 0.96 (0.90-1.00) |
| NIO pool, 1 worker (3 runs) | 2.12 | | 2.54 | 2.41 |
| NIO pool, 4 workers (3 runs) | 0.68 | | 0.63 | 0.59 |

For a flow a connection (kqueue, the realistic scenario): 9.59 pinned against 9.54 with the queue on one worker, and
4.34 against 4.62 on four, inside each other's ranges.

- Pinned, the reactor without a queue is as fast as with one, on one worker and on four. For a reactor the queue
  itself is not needed: a shard to pin an op to is.
- So what a backend owes a reactor is small: how many shards it has, an op performed on a named one, and, where its
  ops do not wait through `Async` (Node.js), a way for the reactor's future to wait and be woken at a turn's end.
- `performQueue` stays what it was measured as before: worth 7 to 13% on one worker to a server written against it
  by hand, as a state machine with no flows at all.

Earlier results that this rests on: for one flow a connection, a reactor is level with a future a connection
(1 to 6% ahead on one worker, nothing on four); a server written over `performQueue` by hand is 7 to 13% ahead on one
worker.

## Capabilities, and how a server picks

Status of this section: on the branch `experiment/capabilities`.

What was one thing is three, which a backend has or has not independently:

| | What it promises | Who has it |
|---|---|---|
| `Shards` | threads of the backend's own, a shard each; an op performed on a named one is settled there. `io.shards`, `io.onShard(n)`, a view of the same ops and handles with every op pinned | the pools: kqueue, epoll, NIO |
| `SubmissionQueues` | many ops begun and waited for with one call (`performQueue`) | the pools, and Node.js |
| flows that are continuations | a reactor's flow is resumed where the reactor is, not by way of a thread of its own. Not the `Io`'s to offer and not public: the platform's suspension, which the backend reports | Scala Native, Node.js; not the JVM |

`Reactor` is internal (`private[io]`) and needs none of them: it uses shards if it is pinned and a queue if it is
queued. A program sees `Server`, `Connection`, `Flow`, `Sink`, and `Server.Strategy` if it wants to choose.

`Server.tcp(address, sizes, strategy = Auto)`, its `Io` given, looks once, as it is made, at what the backend's ops are:

| Flows are continuations | `Shards` | `SubmissionQueues` | `Auto` picks |
|---|---|---|---|
| no | yes | any | `PinnedFutures` |
| no | no | any | `Futures` |
| yes | yes | any | `PinnedReactors` |
| yes | no | yes | `QueuedReactors` |
| yes | no | no | `Reactor`, one, unpinned |

It picks `PinnedReactors` on kqueue, `QueuedReactors` on Node.js, `PinnedFutures` on the NIO backends and `Futures`
on the reference backend. A reactor with no queue is only for a backend whose ops wait through `Async`, which the
backend says of itself (`opsAwaitThroughAsync`): the reference backend's park their thread, and Node.js's await a
promise in place, so neither is given one. Asking for a strategy
the backend cannot give is refused.

A handler is the same under every strategy, so it must not count on what only some give. Under `Futures` a
connection's flows are on any thread and in no order among themselves: what they share is locked with a
`Semaphore`, always. The bench's handler now does, under every strategy.

16 streams a connection, microseconds a request, medians; the strategy forced, and `Auto`:

| | `Futures` | `Reactor` | `PinnedReactors` | `QueuedReactors` | `Auto` |
|---|---:|---:|---:|---:|---:|
| kqueue, 1 worker (7 runs) | 4.38 | 3.49 | 3.48 | 3.47 | 3.72 |
| kqueue, 4 workers (7 runs) | 1.55 | 1.72 | 0.90 | 0.91 | 1.13 |
| Node.js (4 runs) | 11.24 | | | 4.80 | 5.41 |
| NIO pool, 1 worker (3 runs) | 1.99 | 2.48 | 2.51 | 2.39 | 1.97 |
| NIO pool, 4 workers (3 runs) | 0.53 | 1.91 | 0.66 | 0.58 | 0.54 |

- `Auto` is the scenario each run does first, and is slower than the same strategy forced later in the run (1.13
  against 0.90 on kqueue, 5.41 against 4.80 on Node.js): the order, not the choice. It was not measured in another
  order.
- The choice is the fastest column, or level with it, on every row.
- One reactor for a pool of four is worse than futures (1.72 against 1.55 on kqueue, 1.91 against 0.53 on NIO): a
  reactor is one thread's worth. `Reactor` is for a backend with one thread.
- `Strategy.Reactor` and `PinnedReactors` are not for Node.js, whose ops do not wait through `Async`.

### `onShard` in use

Both pinned strategies are made of `io.onShard(n)`. A pinned reactor is a reactor with no queue over the view on its
shard; `PinnedFutures` gives each connection, in turn, the view on a shard, so that a connection's future performs
all its ops there. 16 streams a connection, medians:

| | `Futures` | `PinnedFutures` | `PinnedReactors` | `QueuedReactors` |
|---|---:|---:|---:|---:|
| kqueue, 1 worker (7 runs) | 4.55 | 4.28 | 3.36 | 3.36 |
| kqueue, 4 workers (7 runs) | 1.75 (1.70-1.79) | 1.28 (1.23-1.39) | 1.06 | 0.99 |
| NIO pool, 1 worker (4 runs) | 2.08 | 2.04 | 2.51 | 2.43 |
| NIO pool, 4 workers (4 runs) | 0.58 (0.57-0.65) | 0.51 (0.49-0.53) | 0.60 | 0.58 |

- Pinning helps futures too: 27% on four kqueue workers, 12% on four NIO workers, nothing with one worker, where
  there is nothing to cross.
- On the JVM pinned futures are the fastest of all with four workers, so they are what `Auto` picks there.
- On Scala Native reactors are still ahead of pinned futures, by 17 to 23%.

Not done: a queue of a shard still only asks for `SubmissionQueues`;
both capabilities are declared by every NIO backend, the one that performs on its caller among them, where there is
one shard. One run of the Node.js server did not end, in some fifty; it did not happen again in forty more. Its likely cause was
found since: `java.util.ArrayDeque` on Scala.js loses what it holds when it grows after something was taken from it.
The reactor and `groupBy` use a queue of their own (`Ring`), as the readiness layer's waiter queue does
(`RingDeque`); the build has since moved to Scala.js 1.23.0, where `ArrayDeque` is right.

## Scheduler hooks, and a reactor made of futures

Two questions, tried together. If a reactor gains by affinity and by one write a turn, can the pool's scheduler give
those to plain futures? And how does a reactor whose flows are futures, waiting with the `Async` every future has,
compare with one whose flows are continuations of its own?

Two hooks on `Io`, each with a meaning on any backend and an implementation worth something on the pools:

- `afterTurn(shard)(run)`: `run` runs when the shard's worker has run what was ready this turn, before it looks for
  more. The worker's loop has a place for it, after its timers, its ready continuations and its inbox.
- `moveTo(shard)`: the calling future goes on on the shard's worker, so that what it begins is there too.

`Server.Strategy.ShardFutures` is `PinnedFutures` with both: a connection's future begins on its shard, and what its
flows send in a turn is written when the turn's work is done, the writer woken by the hook and not by each send. Its
streams are futures, dealt to by channels, its shared state locked: a reactor's two gains with futures for flows,
which is as near as a reactor made of futures comes, since a future may still be taken by another worker and so
cannot share a reactor's unguarded signals.

16 streams a connection, microseconds a request, medians (kqueue 7 runs, NIO 3):

| | `Futures` | `PinnedFutures` | `ShardFutures` | `PinnedReactors` | `QueuedReactors` |
|---|---:|---:|---:|---:|---:|
| kqueue, 1 worker | 4.09 | 3.66 (3.57-3.84) | 3.55 (3.49-3.79) | 2.94 | 2.98 |
| kqueue, 4 workers | 1.60 | 1.16 (1.15-1.18) | 1.12 (1.09-1.16) | 0.88 | 0.86 |
| NIO pool, 1 worker | 2.02 | 1.82 | 1.85 | 2.34 | 2.29 |
| NIO pool, 4 workers | 0.58 | 0.51 | 0.50 | 0.59 | 0.57 |

- The hooks are worth little over pinning alone: 3% on four kqueue workers, inside the ranges with one, nothing on
  the JVM. Pinned, a connection's futures were on their shard already, and the writer future, woken by the first send
  and run after what was ready, was writing once a turn already.
- With both, futures are still 21 to 27% behind continuation flows on Scala Native. What is left is not where a
  future runs or when it writes but what a wait costs it: a listener with a lock, a link to its group made and
  unmade, a channel's lock for each element dealt to a stream, where a reactor's flow parks on a signal that is two
  fields and is resumed from a queue no other thread sees.
- On the JVM futures are ahead, as before: a flow there is a thread as a future is.

So the reactor is not made unnecessary by hooks of this kind. To have its speed from the scheduler itself, the
scheduler would need what makes a reactor's wait cheap: futures that are never another worker's, and waits between
them that need no lock for that reason. That is the reactor's inside, moved into the worker; it was not tried.

### A cheaper wait for futures

What was left, then, was what a wait costs a future. `Async.park(register)`, in the core: the caller is suspended
until the `Runnable` that `register` is handed has been run, once, by any thread. For a future it is the bare
suspension and its scheduler's resume: no listener, no lock, no link to its group. It is not ended by a cancel, as a
reactor's `Signal.park` is not: whoever parks so answers for being woken. Any other `Async` has it by way of `await`.

Two waits use it, both between futures that serve one connection: a stream's future waiting for its next frame
(`groupBy` with no reactor is now a queue that one future fills and another empties, each parked when it has nothing
to do there, in place of a channel), and the future that writes what a connection sends, waiting to be told there is
something.

It is ended by a cancel, as an `await` is: a future keeps the one place it is parked in, and its cancel takes it
from there and resumes it with `CancellationException`, as the runnable takes it to resume it, whichever is first, by
one swap. Run alternately with a build in which a cancel did not end it, it measured the same (kqueue 3.48 against
3.44 and 1.12 against 1.13). Under a group made inside the future, which something other than the future's cancel
may cancel, a time limit say, a park is an ordinary await in that group.

How a future parks is its platform's (`AsyncSupport.parkHere`). On the JVM and Scala Native it is a suspension that
the scheduler resumes. On Scala.js it waits where it is, on a promise of its own: an `await` there begins another
stack to resume the first, and that was most of what a future's wait cost on Node.js.

16 streams a connection, microseconds a request, medians. Each strategy in a process of its own, run in turn with
the others, since the order of scenarios in one process turned out to move a figure by a tenth:

| | `Futures`, channels | `Futures` | `PinnedFutures` | `ShardFutures` | reactors |
|---|---:|---:|---:|---:|---:|
| kqueue, 1 worker (9 runs) | 4.01 | 3.53 | 3.54 | 3.44 (3.37-3.67) | 3.15 (3.05-3.24) |
| kqueue, 4 workers (9 runs) | 1.49 | 1.32 | 1.11 | 1.10 (1.07-1.15) | 1.02 (1.00-1.05) |
| Node.js (5 runs) | 9.76 | 5.10 (5.08-5.18) | | | 4.89 (4.88-4.92) |
| NIO pool, 1 worker (3 runs) | 2.02 | 1.49 | 1.59 | 1.53 | 2.22 |
| NIO pool, 4 workers (3 runs) | 0.58 | 0.44 | 0.42 | 0.41 | 0.58 |

(The NIO rows are of the earlier kind of run, scenarios together in one process.)

- Futures are within 8 to 9% of reactors on kqueue, within 4% on Node.js, where they cost twice as much before, and
  ahead by 30% on the JVM.
- The park is worth 9 to 12% to futures on kqueue and 48% on Node.js. The hooks, over pinning, are worth 1 to 3%.
- An earlier reading here, that futures had reached reactors on four kqueue workers (0.99 against 0.97), was of
  scenarios run together, and is withdrawn for the row above.

So nearly all of a reactor's speed is had from futures on every backend, with two things: a connection's futures
pinned to a shard where there are shards, and futures that hand work to one another waiting without a listener.

With more connections, each of 16 streams, on four kqueue workers and on Node.js (5 runs, fewer at 1,024, where
some did not finish in their time):

| connections | kqueue: `ShardFutures` | kqueue: pinned reactors | Node.js: `Futures` | Node.js: queued reactor |
|---:|---:|---:|---:|---:|
| 16 | 1.02 | 0.96 | 4.97 | 4.75 |
| 64 | 1.10 | 1.02 | 5.10 | 4.89 |
| 256 | 1.67 (1.64-1.71) | 1.42 (1.38-1.43) | 6.76 | 6.40 |
| 1,024 | 2.20 | 2.16 | 11.29 | 8.76 |

Both cost more a request as connections grow, and the two do not keep one distance: on kqueue futures are 18% behind
at 256 connections and level at 1,024; on Node.js they are 6% behind at 256 and 29% at 1,024, which is 16,000
futures. Why was not looked for. So futures are near reactors, not a replacement measured at every size.

### What the rest of the gap is

Looked for on kqueue, where futures are some 10% behind reactors. It is no one thing:

- **Instructions.** 512,000 requests on one worker retire 24.6 thousand million instructions served by futures and
  23.6 by reactors: 2,000 more a request, of 48,000, which is 4%.
- **Memory.** The same run's peak is 782 MB against 560, and with 256 connections on four workers 1,503 against
  1,202. Time differs by more than instructions do (13% against 4% there), which is what touching more memory looks
  like.
- **Where a wait is made.** A wait on Scala Native copies the stack between where a future or a flow began and
  where it parks, out to the heap and back. A stream's future parked 27 frames from its beginning, a reactor's flow
  16: a future begins inside a `Try`, a completion group's scope and the lambdas of both, and its stream was a
  `Flow { ... }` with frames of its own. Written out and made a class, six of the eleven went, and that was worth 2%
  (3.62 to 3.56 with one worker, the echo 7.41 to 7.23), with nothing to be seen on four.
- **Not it:** the queue between a stream and its dealer, made a ring with nothing allocated for an element, and the
  worker's list of what to run at a turn's end, no longer copied each turn. Both are kept, and neither moved a figure.
  A profile of the whole process, clients and all, shows no symbol more than half a percent apart between the two.

So what is left is spread over a future being a larger thing than a flow: more frames under each wait, a few more
objects for each park and resume, structures that are safe for two threads where a flow's are for one. On Node.js
with 1,024 connections a second look found futures 12% behind (9.52 against 8.52, instructions 7% more, memory the
same), not the 29% of the first, which had runs that did not finish.

Tried and not kept: an op's own wait by a park. It passed the contracts for cancelling and for time limits, a time
limit being the op's own on its worker's heap and a cancel taking the performer from where it is parked, and it
measured inside the ranges of the wait it replaced (kqueue: echo 7.10 against 7.25, realistic 9.48 against 9.67 with
one worker; 3.88 against 3.86 with four). An op's wait is a system call's worth; its listener is not what it costs.

## Pipelines of stages, and a machine that steps them

A handler that is run needs somewhere to stand while it waits: a future or a flow for the connection, and for each of
its streams. A pipeline whose stages never wait needs nowhere. Its stages are functions called with an element and
returning, so a connection is its buffers and a read in flight, and a stream is an object in a map.

Stages are the combinators that could always have been functions: `map`, `filter`, `collect`, `mapConcat`, `tap`,
and two for this. `process(handler)` is a stage with something to keep between elements, a decoder: `handler` is
called once with what to call for each element it makes, and gives what to call with each that comes.
`groupThrough(key)(sub)` deals elements by key, each key's through a pipeline of its own, `sub` called once for a
key. A flow made by stages is the same flow as before when it is run; it can also be stepped (`Flow.Step`).

```scala
server.serveFlow { connection =>
  val cache = new Cache
  connection.bytes.process[Frame](decoder).groupThrough(_.stream) { (stream, requests) =>
    requests.map(request => answer(cache, stream, request))
  }
}
```

`serveFlow` takes what makes a connection's pipeline, the flow of what is to be sent. Where the backend has
submission queues a machine for each shard serves it: a read in flight for each connection, each chunk pushed through
its stages as its read completes, what comes out written, the read not armed again while more than a write's worth
waits. Elsewhere, or asked, the same pipeline is run by `serve`, a future or a flow for each connection and none for
a stream. Either way a connection's stages are called one chunk after another on one thread, so what its streams
share needs no lock, which a handler that is run cannot count on.

16 streams a connection, 64 connections, microseconds a request, medians, each in a process of its own:

| | handler, futures | handler, reactors | stages, run by `serve` | stages, the machine | no streams |
|---|---:|---:|---:|---:|---:|
| kqueue, 1 worker (7 runs) | 3.79 | 3.36 | 2.83 | 2.75 (2.64-2.94) | 2.58 |
| kqueue, 4 workers (7 runs) | 1.18 | 1.10 | 0.87 | 0.85 (0.84-0.87) | 0.72 |
| Node.js (5 runs) | 5.61 | 5.43 | 4.44 | 4.31 (4.21-4.45) | 3.64 |
| NIO pool, 1 worker | 1.77 | 2.77 | 0.99 | 0.86 (0.79-0.88) | 0.83 |
| NIO pool, 4 workers | 0.49 | 0.69 | 0.41 | 0.44 (0.43-0.44) | 0.38 |

- Stages are 20 to 28% faster than a handler on reactors on kqueue and Node.js, and on the JVM 16 to 50% faster than
  a handler on futures, the best it had. They are within 7 to 20% of the same work with no streams at all.
- Nearly all of that is the stages, not the machine. The machine is 2 to 3% ahead of `serve` running the same
  pipeline on kqueue and Node.js, 13% ahead on one NIO worker, and behind on four.
- It is one design on every backend, the JVM among them, which reactors never were.

### The stage that waits

`mapAsync(atOnce)(f)`: `f` of each element, which may perform ops and await, up to `atOnce` of them under way at a
time, what each made pushed on as it is done. With more elements than that the source is held, and nothing more is
read. It is how a pipeline asks another service or reads a file and stays a pipeline.

Stepped by the machine, each `f` is a future beside the pipeline. What it made comes back through the machine, which
is signalled, and is pushed on where every other stage is called, on the pipeline's thread: so the stages after it
are still called one at a time, and only `f` itself runs beside its like. A stage that steps a pipeline knows what
drives it by three things (`Flow.Drive`): where work is begun, how what it made comes back, and how the source is
held. A pipeline that has ended with work under way is closed when the last of it is done. Run, not stepped, it is
`mapPar`, or inside a key's pipeline done in place, one after another.

Every request answered by work in a future of its own, which is what a wait costs with nothing to wait for;
microseconds a request, medians, each in a process of its own:

| | a handler that awaits a future | the machine, `mapAsync` | the machine, no wait |
|---|---:|---:|---:|
| kqueue, 1 worker (5 runs) | 8.09 | 3.69 | 2.59 |
| kqueue, 4 workers (5 runs) | 2.65 | 1.44 | 0.84 |
| Node.js (4 runs) | 6.80 | 5.53 | 4.08 |
| NIO pool, 1 worker | 3.31 | 1.90 | 0.78 |
| NIO pool, 4 workers | 0.90 | 0.53 | 0.34 |

- A wait in a stage costs a request 0.6 to 1.5 microseconds over none: a future begun, and its result handed back.
- It is 19 to 54% less than a handler that spawns a future and awaits it for the same work.

Not right yet: run by a reactor's flow on Node.js, an op that `f` performs on the program's own `Io` is waited for by
the whole reactor, so its work is one at a time whatever was asked. The machine is what serves a pipeline there
unless told otherwise. And a send larger than a connection's write buffer is refused by a handler's
sink and grown for by the machine; the two should agree.

## Locks

A lock that belongs to a thread (`synchronized`, `ReentrantLock`) is wrong for state that flows or futures share
across a wait. Flows of one reactor are one thread, so such a lock held by one is entered by the next; and a future
may go on on another thread than it began on. What they share is locked with a lock that is waited for as a future
waits and is no thread's: `gears.async.Semaphore(1)`. Its `acquire` awaits, so in a flow it sets that flow aside and
no other, and it is not re-entered by whoever holds it. The bench's futures-only server locks its cache so; against
`synchronized` it measured the same (kqueue 4.36 and 1.68 against 4.21 and 1.62, within the ranges).

A thread's lock around code that does not wait, as the futures-only outbox has around its buffer, is not wrong: it is
never held across a wait. Flows of one reactor that share state and do not wait while they use it need no lock at
all.

## Not done, and known to be wrong

- Tests: `FlowContract`, `ShardsContract` and `SubmissionQueueContract` in the testkit, bound to the reference
  backend, both NIO backends, the pools on the JVM and Scala Native, and Node.js. The server's checks are made under
  every strategy, and a strategy a backend has not must be refused.
- Fairness: a flow whose ops never wait holds its reactor.
- File ops, and whatever else is done off the workers, are performed whole and hold the reactor meanwhile.
- A cancelled reactor drops its flows where they are; their `finally` blocks do not run.
- A sink's failure reaches a flow only at its next `send` or `flush`.
- `groupBy` never forgets a key, and its keys are boxed.
- `Server` is for TCP only, deals connections in turn with no regard to load, and makes three buffers a connection.
- On the JVM a flow should be a continuation, not a virtual thread; that needs the internal API the pool backend
  already opens.

## What would come next

1. Tests.
2. Ops off the workers begun and ended in two parts, so that a file's read is a flow's op like any other.
3. Sources and sinks for files, datagrams and timers; `merge`; `Flow.channel` between a reactor and a future.
4. A fairness budget in the reactor's loop.
5. An io_uring backend under the same queue, where a turn's submissions are one system call.
