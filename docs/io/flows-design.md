# Flows, sinks and servers over submission queues

Status: experiment, on the branch `experiment/reactor`. Nothing here is on the main branch, and nothing has tests of
its own yet. Measurements are of 2026-11-19, on one macOS machine, Scala Native
`0.5.13-20261114-143517b-SNAPSHOT`.

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
| `Server`, `Connection`, `Flow`, `Sink`, `Pipe` | streams and their combinators, a listener served on every shard | programs |
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

### Flow, Sink, Pipe

```scala
trait Flow[+A] { def pull()(using Async): A | Flow.End.type }
trait Sink[-A] { def send(a: A)(using Async): Unit; def flush()(using Async): Unit }
trait Pipe[-A, +B] { def apply(in: Flow[A]): Flow[B] }
```

- A flow is pulled. Nothing is read until something pulls, and that is all the backpressure there is on the way in.
- `map`, `filter`, `foreach`, `fold` are inline loops over `pull`; `take`, `through`, `to` are plain methods. None has
  a runtime of its own, and all work under any `Async`.
- `groupBy` deals elements by key into a flow for each key; `foreachPar` runs a flow for each element and joins
  them. In a flow of a reactor's they use its signals and forks; elsewhere channels and futures.
- A sink takes an element at once and waits only when too much is taken and unwritten. A connection's sink is its
  outbox, so a send is whole, in order, and written with the rest of the turn's.

### Server and Connection

```scala
val server = Server.tcp(io, address)
server.serve { connection =>
  connection.bytes.through(frames).groupBy(_.stream).foreachPar { (stream, requests) =>
    requests.foreach(request => connection.sink.send(answer(stream, request)))
  }
}
```

`serve` starts a reactor for each shard. A future of its own accepts and hands each connection to a shard's reactor in
turn (`submit`): that is the hand-off from one shard to another. The handler runs there as a flow. When it ends, what
it sent is written and the connection closed. `close`, from any thread, stops the accepting and has `serve` return
once the connections there are have been served.

## Decisions

1. **Pull, not push.** It keeps direct style and makes backpressure the absence of a pull. The cost is that fan-out
   needs a flow for each branch.
2. **Lent elements.** A connection's bytes are its read buffer, good until the next pull. What crosses to another
   flow (`groupBy`) must be owned, so a decoder that feeds one copies. In the bench that is an array for each frame.
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

`Server.tcp(io, address, sizes, strategy = Auto)` looks once, as it is made, at what the backend's ops are:

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
