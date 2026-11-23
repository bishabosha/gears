package gears.async.io

import gears.async.Async

import scala.concurrent.duration.FiniteDuration

/** EXPERIMENT.
  *
  * Many ops performed by one future, with one wait for all of them: the future puts ops on a submission queue, and
  * `Io.performQueue` starts every one, returns with those that are done on a completion queue, and otherwise waits
  * until one is. An op that has to wait stays in flight when `performQueue` returns, and is done, by whichever thread
  * of the backend finds what it waited for, while the future goes on: its completion is taken by the next
  * `performQueue`. So a future that serves many connections has a read in flight on each, writes its answers as ops
  * that are most often done as they are started, and waits once for however many of them a turn of the backend brings.
  *
  * It is every op of the backend's, not a kind of its own: a read, a write, an accept, a datagram's receive, a timer, a
  * close. A timer among the ops in flight is how a wait is given a limit.
  */
trait SubmissionQueues extends Interface {
  type Op <: PipelineOp
}

/** An op that a submission queue takes: every op of a backend that has `SubmissionQueues`. */
trait PipelineOp

/** The ops a future has asked for and not yet been handed back: those it has submitted since the last `performQueue`,
  * which that starts, and those in flight, started and waiting. One future's, at a time.
  *
  * A queue may be of a shard of its backend, one of `Io.queueShards`, numbered from 0. A backend with several threads
  * has a shard for each: a shard's queue has its ops listened for by that thread and its future resumed on it, so that
  * a program which gives each shard a future and deals what it serves among them, connection by connection, is served
  * on every thread the backend has, with nothing crossing from one to another, and need not know how many there are.
  */
final class SubmissionQueue[O](val shard: Int) {

  /** A queue of no shard in particular: its ops are begun wherever its future is running as it performs them. */
  def this() = this(-1)

  private var ops = new Array[AnyRef | Null](16)
  private var limits = new Array[Long](16)
  private var tags = new Array[AnyRef | Null](16)
  private var count = 0
  private var flying = 0

  /** Set to have a `performQueue` return though no op has completed: by any thread, for what its future waits for
    * that is not an op. Kept until a `performQueue` has seen it.
    */
  @volatile private[io] var signalled: Boolean = false

  /** Whether a `performQueue` with nothing in flight waits all the same, for a `signal`. The future's own to set. */
  private[io] var lingers: Boolean = false

  /** The backend's, to wake the queue's waiting future. */
  @volatile private[io] var waker: Runnable | Null = null

  private[io] def signal(): Unit = {
    signalled = true
    val wake = waker
    if wake != null then wake.run()
  }

  /** What the backend keeps for the queue: its ops in flight and their completions. */
  private[io] var attached: AnyRef | Null = null

  /** Submits `op`, to be started by the next `performQueue`, with no limit on its time. An op is submitted as it is
    * performed: once, unless it is `Repeatable`, and then again only once it has come back completed.
    */
  def submit(op: O): Unit = add(op, null, Backend.NoTimeout)

  /** Submits `op` with `tag`, whatever the future keeps for it, which its completion is handed back with: so that what
    * an op was for is found without looking the op up.
    */
  def submit(op: O, tag: AnyRef): Unit = add(op, tag, Backend.NoTimeout)

  /** Submits `op` with a time limit, counted from when it is started: it completes with a `TimeoutException` if it is
    * not done by then, as `Io.perform`'s does.
    */
  def submit(op: O, tag: AnyRef | Null, timeout: FiniteDuration): Unit = {
    require(timeout.length >= 0, s"A time limit is not negative: $timeout")
    val nanos = timeout.toNanos
    add(op, tag, nanos / 1_000_000L + (if nanos % 1_000_000L == 0 then 0 else 1))
  }

  private[io] def submitWithin(op: O, tag: AnyRef | Null, millis: Long): Unit = add(op, tag, millis)

  private def add(op: O, tag: AnyRef | Null, millis: Long): Unit = {
    if count == ops.length then {
      ops = java.util.Arrays.copyOf(ops, count * 2)
      limits = java.util.Arrays.copyOf(limits, count * 2)
      tags = java.util.Arrays.copyOf(tags, count * 2)
    }
    ops(count) = op.asInstanceOf[AnyRef]
    tags(count) = tag
    limits(count) = millis
    count += 1
  }

  /** How many ops are submitted and not yet started. */
  def submitted: Int = count

  /** How many ops are in flight: started, and neither done nor handed back. */
  def inFlight: Int = flying

  /** Whether there is nothing to start and nothing in flight: a `performQueue` would have nothing to wait for. */
  def isEmpty: Boolean = count == 0 && flying == 0

  private[io] def op(index: Int): O = ops(index).asInstanceOf[O]
  private[io] def limit(index: Int): Long = limits(index)
  private[io] def tag(index: Int): AnyRef | Null = tags(index)
  private[io] def started(): Unit = {
    java.util.Arrays.fill(ops, 0, count, null)
    java.util.Arrays.fill(tags, 0, count, null)
    count = 0
  }
  private[io] def flying(by: Int): Unit = flying += by

  /** For a future that was cancelled as it performed the queue and has things to put right: what was submitted and
    * never started is taken back, each handed over on `completed` as failed with a `CancellationException`. It is
    * never started. With what `performQueue` does as it is cancelled, giving up every op in flight and handing those
    * over too, every op the queue was given is then accounted for, and none is in use.
    */
  def withdraw(completed: CompletionQueue[O]): Unit = {
    var i = 0
    while i < count do {
      completed.add(ops(i).asInstanceOf[O], tags(i), new gears.async.GearsCancellationException("Never started"))
      i += 1
    }
    started()
  }
}

/** The ops that are done, each with how it ended, in the order they were found to be: filled by `performQueue`, which
  * adds to it, and emptied by the future that reads it (`clear`).
  */
final class CompletionQueue[O] {
  private var ops = new Array[AnyRef | Null](16)
  private var failures = new Array[Throwable | Null](16)
  private var tags = new Array[AnyRef | Null](16)
  private var filled = 0

  def count: Int = filled

  /** The op at `index`, from 0: no longer in use anywhere, as one `Io.perform` has returned from. */
  def op(index: Int): O = ops(index).asInstanceOf[O]

  /** What the op failed with, as `Io.perform` would have thrown it, a `CancellationException` if it was cancelled, a
    * `TimeoutException` if its time ran out; null if it completed.
    */
  def failure(index: Int): Throwable | Null = failures(index)

  /** What the op was submitted with; null if nothing. */
  def tag(index: Int): AnyRef | Null = tags(index)

  def clear(): Unit = {
    java.util.Arrays.fill(ops, 0, filled, null)
    java.util.Arrays.fill(failures.asInstanceOf[Array[AnyRef | Null]], 0, filled, null)
    java.util.Arrays.fill(tags, 0, filled, null)
    filled = 0
  }

  private[io] def add(op: O, tag: AnyRef | Null, failure: Throwable | Null): Unit = {
    if filled == ops.length then {
      ops = java.util.Arrays.copyOf(ops, filled * 2)
      failures = java.util.Arrays.copyOf(failures, filled * 2)
      tags = java.util.Arrays.copyOf(tags, filled * 2)
    }
    ops(filled) = op.asInstanceOf[AnyRef]
    tags(filled) = tag
    failures(filled) = failure
    filled += 1
  }
}

object SubmissionQueues {

  extension [I <: SubmissionQueues](io: Io[I]) {

    /** Starts every op submitted to `ops`, each entered as `perform` enters it and given its time limit, and tried at
      * once without waiting. Those that are done, by that or because they failed, are added to `completed` with how
      * they ended; the rest are in flight. Completions of ops in flight from before are added too.
      *
      * It returns as soon as `completed` has something, and at once if there is nothing in flight to wait for;
      * otherwise it suspends the caller until an op in flight is done, and then returns with every one that is. What
      * is still in flight goes on, and is taken by a later `performQueue` on the same queue.
      *
      * No op's failure is thrown: each is its completion's. If the caller is cancelled as it waits, every op in flight
      * is cancelled, as `perform`'s one op is, and `CancellationException` is thrown.
      */
    def performQueue(ops: SubmissionQueue[io.Op], completed: CompletionQueue[io.Op])(using Async): Unit =
      io.performSubmitted(ops, completed)

    /** How many shards the backend's queues have (see `SubmissionQueue`): one for each of its threads. */
    def queueShards: Int = io.submissionShards
  }
}
