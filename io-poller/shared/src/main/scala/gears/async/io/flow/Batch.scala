package gears.async.io
package flow

import gears.async.Async

import scala.concurrent.duration.FiniteDuration

/** A queue of ops performed together, by one future, with one wait: ops are added (`add`), which begins nothing, and
  * `perform` then begins each. An op that is done as it is begun, a read whose bytes are there, leaves the queue at
  * once; one that has to wait stays in it, and is found done by a later `perform`, once what it waited for has come.
  * `perform` returns how many ops are done, which it then tells of (`op`, `note`, `failure`), and suspends the caller
  * only if none is and some wait: once, until the first of them is done.
  *
  * So a future that serves many handles adds an op for each, performs, and deals with what is done, adding what comes
  * next: it is suspended once a round, however many ops the round began, and not at all while there is always something
  * done.
  *
  * A batch is one future's, and is closed by it (`close`), which ends the ops still waiting. Its ops are otherwise as
  * `Io.perform` has them: an op is in one batch at a time, and a repeatable op may be added again once it is done.
  */
final class Batch[O] private (queue: shareable.OpQueue[?]) {

  /** Adds `op`, to be begun by the next `perform`; `note` comes back with it when it is done. */
  def add(op: O, note: AnyRef | Null = null): Unit = queue.add(op.asInstanceOf[AnyRef], note, Backend.NoTimeout)

  /** As `add`, with a time limit from when the op is begun, as `Io.perform` has it. */
  def add(op: O, note: AnyRef | Null, timeout: FiniteDuration): Unit = {
    require(timeout.length >= 0, s"A time limit is not negative: $timeout")
    val nanos = timeout.toNanos
    queue.add(op.asInstanceOf[AnyRef], note, nanos / 1_000_000L + (if nanos % 1_000_000L == 0 then 0 else 1))
  }

  /** How many ops are in the batch, added or waiting. */
  def size: Int = queue.size

  /** Begins the ops added and returns how many of the batch's ops are done, at least one unless the batch is empty: the
    * caller is suspended until one is, if none is yet. Throws only the caller's cancellation, with the batch as it was:
    * an op's failure is told with it (`failure`).
    */
  def perform()(using Async): Int = queue.perform()

  /** The `i`th op the last `perform` found done. */
  def op(i: Int): O = queue.doneOp(i).asInstanceOf[O]

  /** Whether the `i`th op the last `perform` found done is `op` itself. */
  def isOp(i: Int, op: O): Boolean = queue.doneOp(i) eq op.asInstanceOf[AnyRef]

  /** The note it was added with. */
  def note(i: Int): AnyRef | Null = queue.doneNote(i)

  /** What it failed with, or null. */
  def failure(i: Int): Throwable | Null = queue.doneFailure(i)

  /** Ends the batch's ops: those not begun are dropped, those waiting are stopped as a cancelled `perform` stops its
    * op.
    */
  def close()(using Async): Unit = queue.close()
}

object Batch {

  /** A batch of `io`'s ops, for the calling future. */
  def apply(io: Io[Interface]): Batch[io.Op] = io match
    case pool: shareable.WorkpoolScheduler[?] => new Batch[io.Op](pool.newQueue())
    case _ => throw new UnsupportedOperationException(s"Batches are not performed by this backend yet: ${io.backend}")
}
