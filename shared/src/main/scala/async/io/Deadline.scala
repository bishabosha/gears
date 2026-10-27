package gears.async.io

/** A timer whose deadline may be moved while it is pending (see `Ops.deadlineTimer`). */
trait Deadline {

  /** The deadline, on `System.nanoTime`'s clock. */
  def deadline: Long

  /** Moves the deadline to `nanoTime`, from any thread. A submission pending completes at the new deadline instead, at
    * once if it has passed, however often the deadline was moved; a later one starts from it. Moving a deadline later
    * costs a store, the reactor finding the new one when the old comes; moving it earlier wakes the reactor, unless on
    * its own thread.
    */
  def moveTo(nanoTime: Long): Unit
}
