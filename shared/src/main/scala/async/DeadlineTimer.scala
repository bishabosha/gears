package gears.async

import gears.async.io.Deadline

/** A reusable timer created by [[AsyncOperations.deadlineTimer]], on `System.nanoTime`'s clock.
  *
  * The deadline may be moved from any thread, including while a caller is waiting. A move after a wait completes or is
  * cancelled affects the next wait; it does not restart the previous one. A move racing completion may be too late.
  */
trait DeadlineTimer extends Deadline {

  /** Suspends until the current deadline, at once if it has passed, or throws if the caller is cancelled. At most one
    * caller may wait at a time; another is refused with `IllegalArgumentException`. May be called again after
    * returning.
    */
  def await()(using Async): Unit
}
