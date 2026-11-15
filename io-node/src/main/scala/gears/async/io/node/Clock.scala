package gears.async.io
package node

import scala.scalajs.js

/** Times on `System.nanoTime`'s clock, compared by their difference, as it may wrap, and what waits for them: Node's
  * timers, which count whole milliseconds on a clock of their own and may fire a little early by this one, so that a
  * time waited for is looked at again when its timer fires, and waited for again if it has not come.
  */
private[node] object Clock {

  /** No time: what waits for it never fires, though it waits, and keeps the event loop alive as any wait does. */
  inline val Never = Long.MaxValue

  /** The longest Node's timers wait, in milliseconds: beyond 2^31 - 1 they fire at once. */
  private inline val LongestWait = 1L << 30

  /** `nanos` after `from`, or `Never` if that is beyond what the clock counts. */
  def after(from: Long, nanos: Long): Long =
    if nanos >= Never - from then Never else from + nanos

  /** Whether `time` has come. */
  def passed(time: Long): Boolean = time != Never && time - System.nanoTime() <= 0

  /** Calls `fire` once `time` has come, never before, and never from within this call: on a later turn of the event
    * loop even if it has come already. The alarm returned cancels it.
    */
  def at(time: Long)(fire: () => Unit): Alarm = {
    val alarm = new Alarm(time, fire)
    alarm.arm(first = true)
    alarm
  }

  /** A call waiting for its time (see `at`). */
  final class Alarm private[Clock] (time: Long, fire: () => Unit) {
    private var timeout: js.UndefOr[js.timers.SetTimeoutHandle] = js.undefined
    private var immediate: js.UndefOr[js.Dynamic] = js.undefined
    private var over = false

    private[Clock] def arm(first: Boolean): Unit = {
      timeout = js.undefined
      if !over then {
        // A program that waits for ever is still running: without a timer of Node's, Node would end the process.
        val left = if time == Never then Never else time - System.nanoTime()
        if left > 0 then {
          val millis = math.min((left + 999_999L) / 1_000_000L, LongestWait)
          timeout = js.timers.setTimeout(millis.toDouble)(arm(first = false))
        } else if first then
          immediate = Node.setImmediate { () =>
            immediate = js.undefined
            ring()
          }
        else ring()
      }
    }

    private def ring(): Unit =
      if !over then {
        over = true
        fire()
      }

    /** Stops the alarm: `fire` is not called after. */
    def cancel(): Unit = {
      over = true
      timeout.foreach(js.timers.clearTimeout(_))
      immediate.foreach(Node.clearImmediate(_))
      timeout = js.undefined
      immediate = js.undefined
    }
  }
}
