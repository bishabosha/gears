package asyncio.nio

import java.nio.channels.ClosedSelectorException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.concurrent.locks.LockSupport

/** NIO's waits on the calling thread (`ReactorBackend.waitHere`), for a caller on no reactor's thread: a command waits
  * on its handle's own selector, a timer parks until its time. Nothing is interrupted and nothing spins; a cancel
  * wakes the wait. Written for a backend with no reactor, since retired, whose way of waiting these are (see the NIO
  * TODO, items 10 to 12 and 14).
  */
private[nio] object WaitsHere {

  /** For `select`'s action: the op tries again itself, so the selected keys need nothing done. */
  val Ignore: java.util.function.Consumer[SelectionKey] = _ => ()
}

/** A wait on the calling thread for a handle to be ready for `ops`, on the handle's own selector for that direction
  * (see `Selectable.enterWait`): `ReactorBackend.waitHere` for NIO's commands. Cancelled with `Selector.wakeup`, so
  * the handle stays open.
  */
private[nio] final class SelectorWait(entry: Selectable, write: Boolean, ops: Int) extends asyncio.WaitHere {
  @volatile private var cancelled = false
  @volatile private var waitingOn: Selector | Null = null

  /** Never finishes the op itself: the caller tries it once the selector reports the handle ready, or its time is
    * up.
    */
  def await(timeoutNanos: Long): Boolean = {
    val selector = entry.enterWait(write)
    try {
      val key = entry.channel.keyFor(selector)
      if key != null && key.interestOps() != ops then key.interestOps(ops): Unit
      waitingOn = selector
      if !cancelled then {
        if timeoutNanos < 0 then selector.select(WaitsHere.Ignore)
        else if timeoutNanos == 0 then selector.selectNow(WaitsHere.Ignore)
        else selector.select(WaitsHere.Ignore, (timeoutNanos + 999_999L) / 1_000_000L) // rounded up: due when it returns
      }: Unit
    } catch {
      case _: ClosedSelectorException | _: java.nio.channels.CancelledKeyException => ()
    } finally {
      waitingOn = null
      entry.exitWait()
    }
    false
  }

  def cancel(): Unit = {
    cancelled = true
    val selector = waitingOn
    if selector != null then selector.wakeup(): Unit
  }
}

/** A timer waited for on the calling thread, parked until its time has passed: `ReactorBackend.waitHere` for NIO's
  * timers, which contend for nothing, so a caller waits this way whatever else is waiting. Cancelled with an unpark.
  */
private[nio] final class TimerWait(milliseconds: Int) extends asyncio.WaitHere {
  private val deadline = System.nanoTime() + milliseconds * 1_000_000L
  @volatile private var cancelled = false
  @volatile private var parked: Thread | Null = null

  /** Parks until the deadline, a cancel, or the end of `timeoutNanos` if not negative; true once the timer's time has
    * passed.
    */
  def await(timeoutNanos: Long): Boolean = {
    parked = Thread.currentThread()
    try {
      val until = if timeoutNanos < 0 then deadline else math.min(deadline, System.nanoTime() + timeoutNanos)
      var remaining = until - System.nanoTime()
      while remaining > 0 && !cancelled do {
        LockSupport.parkNanos(this, remaining)
        remaining = until - System.nanoTime()
      }
      deadline - System.nanoTime() <= 0
    } finally parked = null
  }

  def cancel(): Unit = {
    cancelled = true
    val thread = parked
    if thread != null then LockSupport.unpark(thread)
  }
}
