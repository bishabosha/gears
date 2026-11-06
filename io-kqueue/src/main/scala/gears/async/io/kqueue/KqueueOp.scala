package gears.async.io.kqueue

import gears.async.io.Deadline
import gears.async.io.Repeatable

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

sealed trait KqueueOp

/** Timer descriptions are reusable; completion and queue membership belong to each performance. */
sealed abstract class KqueueTimerOp extends KqueueOp with Repeatable {
  private val active = new AtomicReference[TimerWait](null)

  private[kqueue] def deadlineAt(startedAt: Long): Long

  private[kqueue] final def attach(wait: TimerWait): Unit =
    if !active.compareAndSet(null, wait) then throw new IllegalArgumentException("The timer is already being performed")

  private[kqueue] final def detach(wait: TimerWait): Unit = {
    if !active.compareAndSet(wait, null) then throw new IllegalStateException("Not this timer's active performance")
  }

  protected final def movedEarlier(): Unit = {
    val wait = active.get()
    if wait != null then wait.requestUpdate()
  }
}

sealed abstract class KqueueDeadlineOp extends KqueueTimerOp with Deadline
sealed trait KqueueReadableOp extends KqueueOp
sealed trait KqueueWritableOp extends KqueueOp
sealed trait KqueueCloseOp extends KqueueOp

private[kqueue] final class KqueueMakeTimer(duration: FiniteDuration) extends KqueueTimerOp {
  private val nanos = duration.toNanos
  override private[kqueue] def deadlineAt(startedAt: Long): Long = startedAt + nanos
}

private[kqueue] final class KqueueMakeDeadline(initial: Long) extends KqueueDeadlineOp {
  private val at = new AtomicLong(initial)
  override def deadline: Long = at.get()

  override def moveTo(nanoTime: Long): Unit = {
    val previous = at.getAndSet(nanoTime)
    // Later moves need no queue command: the owner rechecks at the old, earlier wakeup.
    if nanoTime - previous < 0 then movedEarlier()
  }

  override private[kqueue] def deadlineAt(startedAt: Long): Long = at.get()
}
