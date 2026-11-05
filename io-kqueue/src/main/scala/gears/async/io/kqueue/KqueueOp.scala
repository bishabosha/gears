package gears.async.io.kqueue

import gears.async.Async
import gears.async.io.Deadline
import gears.async.io.Io
import gears.async.io.Repeatable

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

sealed trait KqueueOp extends BaseWorkpoolScheduler.TimerOp, BaseWorkpoolScheduler.ReentrantOp

sealed abstract class WaiterAbstractKqueueOp extends KqueueOp {
  private var waiter: BaseWorkpoolScheduler.Waiter | Null = null
  def accessWaiter: BaseWorkpoolScheduler.Waiter | Null = waiter
  def setWaiter(waiter: BaseWorkpoolScheduler.Waiter): Unit = {
    this.waiter = waiter
  }
  def clearWaiter(): Unit = {
    this.waiter = null
  }
}

sealed abstract class SingleUseAbstractKqueueOp extends WaiterAbstractKqueueOp {
  @volatile private var spent = false
  def requestEntry(): Unit = {
    if spent then throw new IllegalStateException("Already consumed")
    spent = true
  }
  def requestExit(): Unit = {
    if !spent then throw new IllegalStateException("Not spent")
  }
}

sealed abstract class RepeatableAbstractKqueueOp extends WaiterAbstractKqueueOp with Repeatable {
  @volatile private var entered = false
  def requestEntry(): Unit = {
    if entered then throw new IllegalStateException("Concurrent entry not allowed")
    entered = true
  }
  def requestExit(): Unit = {
    if !entered then throw new IllegalStateException("Not entered")
    entered = false
  }
}

sealed trait KqueueReadableOp extends KqueueOp

sealed trait KqueueWritableOp extends KqueueOp

sealed trait KqueueTimerOp extends KqueueOp

sealed trait KqueueDeadlineOp extends KqueueOp with Deadline

sealed trait KqueueCloseOp extends KqueueOp

private[kqueue] final class KqueueMakeTimer(duration: FiniteDuration)
    extends RepeatableAbstractKqueueOp
    with KqueueTimerOp {
  val deadlineNow: Long = System.nanoTime() + duration.toNanos
  def beginAfterTimeout(actAfter: Long): Unit = {
    if actAfter < this.deadlineNow then
      accessWaiter match
        case null => () // TODO: what now?
        case w    => w.timeout()
    else
      accessWaiter match
        case null => ()
        case w    => w.trySucceed()
  }
}

private[kqueue] final class KqueueMakeDeadline(initial: Long) extends RepeatableAbstractKqueueOp with KqueueDeadlineOp {
  @volatile private var myDeadline: Long = initial

  override def deadline: Long = synchronized(myDeadline)

  override def moveTo(nanoTime: Long): Unit = synchronized {
    myDeadline = nanoTime
  }

  def deadlineNow: Long = deadline
  def beginAfterTimeout(actAfter: Long): Unit = {
    if actAfter < this.deadlineNow then
      accessWaiter match
        case null => () // TODO: what now?
        case w    => w.timeout()
    else
      accessWaiter match
        case null => ()
        case w    => w.trySucceed()
  }
}
