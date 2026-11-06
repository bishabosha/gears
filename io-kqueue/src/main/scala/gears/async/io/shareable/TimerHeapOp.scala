package gears.async.io.shareable

import scala.util.Try
import java.util.concurrent.atomic.AtomicReference
import gears.async.Listener
import scala.util.Failure
import gears.async.io.Repeatable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.TimeoutException
import gears.async.io.Deadline

object TimerHeapOp {
  sealed trait Hooks {
    def onEntry(): Unit = ()
  }
}

trait TimerHeapOp extends WorkpoolScheduler.AsyncOp, WorkpoolScheduler.ReentrantOp


sealed abstract class AbstractTimerHeapOp extends TimerHeapOp with TimerHeapOp.Hooks {
  private val state = AtomicReference[Try[Unit]](null)
  private val timersLock = new Object
  @volatile private var isTimerCancelled = false

  /** Guards `state` against `listener`, so that a result can't arrive between `onComplete` seeing none and registering
    * its listener, which would then wait for ever.
    */
  private val listenerLock = new Object

  @volatile private var listener: Listener[Try[Unit]] | Null = null
  @volatile private var worker: TimerHeap.TimerSource[?] | Null = null

  /** Forgets the last performance's outcome, for the next performance of a repeatable op. */
  protected def reset(): Unit =
    listenerLock.synchronized {
      state.set(null)
      listener = null
    }
    timersLock.synchronized {
      isTimerCancelled = false
    }

  def settled: Boolean = state.get() != null

  def wakeup(deadline: Long): Unit

  final def beginAfterTimeout(deadline: Long): Unit =
    worker match
      case null =>
      case w =>
        w.sync() // let the timer heap potentially cancel us.
        // Ensure that the timer has not been cancelled before beginning after timeout
        if !isTimerCancelled then
          println(s"WAKING UP OP")
          wakeup(deadline)
          println(s"POST WAKEUP OP")

  final def registerTimerSource(source: TimerHeap.TimerSource[?]): Unit = timersLock.synchronized {
    worker = source
  }
  final def clearTimerSource(): Unit = timersLock.synchronized {
    worker = null
  }
  final def requestTimerSort(): Unit = timersLock.synchronized {
    val w = worker
    if w != null then
      w.notifyDeadlineChange()
  }
  final def timerCancelled(): Unit = timersLock.synchronized {
    isTimerCancelled = true
  }

  final def settle(result: Try[Unit]): Unit = {
    val l = listenerLock.synchronized {
      if !state.compareAndSet(null, result) then throw new IllegalStateException("Already settled")
      val waiting = listener
      listener = null
      waiting
    }
    if l != null then l.completeNow(result, this)
  }

  final def succeed(): Unit = settle(WorkpoolScheduler.SuccessUnit)
  final def fail(ex: Throwable): Unit = settle(Failure(ex))

  final override def poll(k: Listener[Try[Unit]]): Boolean =
    println("KqueueOp poll called")
    state.get() match
      case null => false
      case data => k.completeNow(data, this)

  final override def onComplete(k: Listener[Try[Unit]]): Unit = {
    val result = listenerLock.synchronized {
      val current = state.get()
      if current == null then {
        if listener != null then
          throw new IllegalStateException("Listener already registered")
        listener = k
        println("registered listener to KqueueOp of class " + k.getClass)
      }
      current
    }
    if result != null then k.completeNow(result, this)
  }

  /** Best effort, as `Source` has it: `settle` may already have taken the listener. */
  final override def dropListener(k: Listener[Try[Unit]]): Unit = listenerLock.synchronized {
    if listener eq k then listener = null
  }
}

abstract class SingleUseAbstractTimerHeapOp extends AbstractTimerHeapOp {
  @volatile private var spent = false
  final def requestEntry(): Unit = {
    if spent then throw new IllegalStateException("Already consumed")
    spent = true
    onEntry()
  }
  final def requestExit(): Unit = {
    if !spent then throw new IllegalStateException("Not spent")
  }
}

abstract class RepeatableAbstractTimerHeapOp extends AbstractTimerHeapOp with Repeatable {
  @volatile private var entered = false
  final def requestEntry(): Unit = {
    if entered then throw new IllegalStateException("Concurrent entry not allowed")
    entered = true
    reset()
    onEntry()
  }
  final def requestExit(): Unit = {
    if !entered then throw new IllegalStateException("Not entered")
    entered = false
  }
}

abstract class AbstractMakeTimerOp(duration: FiniteDuration)
    extends RepeatableAbstractTimerHeapOp {

  @volatile private var timeoutDeadline = -1L

  def setTimeout(t: Int): Unit = {
    if t < 0 then timeoutDeadline = -1L
    else
      timeoutDeadline = System.nanoTime() + t * 1000000L
  }
  def clearTimeout(): Unit = {
    timeoutDeadline = -1L
  }


  /** When this performance's `duration` is up: a timer counts from each submission. */
  @volatile private var initialDeadline: Long = -1L

  override def onEntry(): Unit = {
    super.onEntry()
    initialDeadline = System.nanoTime() + duration.toNanos
  }

  def deadlineNow: Long =
    val t = timeoutDeadline
    if t >= 0L then math.min(initialDeadline, t)
    else initialDeadline

  def wakeup(actAfter: Long): Unit = {
    val t = timeoutDeadline
    if t >= 0L && actAfter >= t && t < initialDeadline then
      fail(new TimeoutException("Operation timed out"))
    else
      succeed()
  }
}

abstract class AbstractMakeDeadlineOp(initial: Long) extends RepeatableAbstractTimerHeapOp with Deadline {
  @volatile private var myDeadline: Long = initial

  @volatile private var timeoutDeadline = -1L

  private val lock = new Object

  def setTimeout(t: Int): Unit = lock.synchronized {
    if t < 0 then timeoutDeadline = -1L
    else
      timeoutDeadline = System.nanoTime() + t * 1000000L
  }
  def clearTimeout(): Unit = lock.synchronized {
    timeoutDeadline = -1L
  }

  override def deadline: Long = lock.synchronized(myDeadline)

  override def moveTo(nanoTime: Long): Unit =
    if nanoTime < 0 then throw new IllegalArgumentException("Deadline must be non-negative")
    lock.synchronized {
      myDeadline = nanoTime
    }
    // the heap's order has changed, and the worker may be sleeping until the old deadline
    requestTimerSort()

  def deadlineNow: Long = lock.synchronized {
    val t = timeoutDeadline
    if t >= 0L then math.min(myDeadline, t)
    else myDeadline
  }

  def wakeup(actAfter: Long): Unit = {
    if timeoutDeadline >= 0L then
      val timedout = lock.synchronized {
        actAfter >= timeoutDeadline && timeoutDeadline < myDeadline
      }
      if timedout then
        fail(new TimeoutException("Operation timed out"))
      else
        succeed()
    else
      succeed()
  }
}
