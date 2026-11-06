package gears.async.io.shareable

import TimerHeap.{TimerOp, TimerSource}
import gears.async.io.shareable.Poller

object TimerHeap {
  trait TimerSource[A <: TimerOp] {
    def notifyDeadlineChange(): Unit
    def debugTimerState(): String
    def addTimer(timerOp: A): Unit
    /** Whether `timerOp` was taken out of the heap, and so will not fire. If not, it has already left the heap, to fire
      * or having fired.
      */
    def removeTimer(timerOp: A): Boolean
    /** Takes the next timer due by `now` out of the heap, or gives null if none is. */
    def nextReadyTimer(now: Long): A | Null
    def peekNextDeadline(): Long
  }

  trait TimerOp {
    def deadlineNow: Long
    /** Fires the timer. It left the heap for this, under the heap's lock, so `removeTimer` can no longer stop it. */
    def timerWakeup(actAfter: Long): Unit
  }
}

abstract class TimerHeap[A <: TimerOp] extends Poller with TimerSource[A]  {
  private object timerLock

  private given Ordering[A]:
    def compare(x: A, y: A): Int =
      java.lang.Long.compare(y.deadlineNow, x.deadlineNow)
  private var timers: collection.mutable.PriorityQueue[A] =
    collection.mutable.PriorityQueue.empty[A]

  final def nextReadyTimer(now: Long): A | Null = timerLock.synchronized {
    if timers.nonEmpty && timers.head.deadlineNow <= now then timers.dequeue() else null
  }

  final def peekNextDeadline(): Long = timerLock.synchronized {
    if timers.nonEmpty then timers.head.deadlineNow else -1
  }

  final def debugTimerState() = timerLock.synchronized({
    timers.toString()
  })

  final def addTimer(timerOp: A): Unit =
    timerLock.synchronized {
      println("adding timer")
      timers.addOne(timerOp)
    }
    wake()
  final def removeTimer(timerOp: A): Boolean =
    val removed = timerLock.synchronized {
      val (rest, matching) = timers.partition(_ ne timerOp)
      timers = rest
      matching.nonEmpty
    }
    wake()
    removed
  final def notifyDeadlineChange(): Unit =
    timerLock.synchronized {
      // resort the priority queue to reflect any changes in deadlines
      timers = collection.mutable.PriorityQueue(timers.toSeq*)
    }
    wake()
}
