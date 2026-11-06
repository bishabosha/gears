package gears.async.io.shareable

import TimerHeap.{TimerOp, TimerSource}
import gears.async.io.shareable.Poller

object TimerHeap {
  trait TimerSource[A <: TimerOp] {
    def notifyDeadlineChange(): Unit
    def sync(): Unit
    def debugTimerState(): String
    def addTimer(timerOp: A): Unit
    def removeTimer(timerOp: A): Unit
    def readyTimers(now: Long): IterableOnce[A]
    def peekNextDeadline(): Long
  }

  trait TimerOp {
    def deadlineNow: Long
    def timerCancelled(): Unit
    def beginAfterTimeout(actAfter: Long): Unit
  }
}

abstract class TimerHeap[A <: TimerOp] extends Poller with TimerSource[A]  {
  private object timerLock

  private given Ordering[A]:
    def compare(x: A, y: A): Int =
      java.lang.Long.compare(y.deadlineNow, x.deadlineNow)
  private var timers: collection.mutable.PriorityQueue[A] =
    collection.mutable.PriorityQueue.empty[A]

  final def readyTimers(now: Long): Vector[A] = timerLock.synchronized {
    var timerBudget = 32
    val buf = Vector.newBuilder[A]
    while timerBudget > 0 && timers.nonEmpty && timers.head.deadlineNow <= now do
      timerBudget -= 1
      buf += timers.dequeue()
    buf.result()
  }

  final def peekNextDeadline(): Long = timerLock.synchronized {
    if timers.nonEmpty then timers.head.deadlineNow else -1
  }

  final def sync(): Unit = timerLock.synchronized {}

  final def debugTimerState() = timerLock.synchronized({
    timers.toString()
  })

  final def addTimer(timerOp: A): Unit =
    timerLock.synchronized {
      println("adding timer")
      timers.addOne(timerOp)
    }
    wake()
  final def removeTimer(timerOp: A): Unit =
    timerLock.synchronized {
      timerOp.timerCancelled()
      val (rest, _) = timers.partition(_ ne timerOp)
      timers = rest
    }
    wake()
  final def notifyDeadlineChange(): Unit =
    timerLock.synchronized {
      // resort the priority queue to reflect any changes in deadlines
      timers = collection.mutable.PriorityQueue(timers.toSeq*)
    }
    wake()
}
