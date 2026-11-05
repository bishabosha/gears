package gears.async.io.kqueue

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Cancellable
import gears.async.Listener
import gears.async.Scheduler
import gears.async.io.Backend
import gears.async.io.Io
import gears.async.native.NativeSuspend

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import BaseWorkpoolScheduler.{ReentrantOp, TimerOp, Worker}

abstract class BaseWorkpoolScheduler[B <: Backend { type Op <: TimerOp & ReentrantOp }](val backend: B, workers: Int)
    extends Scheduler
    with AsyncSupport
    with NativeSuspend
    with AsyncOperations
    with Io[B] {

  def makeWorker(): Worker[backend.type]

  private val workerTicker = new java.util.concurrent.atomic.AtomicInteger(0)
  private val workerGroup: Array[Worker[backend.type]] = Array.fill(workers)(makeWorker())
  private def nextWorker(): Worker[backend.type] = {
    val index = workerTicker.getAndIncrement() % workerGroup.length
    workerGroup(index)
  }

  type Scheduler = this.type

  override def performWithin(op: backend.Op, timeoutMillis: Int)(using Async): Unit = {
    op.requestEntry()
    try {
      val waiter = new BaseWorkpoolScheduler.Waiter(timeoutMillis)
      try {
        val worker = nextWorker()
        if timeoutMillis >= 0 then worker.addTimer(waiter)
        worker.schedule(op, waiter)
        waiter.await
      } finally {
        op.clearWaiter()
      }
    } finally {
      op.requestExit()
    }

    // if timeoutMillis <= 0 then
    //   // no deadline
    //   ()
    // else
    //   val waiter = new Async.Source[Unit] {

    //     override def poll(k: Listener[Unit]): Boolean = ???

    //     override def onComplete(k: Listener[Unit]): Unit = ???

    //     override def dropListener(k: Listener[Unit]): Unit = ???

    //   }
    //   summon[Async].scheduler.schedule(FiniteDuration(timeoutMillis.toLong, "millis"), new Runnable {
    //     override def run(): Unit = {
    //       val now = System.nanoTime()
    //       op.beginAfterDelay(now)
    //     }
    //   })
  }

  override def execute(body: Runnable): Unit =
    val worker = nextWorker()
    nextWorker().inbox.add(body)

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val initialDelay = System.nanoTime() + delay.toNanos
    val worker = nextWorker()
    val timerOp = new BaseWorkpoolScheduler.DoAfter(initialDelay, body)
    worker.addTimer(timerOp)
    new Cancellable {
      override def cancel(): Unit = {
        timerOp.requestCancel()
        worker.removeTimer(timerOp)
      }
    }
  }

}

object BaseWorkpoolScheduler {
  trait TimerOp {
    def deadlineNow: Long
    def beginAfterTimeout(actAfter: Long): Unit
  }

  trait ReentrantOp {
    def requestEntry(): Unit
    def setWaiter(waiter: BaseWorkpoolScheduler.Waiter): Unit
    def clearWaiter(): Unit
    def requestExit(): Unit
  }

  class Waiter(val deadlineNow: Long) extends Async.Source[Try[Unit]] with TimerOp {

    // val listeners = new java.util.concurrent.CopyOnWriteArrayList[Listener[Try[Unit]]]()
    val state = new java.util.concurrent.atomic.AtomicReference[Try[Unit]](null)

    def beginAfterTimeout(actAfter: Long): Unit = {
      if actAfter < this.deadlineNow then timeout()
    }

    def trySucceed(): Unit = {
      state.compareAndSet(null, Success(()))
    }

    def timeout(): Unit = {
      state.compareAndSet(null, Failure(new TimeoutException("Operation timed out")))
    }

    override def poll(k: Listener[Try[Unit]]): Boolean =
      state.get() match
        case null => false
        case data    =>
          k.completeNow(data, this)

    override def onComplete(k: Listener[Try[Unit]]): Unit =
      ???

    override def dropListener(k: Listener[Try[Unit]]): Unit =
      ???

  }

  class DoAfter(val deadlineNow: Long, body: Runnable) extends TimerOp {
    @volatile private var cancelled = false
    def beginAfterTimeout(actAfter: Long): Unit = {
      if (!cancelled) {
        if actAfter < this.deadlineNow then throw new TimeoutException("Operation timed out")
        body.run()
      }
    }
    def requestCancel(): Unit = {
      cancelled = true
    }
  }

  abstract class Worker[B <: Backend { type Op <: TimerOp }](val backend: B) {
    type TimerHeapObject = backend.Op | DoAfter | Waiter
    private given Ordering[TimerHeapObject]:
      def compare(x: TimerHeapObject, y: TimerHeapObject): Int =
        java.lang.Long.compare(y.deadlineNow, x.deadlineNow)
    private object timerLock
    private var timers: collection.mutable.PriorityQueue[TimerHeapObject] =
      collection.mutable.PriorityQueue.empty
    private[kqueue] val inbox: java.util.concurrent.ConcurrentLinkedQueue[Runnable] =
      new java.util.concurrent.ConcurrentLinkedQueue[Runnable]

    private val thread = {
      val t = new Thread(() => loop())
      t.setDaemon(true)
      t.start()
      t
    }

    def schedule(op: backend.Op, waiter: Waiter): Unit

    def loop(): Unit = {
      while (true) {
        val now = System.nanoTime()
        val readyTimers = timerLock.synchronized {
          var timerBudget = 32
          val buf = Vector.newBuilder[TimerHeapObject]
          while (timerBudget > 0 && timers.nonEmpty && timers.head.deadlineNow <= now) {
            timerBudget -= 1
            buf += timers.dequeue()
          }
          buf.result()
        }
        readyTimers.foreach {
          case timer: DoAfter =>
            timer.beginAfterTimeout(timer.deadlineNow)
          case timer: Waiter =>
            timer.beginAfterTimeout(timer.deadlineNow)
          case other: backend.Op @unchecked =>
            other.beginAfterTimeout(now)
        }
        var inboxBudget = 32
        var task = inbox.poll()
        while (inboxBudget > 0 && task != null) {
          task.run()
          inboxBudget -= 1
          task = inbox.poll()
        }
      }
    }

    def addTimer(timerOp: TimerHeapObject): Unit = timerLock.synchronized {
      timers.addOne(timerOp)
    }
    def removeTimer(timerOp: TimerHeapObject): Unit = timerLock.synchronized {
      val (rest, _) = timers.partition(_ ne timerOp)
      timers = rest
    }
  }
}

class KqueueScheduler(workers: Int) extends BaseWorkpoolScheduler(KqueueBackend, workers) {
  class KqueueWorker extends BaseWorkpoolScheduler.Worker(KqueueBackend) {
    override def schedule(op: KqueueBackend.Op, waiter: BaseWorkpoolScheduler.Waiter): Unit = {
      op match
        case timer: KqueueMakeTimer =>
          addTimer(timer)
        case deadline: KqueueMakeDeadline =>
          addTimer(deadline)
    }
  }
  override def makeWorker(): Worker[KqueueBackend.type] = new KqueueWorker()
}
