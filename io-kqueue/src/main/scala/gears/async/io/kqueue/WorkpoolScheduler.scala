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

import BaseWorkpoolScheduler.{ReentrantOp, AsyncOp, Worker}
import gears.async.io.kqueue.BaseWorkpoolScheduler.TimerSource
import gears.async.io.kqueue.BaseWorkpoolScheduler.TimerOp
import gears.async.io.kqueue.BaseWorkpoolScheduler.DoAfter

abstract class BaseWorkpoolScheduler[Op1 <: AsyncOp & ReentrantOp, B <: Backend { type Op = Op1 }](val backend: B, workers: Int)
    extends Scheduler
    with AsyncSupport
    with NativeSuspend
    with AsyncOperations
    with Io[B] {

  override def suspend[T, R](body: Suspension[T, R] => R)(using Label[R]): T =
    println("Suspending...")
    super.suspend(body)

  def makeWorker(id: Int): Worker[Op1, B]

  private val workerTicker = new java.util.concurrent.atomic.AtomicInteger(0)
  private val workerGroup: Array[Worker[Op1, B]] = Array.tabulate(workers)(makeWorker)
  private def nextWorker(): Worker[Op1, B] = {
    val index = workerTicker.getAndIncrement() % workerGroup.length
    workerGroup(index)
  }

  type Scheduler = this.type

  override def performWithin(op: backend.Op, timeoutMillis: Int)(using Async): Unit = {
    op.requestEntry()
    try {
      val worker = nextWorker()
      try {
        op.setTimeout(timeoutMillis)
        op.registerTimerSource(worker)
        worker.schedule(op)
        op.await
      } finally {
        // Cancelled: the op has stopped, so it can't fire later, into its next performance if it has one.
        if !op.settled then worker.removeTimer(op)
        op.clearTimeout()
        op.clearTimerSource()
      }
    } finally {
      op.requestExit()
    }
  }

  override def execute(body: Runnable): Unit =
    val worker = nextWorker()
    nextWorker().submit(body)

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val initialDelay = System.nanoTime() + delay.toNanos
    val worker = nextWorker()
    val timerOp = new BaseWorkpoolScheduler.DoAfter(initialDelay, body)
    worker.addTimer(timerOp)
    new Cancellable {
      override def cancel(): Unit = {
        worker.removeTimer(timerOp)
      }
    }
  }

}

object BaseWorkpoolScheduler {

  trait TimerSource {
    def sortTimers(): Unit
    def sync(): Unit
  }

  trait TimerOp {
    def deadlineNow: Long
    def timerCancelled(): Unit
    def beginAfterTimeout(actAfter: Long): Unit
  }

  trait AsyncOp extends TimerOp, Async.Source[Try[Unit]] {
    def setTimeout(timeoutMillis: Int): Unit
    def clearTimeout(): Unit
    def settled: Boolean
    def registerTimerSource(source: TimerSource): Unit
    def clearTimerSource(): Unit
  }

  val SuccessUnit: Try[Unit] = Success(())

  trait ReentrantOp {
    def requestEntry(): Unit
    def requestExit(): Unit
  }

  class DoAfter(val deadlineNow: Long, body: Runnable) extends TimerOp {
    @volatile private var cancelled = false
    def beginAfterTimeout(actAfter: Long): Unit = {
      if (!cancelled) {
        if actAfter < this.deadlineNow then throw new TimeoutException("Operation timed out")
        body.run()
      }
    }
    def timerCancelled(): Unit = {
      cancelled = true
    }
  }

  abstract class Worker[Op1 <: TimerOp & ReentrantOp, B <: Backend { type Op = Op1 }](val id: Int, val backend: B) extends TimerHeap[Op1 | DoAfter] {
    // private[kqueue] val inbox: java.util.concurrent.ArrayBlockingQueue[Runnable] =
    //   new java.util.concurrent.ArrayBlockingQueue[Runnable](1024)
    private val inbox: java.util.concurrent.ConcurrentLinkedQueue[Runnable] =
      new java.util.concurrent.ConcurrentLinkedQueue[Runnable]

    def submit(task: Runnable): Unit = {
      inbox.add(task)
      wake()
    }

    private val thread = {
      val t = new Thread(() => loop())
      t.setDaemon(true)
      t.start()
      t
    }

    def schedule(op: Op1): Unit

    def isEmpty(): Boolean = inbox.isEmpty()

    def drainInbox(): Unit = {
      var inboxBudget = 32
      while (inboxBudget > 0) {
        val task = inbox.poll()
        if task == null then inboxBudget = 0
        else {
          task.run()
          println(s"Processed task: $task")
          inboxBudget -= 1
        }
      }
    }

    def loop(): Unit = {
      while (true) {
        val now = System.nanoTime()
        // Ops fire with the lock held, so that once `removeTimer` returns, a cancelled op is neither in the heap nor
        // about to fire. Completing one only schedules its waiter, so no program code runs here.
        readyTimers(now).foreach(timer => timer.beginAfterTimeout(now))
        drainInbox()
        waitForDeadline(peekNextDeadline())
      }
    }
  }
}

class KqueueScheduler(workers: Int) extends BaseWorkpoolScheduler(KqueueBackend, workers) {
  class KqueueWorker(id: Int) extends BaseWorkpoolScheduler.Worker(id, KqueueBackend) {
    override def schedule(op: KqueueBackend.Op): Unit = {
      op match
        case timer: KqueueMakeTimer =>
          addTimer(timer)
        case deadline: KqueueMakeDeadline =>
          addTimer(deadline)
    }
  }
  override def makeWorker(id: Int): Worker[KqueueBackend.Op, KqueueBackend.type] = new KqueueWorker(id)
}

abstract class Poller {
  private val notifier = new Object

  protected def wake(): Unit = notifier.synchronized {
    signalled = true
    notifier.notify()
  }

  protected def isEmpty(): Boolean

  def waitForDeadline(deadlineTask: Long): Unit = notifier.synchronized {
    // Anything that arrived after the reads above has set `signalled`; the inbox may also be over budget.
    if !signalled && isEmpty() then
      if deadlineTask >= 0 then
        val sleepTime = deadlineTask - System.nanoTime()
        if sleepTime > 0 then notifier.wait(sleepTime / 1000000, (sleepTime % 1000000).toInt)
      else notifier.wait()
    signalled = false
  }

  /** Guarded by `notifier`. Whether there has been anything new since the worker last looked: a `notify` alone is lost
    * if the worker is not yet waiting.
    */
  private var signalled = false
}

abstract class TimerHeap[A <: TimerOp] extends Poller with TimerSource  {
  private object timerLock

  private given Ordering[A]:
    def compare(x: A, y: A): Int =
      java.lang.Long.compare(y.deadlineNow, x.deadlineNow)
  private var timers: collection.mutable.PriorityQueue[A] =
    collection.mutable.PriorityQueue.empty[A]

  def readyTimers(now: Long): Vector[A] = timerLock.synchronized {
    var timerBudget = 32
    val buf = Vector.newBuilder[A]
    while timerBudget > 0 && timers.nonEmpty && timers.head.deadlineNow <= now do
      timerBudget -= 1
      buf += timers.dequeue()
    buf.result()
  }

  def peekNextDeadline(): Long = timerLock.synchronized {
    if timers.nonEmpty then timers.head.deadlineNow else -1
  }

  def sync(): Unit = timerLock.synchronized {}

  def addTimer(timerOp: A): Unit =
    timerLock.synchronized {
      timers.addOne(timerOp)
    }
    wake()
  def removeTimer(timerOp: A): Unit =
    timerLock.synchronized {
      timerOp.timerCancelled()
      val (rest, _) = timers.partition(_ ne timerOp)
      timers = rest
    }
    wake()
  def sortTimers(): Unit =
    timerLock.synchronized {
      timers = collection.mutable.PriorityQueue(timers.toSeq*)
    }
    wake()
}
