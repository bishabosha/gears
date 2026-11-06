package gears.async.io.shareable

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

import WorkpoolScheduler.{ReentrantOp, AsyncOp, Worker}
import gears.async.io.shareable.TimerHeap.{TimerOp, TimerSource}
import gears.async.io.shareable.WorkpoolScheduler.DoAfter
import gears.async.SuspendSupport
import gears.async.io.shareable.TimerHeap

abstract class WorkpoolScheduler[Op1 <: AsyncOp & ReentrantOp, B <: Backend { type Op = Op1 }](val backend: B, workers: Int)
    extends Scheduler
    with AsyncSupport
    with AsyncOperations
    with Io[B] {

  override private[async] def resumeAsync[T, R](suspension: Suspension[T, R])(arg: T)(using s: Scheduler): Unit =
    // "awaits" are called from the thread pool only.
    assert(members.contains(Thread.currentThread()), "SEMANTIC ERROR, resumed on non-pool thread")
    suspension.resume(arg)

  /** Schedule a computation with the suspension boundary already created. */
  // override private[async] def scheduleBoundary(body: Label[Unit] ?=> Unit)(using s: Scheduler): Unit =
  //   s.execute(() => boundary(body))


  def makeWorker(id: Int): Worker[Op1, B]

  private val workerTicker = new java.util.concurrent.atomic.AtomicInteger(0)
  private val workerGroup: Array[Worker[Op1, B]] = Array.tabulate(workers)(makeWorker)
  private val members = workerGroup.map(_.thread).toSet
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
    println("scheduling task")
    val initialDelay = System.nanoTime() + delay.toNanos
    val worker = nextWorker()
    val timerOp = new WorkpoolScheduler.DoAfter(initialDelay, body)
    worker.addTimer(timerOp)
    new Cancellable {
      override def cancel(): Unit = {
        worker.removeTimer(timerOp)
      }
    }
  }

}

object WorkpoolScheduler {

  trait AsyncOp extends TimerOp, Async.Source[Try[Unit]] {
    def setTimeout(timeoutMillis: Int): Unit
    def clearTimeout(): Unit
    def settled: Boolean
    def registerTimerSource(source: TimerSource[?]): Unit
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
        println("running Scheduled task")
        body.run()
        println("finished scheduled task")
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
      println("submit scheduled...")
      inbox.add(task)
      wake()
    }

    private[WorkpoolScheduler] val thread = {
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
          println("draining inbox task...")
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
