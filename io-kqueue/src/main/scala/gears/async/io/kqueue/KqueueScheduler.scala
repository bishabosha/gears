package gears.async.io.kqueue

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Cancellable
import gears.async.Scheduler
import gears.async.io.Io
import gears.async.native.NativeSuspend
import gears.async.uninterruptible

import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration

/** Runs continuations on workers, each of which consumes an inbox and owns a timer heap. */
class KqueueScheduler(workers: Int)
    extends Scheduler
    with AsyncSupport
    with NativeSuspend
    with AsyncOperations
    with Io[KqueueBackend.type]
    with AutoCloseable {
  require(workers > 0)

  val backend: KqueueBackend.type = KqueueBackend
  type Scheduler = this.type

  private val ticker = new AtomicInteger()
  private val pool = Array.tabulate(workers)(i => new Worker(s"gears-kqueue-$i"))

  private def nextWorker(): Worker = pool(Math.floorMod(ticker.getAndIncrement(), pool.length))

  override def execute(body: Runnable): Unit = nextWorker().execute(body)

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val task = new ScheduledTask(nextWorker(), System.nanoTime() + delay.toNanos, body)
    task.requestUpdate()
    task
  }

  override private[io] def performWithin(op: KqueueOp, timeoutMillis: Int)(using async: Async): Unit = {
    if async.group.isCancelled then throw new CancellationException()
    op match {
      case timer: KqueueTimerOp =>
        val wait = new TimerWait(nextWorker(), timer, timeoutMillis)
        timer.attach(wait)
        try {
          wait.requestUpdate()
          wait.awaitResult match {
            case TimerResult.Succeeded => ()
            case TimerResult.TimedOut  => throw new TimeoutException("Operation timed out")
            case TimerResult.Cancelled => throw new CancellationException()
          }
        } finally {
          try {
            if !wait.isDone then {
              wait.cancel()
              // A cancellation is acknowledged only after the owner removes the heap entry.
              uninterruptible { wait.awaitResult }
            }
          } finally timer.detach(wait)
        }
    }
  }

  override def close(): Unit = pool.foreach(_.close())
}
