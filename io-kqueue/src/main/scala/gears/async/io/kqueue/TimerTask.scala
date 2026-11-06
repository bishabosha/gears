package gears.async.io.kqueue

import gears.async.Async
import gears.async.Cancellable
import gears.async.Listener

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

private[kqueue] enum TimerResult {
  case Succeeded, TimedOut, Cancelled
}

/** One registration is also its inbox node and indexed heap entry. No wrapper or per-update command is needed. The
  * owner alone changes heapIndex and calls finish. Producers may only request updates or cancellation.
  */
private[kqueue] abstract class TimerTask(val owner: Worker) extends WorkerCommand {
  import TimerTask.*

  private val control = new AtomicInteger()
  private[kqueue] var heapIndex: Int = -1

  final def isDone: Boolean = (control.get() & Done) != 0
  final def isCancelled: Boolean = (control.get() & Cancelled) != 0
  final def requestUpdate(): Unit = request(0)
  final def cancel(): Unit = request(Cancelled)

  private def request(flags: Int): Unit = {
    var before = control.get()
    while (before & Done) == 0 do {
      val after = before | Queued | flags
      if before == after then return
      if control.compareAndSet(before, after) then {
        if (before & Queued) == 0 then owner.submit(this)
        return
      }
      before = control.get()
    }
  }

  private[kqueue] final def dequeued(): Unit = {
    var before = control.get()
    while !control.compareAndSet(before, before & ~Queued) do before = control.get()
  }

  /** Called only by the owner, which caches the key in its heap's primitive deadline array. */
  private[kqueue] def refreshDeadline(): Long
  protected def completed(cancelled: Boolean): Unit

  private[kqueue] final def finish(): Unit = {
    val before = control.getAndSet(Done)
    if (before & Done) == 0 then completed((before & Cancelled) != 0)
  }

  private[kqueue] final def abort(): Unit = {
    val before = control.getAndSet(Done)
    if (before & Done) == 0 then completed(true)
  }
}

private object TimerTask {
  final val Queued = 1
  final val Cancelled = 2
  final val Done = 4
}

/** A single-consumer source: null = pending, Listener = subscribed, TimerResult = completed. Completion and
  * subscription compete on one CAS slot, so completion before subscription cannot be lost.
  */
private[kqueue] final class TimerWait(owner: Worker, op: KqueueTimerOp, timeoutMillis: Int)
    extends TimerTask(owner)
    with Async.Source[TimerResult] {
  private val startedAt = System.nanoTime()
  private val timeoutAt =
    if timeoutMillis < 0 then Long.MaxValue else startedAt + timeoutMillis.toLong * 1_000_000L
  private val completion = new AtomicReference[AnyRef](null)
  private var timesOut = false // Owner-only, derived from the same deadline snapshot as the heap key.

  override def refreshDeadline(): Long = {
    val due = op.deadlineAt(startedAt)
    timesOut = timeoutMillis >= 0 && timeoutAt - due < 0
    if timesOut then timeoutAt else due
  }

  override protected def completed(cancelled: Boolean): Unit = {
    val result =
      if cancelled then TimerResult.Cancelled
      else if timesOut then TimerResult.TimedOut
      else TimerResult.Succeeded
    completion.getAndSet(result) match {
      case listener: Listener[TimerResult] @unchecked => listener.completeNow(result, this)
      case _                                          => ()
    }
  }

  override def poll(listener: Listener[TimerResult]): Boolean = completion.get() match {
    case result: TimerResult =>
      listener.completeNow(result, this)
      true
    case _ => false
  }

  override def onComplete(listener: Listener[TimerResult]): Unit = {
    var before = completion.get()
    while true do
      before match {
        case result: TimerResult =>
          listener.completeNow(result, this)
          return
        case _ =>
          // Cancellation can resume the caller before dropListener finishes. Its cleanup await replaces that
          // dead subscription; the old dropListener's CAS cannot remove the replacement.
          if completion.compareAndSet(before, listener) then return
          before = completion.get()
      }
  }

  override def dropListener(listener: Listener[TimerResult]): Unit =
    completion.compareAndSet(listener, null): Unit
}

private[kqueue] final class ScheduledTask(owner: Worker, at: Long, body: Runnable)
    extends TimerTask(owner)
    with Cancellable {
  override def refreshDeadline(): Long = at
  override protected def completed(cancelled: Boolean): Unit =
    if !cancelled then body.run()
}
