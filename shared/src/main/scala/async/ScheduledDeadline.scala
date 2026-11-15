package gears.async

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*

/** A reusable, movable deadline built only on [[Scheduler.schedule]] and [[Cancellable.cancel]].
  *
  * One caller may [[await]] at a time. Moving later only publishes the new deadline: the scheduled callback checks it
  * again and reschedules itself if necessary. Moving earlier cancels and replaces the registration, without resuming
  * the caller. Cancellation cancels the registration before `await` returns. Moves racing completion may be too late,
  * as for an I/O deadline timer. The scheduler must outlive the wait.
  */
final class ScheduledDeadline(initial: Long, scheduler: Scheduler) extends DeadlineTimer {
  private val at = new AtomicLong(initial)
  @volatile private var active: Waiting | Null = null

  private final class Waiting(val resolver: Future.Resolver[Unit]) {
    // Guarded by ScheduledDeadline.this's monitor, as are registration changes and completion.
    var registration: Cancellable | Null = null
    var generation = 0L
    var done = false
  }

  def deadline: Long = at.get()

  def moveTo(nanoTime: Long): Unit = {
    val previous = at.getAndSet(nanoTime)
    if nanoTime - previous < 0 then {
      val wait = active
      if wait != null then
        synchronized {
          if (active eq wait) && !wait.done then {
            if wait.registration != null then wait.registration.nn.cancel()
            arm(wait)
          }
        }
    }
  }

  /** Suspends until the latest deadline, or throws if the caller is cancelled. May be called again after returning. */
  def await()(using async: Async): Unit = {
    if async.group.isCancelled then throw new CancellationException()
    var started: Waiting | Null = null
    val future = Future.withResolver[Unit] { resolver =>
      synchronized {
        if active != null then throw new IllegalArgumentException("The deadline already has a waiter")
        val wait = new Waiting(resolver)
        started = wait
        active = wait
        resolver.onCancel(() => cancel(wait))
        try arm(wait)
        catch {
          case failure: Throwable =>
            active = null
            wait.done = true
            throw failure
        }
      }
    }
    try {
      future.link()
      future.await
    } finally {
      cancel(started.nn)
      future.unlink()
      synchronized { if active eq started then active = null }
    }
  }

  // Called under this timer's monitor. A callback that cancellation could not retract is rejected by generation.
  private def arm(wait: Waiting): Unit = {
    wait.generation += 1
    val generation = wait.generation
    val remaining = at.get() - System.nanoTime()
    wait.registration = scheduler.schedule(math.max(0L, remaining).nanos, () => fired(wait, generation))
  }

  private def fired(wait: Waiting, generation: Long): Unit = {
    val complete = synchronized {
      if (active ne wait) || wait.done || generation != wait.generation then false
      else {
        wait.registration = null // This registration has already left the scheduler.
        if at.get() - System.nanoTime() > 0 then {
          arm(wait)
          false
        } else {
          wait.done = true
          true
        }
      }
    }
    // Resolving can notify listeners. Never do it while holding the timer's monitor.
    if complete then wait.resolver.resolve(())
  }

  private def cancel(wait: Waiting): Unit = {
    val cancelled = synchronized {
      if wait.done then false
      else {
        wait.done = true
        wait.generation += 1
        if wait.registration != null then wait.registration.nn.cancel()
        wait.registration = null
        true
      }
    }
    if cancelled then wait.resolver.rejectAsCancelled()
  }
}
