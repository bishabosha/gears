package gears.async.io.kqueue

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import scala.util.control.NonFatal

/** An intrusive inbox node. Only its producer writes next before publication; the consumer owns it afterwards. */
private[kqueue] abstract class WorkerCommand {
  private[kqueue] var next: WorkerCommand | Null = null
}

private[kqueue] final class RunCommand(val body: Runnable) extends WorkerCommand

/** One consumer owns the heap and the detached inbox batch. Producers only CAS-push commands.
  *
  * Detaching and reversing a batch gives FIFO order by successful publication, without allocating queue nodes for timer
  * updates. A timer's queued bit reserves its node until the consumer removes it from the detached batch.
  */
private[kqueue] final class Worker(name: String) extends AutoCloseable {
  private val incoming = new AtomicReference[WorkerCommand](null)
  private val sleeping = new AtomicBoolean()
  private val timers = new TimerQueue()
  private var ready: WorkerCommand | Null = null
  @volatile private var stopping = false
  private val thread = new Thread(() => loop(), name)
  thread.setDaemon(true)
  thread.start()

  def execute(body: Runnable): Unit = submit(new RunCommand(body))

  def submit(command: WorkerCommand): Unit = {
    if stopping then
      command match {
        case task: TimerTask if task.isDone => return // A deadline mover may still hold a completed registration.
        case _                              => throw new RejectedExecutionException("The worker has stopped")
      }
    var head = incoming.get()
    command.next = head
    while !incoming.compareAndSet(head, command) do {
      head = incoming.get()
      command.next = head
    }
    wake()
  }

  private def wake(): Unit =
    if sleeping.get() && sleeping.compareAndSet(true, false) then LockSupport.unpark(thread)

  private def takeBatch(): Unit = {
    var batch = incoming.getAndSet(null)
    while batch != null do {
      val next = batch.next
      batch.next = ready
      ready = batch
      batch = next
    }
  }

  private def commands(): Unit = {
    var budget = 32
    while budget > 0 do {
      if ready == null then takeBatch()
      if ready == null then return
      val command = ready.nn
      ready = command.next
      command.next = null // No further access to next: a timer may publish its node again after dequeued().
      budget -= 1
      try
        command match {
          case task: TimerTask =>
            task.dequeued()
            if !task.isDone then {
              if task.isCancelled then {
                timers.remove(task)
                task.finish()
              } else timers.update(task, task.refreshDeadline())
            }
          case command: RunCommand => command.body.run()
        }
      catch case NonFatal(error) => report(error)
    }
  }

  private def expired(): Unit = {
    var budget = 32
    while budget > 0 && !timers.isEmpty && timers.firstDeadline - System.nanoTime() <= 0 do {
      val task = timers.first
      timers.remove(task)
      budget -= 1
      val due = task.refreshDeadline()
      if task.isCancelled || due - System.nanoTime() <= 0 then {
        try task.finish()
        catch case NonFatal(error) => report(error)
      } else timers.update(task, due) // A later move deliberately left the old, earlier heap key in place.
    }
  }

  private def idle(): Unit = {
    // Publish intent, then recheck publication. A producer racing the recheck leaves an unpark permit.
    sleeping.set(true)
    if !stopping && ready == null && incoming.get() == null then {
      if timers.isEmpty then LockSupport.park(this)
      else {
        val left = timers.firstDeadline - System.nanoTime()
        if left > 0 then LockSupport.parkNanos(this, left)
      }
    }
    sleeping.set(false)
  }

  private def loop(): Unit = {
    try
      while !stopping do {
        commands()
        expired()
        idle()
      }
    finally {
      // The scheduler normally closes after its structured scope is empty.
      // Also release timers left by direct worker users, including unpublished-to-heap registrations.
      if ready == null then takeBatch()
      while ready != null || incoming.get() != null do {
        if ready == null then takeBatch()
        val command = ready.nn
        ready = command.next
        command.next = null
        command match {
          case task: TimerTask =>
            timers.remove(task)
            task.abort()
          case _ => ()
        }
      }
      while !timers.isEmpty do {
        val task = timers.first
        timers.remove(task)
        task.abort()
      }
    }
  }

  private def report(error: Throwable): Unit = thread.getUncaughtExceptionHandler().uncaughtException(thread, error)

  override def close(): Unit = {
    stopping = true
    wake()
    if Thread.currentThread() ne thread then thread.join()
  }
}
