package asyncio.unsafe

import asyncio.Slot

/** A blocking task run by `Blockers` off a reactor's thread, for a backend's blocking ops: `block` is the body, and
  * `settled` tells the backend it has returned. Cancelling it skips `block` if it has not started, and interrupts the
  * worker thread if it is running. Interruption is cooperative: blocking JDK calls such as `Thread.sleep` throw
  * `InterruptedException`, and long computations should check `Thread.interrupted()`. Native calls like `getaddrinfo`
  * cannot be interrupted and run to the end, with their result discarded. Either way the completion's `onCancel`
  * waits until `block` has returned, since until then it may still write to the op's buffers and slots.
  *
  * Writes made before the cancel are allowed, as with a partly filled buffer, but none may follow it: a subclass must
  * write its final result into its slot through `publish`, which skips the write once cancelled.
  */
trait BlockingRun {
  def block(): Unit

  /** `block` returned, with `failure` if it threw. A cancelled task is detached, so neither outcome reaches its
    * completion, but the reactor learns from it that `block` has returned, which is when its `onCancel` runs.
    */
  protected def settled(failure: Throwable | Null): Unit

  // Guarded by `this`: the worker running `block`, whether the task was cancelled, and whether `block` has returned.
  private var runner: Thread | Null = null
  private var cancelled = false
  private var ended = false

  /** Called by the worker before `block`; false if the task was cancelled before it started. */
  private[unsafe] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Called by the worker after `block`. Once this returns no cancel can interrupt the worker, so any interrupt that
    * raced in is cleared and cannot leak into the worker's next task.
    */
  private[unsafe] def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted()
  }

  /** Marks the task cancelled and interrupts its worker if it is running. Returns true if it was running, so `block`
    * may still touch the op's buffers and slots until it returns: the worker reports that through `settled`, and
    * `awaitEnd` waits for it.
    */
  def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  /** Writes `value` into `into` unless the task has been cancelled. It holds the lock `interrupt` takes, so the write
    * either happens entirely before the cancel or not at all.
    */
  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then into.set(value)
  }

  /** Waits until a running `block` has returned. An interrupt of the waiting thread is kept for later. */
  def awaitEnd(): Unit = {
    var interrupted = false
    synchronized {
      while !ended do
        try wait()
        catch case _: InterruptedException => interrupted = true
    }
    if interrupted then Thread.currentThread().interrupt()
  }

  private[unsafe] def settle(failure: Throwable | Null): Unit = settled(failure)
}

/** The blocker pool: a few daemon threads that run blocking tasks off the reactor threads, shared by every backend. */
object Blockers {
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Runnable]()
  private lazy val threads = (1 to 4).map { i =>
    val thread = new Thread(
      () =>
        while true do {
          Thread.interrupted() // Never start a task with a stale interrupt.
          try queue.take().run()
          catch case _: InterruptedException => () // A late interrupt must not end the worker.
        },
      s"reactor-blocker-$i"
    )
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Runs `task` on the pool and settles it with the outcome, unless it is cancelled first. */
  def run(task: BlockingRun): Unit = {
    threads
    queue.put { () =>
      if task.begin() then {
        var failure: Throwable | Null = null
        try task.block()
        catch case t: Throwable => failure = t
        finally task.end()
        task.settle(failure)
      }
    }
  }
}
