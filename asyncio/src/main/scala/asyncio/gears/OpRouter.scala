package asyncio.gears

import gears.async.Scheduler

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch

import asyncio.Backend
import asyncio.Reactor

/** A reactor and the thread that drives it: where an op is submitted, and where it is cancelled. */
private[gears] trait ReactorDriver {
  def reactor: Reactor

  /** Whether the caller runs on this driver's thread, so it may use the reactor directly. */
  def isCurrent: Boolean

  /** Runs `body` on this driver's thread: now if called there, or queued and the reactor woken otherwise. */
  def sync(body: => Unit): Unit
}

/** Where `ReactorAsync.perform` sends the ops of a program: the reactors of a backend, and how to reach them from the
  * caller. A program on a `ReactorPool` reaches them through its scheduler; one on gears' own scheduler, through the
  * reactor threads its runner set up (`OpRouter.ambient`).
  */
private[gears] trait OpRouter {
  def backend: Backend

  /** The worker running the caller, whose reactor the caller may use directly, or null if it runs on none. */
  def currentWorker: ReactorWorker | Null

  /** Where an op goes from `caller`, which runs on no worker: always the same driver for the same thread. */
  def driverFor(caller: Thread): ReactorDriver
}

private[gears] object OpRouter {

  /** The reactors of a program whose futures run on a scheduler that does not know them, such as gears' virtual
    * threads: set by the runner around the program, and inherited by the threads it starts, futures among them.
    */
  val ambient = new InheritableThreadLocal[OpRouter | Null]
}

/** A gears scheduler whose futures can perform ops on its reactors. */
private[gears] trait ReactorScheduler extends Scheduler with OpRouter

/** A platform thread driving a reactor of `backend`'s for callers on other threads, which hand it ops with `sync`. */
private[gears] final class ReactorThread(backend: Backend, index: Int, started: CountDownLatch)
    extends ReactorDriver {
  private val inbox = new ConcurrentLinkedQueue[Runnable]()
  @volatile private var opened: Reactor | Null = null
  @volatile private var stopped = false

  private val thread = new Thread(() => loop(), s"reactor-$index")
  thread.setDaemon(true)
  thread.start()

  def reactor: Reactor = opened.nn

  def isCurrent: Boolean = Thread.currentThread() eq thread

  def sync(body: => Unit): Unit =
    if isCurrent then body
    else {
      inbox.add(() => body)
      reactor.wakeup() // which a backend may make cheap while the reactor is awake, as NIO does
    }

  /** Opens the reactor here, where it is owned, then runs the tasks handed to it between turns, blocking in the
    * reactor while there are none, until stopped; then closes it.
    */
  private def loop(): Unit = {
    val r = backend.open()
    opened = r
    started.countDown()
    try
      while !stopped do {
        var task = inbox.poll()
        var left = ReactorThread.TasksPerTurn
        while task != null do {
          try task.run()
          catch case t: Throwable => t.printStackTrace()
          left -= 1
          task = if left > 0 then inbox.poll() else null
        }
        // A task handed over after this check wakes the reactor, so the turn does not block for it.
        r.runOnce(block = inbox.isEmpty)
      }
    finally r.close() // Pending ops are cancelled, rejecting any futures still waiting on them.
  }

  def stop(): Unit = {
    stopped = true
    reactor.wakeup() // whether parked or not: it may be about to block
  }

  def join(): Unit = thread.join()
}

private[gears] object ReactorThread {

  /** How many handed-over tasks a reactor thread runs between turns, at most. */
  inline val TasksPerTurn = 256

}

/** `workers` reactor threads for a program on gears' own scheduler (see `OpRouter`). Each caller's ops go to the
  * same one, picked by its thread, so a future's ops stay together.
  */
private[gears] final class ReactorThreads(val backend: Backend, workers: Int) extends OpRouter {
  require(workers >= 1, "At least one reactor thread is needed")

  private val threads = {
    val started = new CountDownLatch(workers)
    val all = Array.tabulate(workers)(i => new ReactorThread(backend, i, started))
    started.await() // every reactor is open before any op is handed over
    all
  }

  def currentWorker: ReactorWorker | Null = null

  def driverFor(caller: Thread): ReactorDriver = threads(Math.floorMod(System.identityHashCode(caller), workers))

  /** Stops the threads, which close their reactors, and waits for them. */
  def stop(): Unit = {
    threads.foreach(_.stop())
    threads.foreach(_.join())
  }
}
