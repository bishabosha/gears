package asyncio.gears

import gears.async.Scheduler

import asyncio.ReactorBackend
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
  * caller. Always a `ReactorPool`, whose workers drive the reactors. A program on the pool reaches them through its
  * scheduler, which the pool is; one on gears' own scheduler, through a pool whose workers carry no futures
  * (`ReactorPool.start`), which its runner set up and left in `OpRouter.ambient`.
  */
private[gears] trait OpRouter {
  def backend: ReactorBackend

  /** The worker running the caller, whose reactor the caller may use directly, or null if it runs on none. */
  def currentWorker: ReactorWorker | Null

  /** Where an op goes from `caller`, which runs on no worker: always the same driver for the same thread, so that a
    * future's ops stay together.
    */
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
