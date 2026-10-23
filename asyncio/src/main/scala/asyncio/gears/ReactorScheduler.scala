package asyncio.gears

import gears.async.Scheduler

import asyncio.ReactorBackend

/** A gears scheduler whose futures run on a `ReactorPool`'s workers, and so perform their ops on the workers' own
  * reactors (see `ReactorAsync.performOnReactor`): the pool itself, as on Scala Native, or a scheduler of virtual
  * threads the pool carries (`VirtualReactorScheduler`).
  */
private[gears] trait ReactorScheduler extends Scheduler {
  def backend: ReactorBackend

  /** The worker running the caller, whose reactor the caller may use directly, or null if it runs on none. */
  def currentWorker: ReactorWorker | Null

  /** Keeps the caller on the thread `currentWorker` names until `unpin`, so that an op found a worker is submitted on
    * that worker's thread: nothing to do where a caller cannot change thread, as on Scala Native; on the JVM, a virtual
    * thread carried by a worker is pinned to it (see `VirtualCarriers.pin`).
    */
  def pin(): Unit = ()

  def unpin(): Unit = ()
}
