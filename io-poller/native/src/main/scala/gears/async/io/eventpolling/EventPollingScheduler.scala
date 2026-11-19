package gears.async.io.eventpolling

import gears.async.Async
import gears.async.io.shareable.HandleOwner
import gears.async.io.shareable.Starvation
import gears.async.io.shareable.Worker
import gears.async.io.shareable.WorkpoolScheduler
import gears.async.native.NativeSuspend

/** A pool of `workers` workers, each with an event poller of the system's. A worker kept from its tasks is reported to
  * `onStarvation` (see `Starvation`). Which poller is its subclass's: `KqueueScheduler` or `EpollScheduler`.
  */
abstract class EventPollingScheduler(workers: Int, onStarvation: Starvation.Handler)
    extends WorkpoolScheduler[EventPollingOps.type](EventPollingBackend, EventPollingOps, workers, onStarvation),
      NativeSuspend {
  override protected def selectWorker() = workerOfCurrentThread()

  override def makeWorker(id: Int, owner: HandleOwner): EventPollingWorker

  /** A worker of this backend's, whatever its poller is. */
  abstract class EventPollingWorker(id: Int, owner: HandleOwner) extends Worker(id, owner), EventPoller {
    type Op = EventPollingOp

    /** either schedules a waiter (false), or completes immediately (true) */
    override def performOp(op: EventPollingOp)(using Async): Unit = {
      op match
        case timer: EventPollingMakeTimer =>
          timer.holdTimerOn(this)
          timer.parkUntilSettled()
        case deadline: EventPollingMakeDeadline =>
          deadline.holdTimerOn(this)
          deadline.parkUntilSettled()
        case close: EventPollingCloseOp         => close.performClose(this)
        case ready: EventPollingReadinessOp     => ready.performWhenReady(this)
        case immediate: EventPollingImmediateOp => immediate.perform(handleOwner)
        case work: EventPollingOffThreadOp      => work.performInPlace(this)
        case connect: EventPollingConnectOp     => connect.performConnect(this)
    }
  }
}

object EventPollingScheduler {

  /** The pool for the system the program is linked for (see `EventPollingBackend.scheduler`). */
  def apply(workers: Int, onStarvation: Starvation.Handler = Starvation.printWarning): EventPollingScheduler =
    EventPollingBackend.scheduler(workers, onStarvation)
}
