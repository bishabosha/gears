package gears.async.io.kqueue

import gears.async.native.NativeSuspend
import gears.async.io.shareable.WorkpoolScheduler

class KqueueScheduler(workers: Int) extends WorkpoolScheduler(KqueueBackend, workers) with NativeSuspend {
  override def suspend[T, R](body: Suspension[T, R] => R)(using Label[R]): T =
    println("Suspending Kqueue...")
    super.suspend(body)

  override def makeWorker(id: Int): KqueueWorker = new KqueueWorker(id)

  class KqueueWorker(id: Int) extends WorkpoolScheduler.Worker(id, KqueueBackend) {

    override def schedule(op: KqueueBackend.Op): Unit = {
      op match
        case timer: KqueueMakeTimer =>
          addTimer(timer)
        case deadline: KqueueMakeDeadline =>
          addTimer(deadline)
    }
  }
}
