package gears.async.io.kqueue

import gears.async.io.internal.*

class RequestCostSuite extends RequestCostContract {
  val backend: KqueueBackend.type = KqueueBackend
  protected def prepareRead(request: ReadRequest[backend.Readable]): KqueueOp & gears.async.io.Repeatable =
    KqueueRequestInterpreter.prepare(request)
  protected def prepareTimer(request: TimerRequest): KqueueOp & gears.async.io.Repeatable =
    KqueueRequestInterpreter.prepare(request)
}
