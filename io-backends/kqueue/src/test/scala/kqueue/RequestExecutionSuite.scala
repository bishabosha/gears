package gears.async.io.kqueue

import gears.async.io.internal.{KqueueRequestInterpreter, ReadRequest, TimerRequest}
import gears.async.io.testkit.RequestExecutionContract

class RequestExecutionSuite extends RequestExecutionContract {
  val backend: KqueueBackend.type = KqueueBackend
  protected def prepareRead(request: ReadRequest[backend.Readable]): KqueueOp & gears.async.io.Repeatable =
    KqueueRequestInterpreter.prepare(request)
  protected def prepareTimer(request: TimerRequest): KqueueOp & gears.async.io.Repeatable =
    KqueueRequestInterpreter.prepare(request)
}
