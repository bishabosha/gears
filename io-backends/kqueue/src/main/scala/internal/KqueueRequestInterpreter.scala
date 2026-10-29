package gears.async.io.internal

import gears.async.io.kqueue.{Command, KqueueBackend, KqueueKinds, KqueueOp}

/** A kqueue read's execution retains the immutable request, its lowered descriptor and the last attempt's readiness
  * information. All reusable lifecycle state remains in the execution, with no state stored in the request.
  */
private[io] object KqueueRequestInterpreter extends RequestInterpreter[KqueueBackend.type] {
  val backend: KqueueBackend.type = KqueueBackend

  def prepare(request: ReadRequest[backend.Readable]): KqueueOp & gears.async.io.Repeatable = new ReadExecution(request)
  def prepare(request: TimerRequest): KqueueOp & gears.async.io.Repeatable = backend.ops.timer(request.duration)

  private final class ReadExecution(val request: ReadRequest[KqueueKinds.Readable]) extends Command {
    val fd: Int = super.bindHandle(request.handle)
    def interest: Interest = Interest.Read
    override def refusal: Throwable | Null =
      if request.buffer.hasRemaining() then null else Checks.bufferFull(request.buffer)
    private var read = 0
    private var filled = false
    def perform(): Boolean = {
      read = NonBlocking.read(fd, request.buffer)
      filled = !request.buffer.hasRemaining()
      read != 0
    }
    override def transferred: Int = read
    override def mayStillBeReady: Boolean = read < 0 || filled
    override def edgeTriggered: Boolean = true
  }
}
