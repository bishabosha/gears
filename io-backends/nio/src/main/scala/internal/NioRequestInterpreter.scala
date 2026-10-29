package gears.async.io.internal

import gears.async.io.nio.{Command, NioBackend, NioKinds, NioOp}

/** The read execution retains its description, making the extra data indirection explicit. Timers lower their duration
  * into the backend's existing timer record. Neither preparation path creates a promise or callback adapter.
  */
private[io] object NioRequestInterpreter extends RequestInterpreter[NioBackend.type] {
  val backend: NioBackend.type = NioBackend

  def prepare(request: ReadRequest[backend.Readable]): NioOp & gears.async.io.Repeatable = new ReadExecution(request)
  def prepare(request: TimerRequest): NioOp & gears.async.io.Repeatable = backend.ops.timer(request.duration)

  private final class ReadExecution(val request: ReadRequest[NioKinds.Readable]) extends Command {
    def handle: NioBackend.Handle = request.handle
    def interest: Interest = Interest.Read
    override private[io] def refusal: Throwable | Null =
      if request.buffer.hasRemaining() then null else Checks.bufferFull(request.buffer)
    def perform(): Boolean = super.readBuffer(request.buffer) != 0
  }
}
