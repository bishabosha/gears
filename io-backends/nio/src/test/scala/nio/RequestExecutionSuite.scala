package gears.async.io.nio

import gears.async.io.internal.{NioRequestInterpreter, ReactorRuns, ReadRequest, TimerRequest}
import gears.async.io.testkit.RequestExecutionContract

class RequestExecutionSuite extends RequestExecutionContract {
  val backend: NioBackend.type = NioBackend
  protected def prepareRead(request: ReadRequest[backend.Readable]): NioOp & gears.async.io.Repeatable =
    NioRequestInterpreter.prepare(request)
  protected def prepareTimer(request: TimerRequest): NioOp & gears.async.io.Repeatable =
    NioRequestInterpreter.prepare(request)

  test("request preparation preserves the backend's readable handle kind") {
    assert(compileErrors("def bad(r: ReadRequest[NioBackend.Writable]) = NioRequestInterpreter.prepare(r)").nonEmpty)
  }

  test("data-only submission hides its fresh execution record") {
    ReactorRuns.run(1)(using backend) { io ?=>
      NioRequestInterpreter.perform(TimerRequest(scala.concurrent.duration.Duration.Zero))
      val in = gears.async.io.Slot[backend.Readable]()
      val out = gears.async.io.Slot[backend.Writable]()
      io.perform(backend.ops.pipe(in, out))
      val read = in.clear()
      val write = out.clear()
      try {
        val buffer = java.nio.ByteBuffer.allocate(1)
        val request = ReadRequest(read, buffer)
        for n <- 1 to 2 do {
          buffer.clear()
          io.perform(backend.ops.write(write, java.nio.ByteBuffer.wrap(Array(n.toByte))))
          NioRequestInterpreter.perform(request)
          assertEquals(buffer.get(0), n.toByte)
        }
      } finally {
        io.perform(backend.ops.close(read))
        io.perform(backend.ops.close(write))
      }
    }
  }
}
