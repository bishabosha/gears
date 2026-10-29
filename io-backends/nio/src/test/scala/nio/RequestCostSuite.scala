package gears.async.io.nio

import gears.async.io.internal.*

class RequestCostSuite extends RequestCostContract {
  val backend: NioBackend.type = NioBackend
  protected def prepareRead(request: ReadRequest[backend.Readable]): NioOp & gears.async.io.Repeatable =
    NioRequestInterpreter.prepare(request)
  protected def prepareTimer(request: TimerRequest): NioOp & gears.async.io.Repeatable =
    NioRequestInterpreter.prepare(request)

  @volatile private var escaped: AnyRef = this

  test("report request and execution construction allocation") {
    val bean = java.lang.management.ManagementFactory.getThreadMXBean
      .asInstanceOf[com.sun.management.ThreadMXBean]
    assume(bean.isThreadAllocatedMemorySupported)
    bean.setThreadAllocatedMemoryEnabled(true)
    val reactor = backend.open()
    val in = gears.async.io.Slot[backend.Readable]()
    val out = gears.async.io.Slot[backend.Writable]()
    reactor.submit(
      backend.ops.pipe(in, out),
      new Completion[NioOp] {
        def onComplete(op: NioOp): Unit = ()
        override def onFailure(op: NioOp, failure: Throwable): Unit = throw failure
      }
    )
    val handle = in.clear()
    val buffer = java.nio.ByteBuffer.allocate(1)
    val request = ReadRequest(handle, buffer)
    try {
      val factories = List[(String, () => AnyRef)](
        "intrusive" -> (() => backend.ops.read(handle, buffer)),
        "description" -> (() => ReadRequest(handle, buffer)),
        "execution" -> (() => prepareRead(request)),
        "description+execution" -> (() => prepareRead(ReadRequest(handle, buffer)))
      )
      def construct(make: () => AnyRef, count: Int): Unit = {
        var i = 0
        while i < count do {
          escaped = make()
          i += 1
        }
      }
      for (name, make) <- factories do {
        construct(make, 500000)
        val id = Thread.currentThread().threadId()
        val before = bean.getThreadAllocatedBytes(id)
        construct(make, 200000)
        val bytes = bean.getThreadAllocatedBytes(id) - before
        println(f"request-allocation $name: ${bytes.toDouble / 200000}%.1f bytes/construction")
      }
    } finally {
      val completion = new Completion[NioOp] { def onComplete(op: NioOp): Unit = () }
      reactor.submit(backend.ops.close(handle), completion)
      reactor.submit(backend.ops.close(out.clear()), completion)
      reactor.close()
    }
  }
}
