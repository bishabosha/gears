package gears.async.io
package kqueue

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Future

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.util.concurrent.CancellationException
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Failure

/** Reads and writes that find their pipe not ready: they wait for the worker's kqueue to report it, and stop waiting
  * when their time limit passes, they are cancelled, or the handle closes.
  */
class KqueueReadinessSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def onPool[T](workers: Int)(program: Io[KqueueBackend.type] ?=> Async.Spawn ?=> T): T = {
    val pool = KqueueScheduler(workers)
    given poolSupport: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given poolScheduler: pool.Scheduler = pool
    try Async.blocking(Future(program(using pool)).await)
    finally pool.shutdown()
  }

  private def pipe()(using io: Io[KqueueBackend.type], async: Async): (KqueueReadable, KqueueWritable) = {
    val read = Slot[KqueueReadable]()
    val write = Slot[KqueueWritable]()
    io.perform(KqueueOps.pipe(read, write))
    (read.clear(), write.clear())
  }

  private def bytes(text: String): ByteBuffer = ByteBuffer.wrap(text.getBytes("UTF-8"))

  private def text(buf: ByteBuffer): String = {
    val got = buf.duplicate()
    got.flip()
    val out = new Array[Byte](got.remaining())
    got.get(out)
    new String(out, "UTF-8")
  }

  for workers <- List(1, 4) do
    test(s"a read on an empty pipe waits for a write ($workers workers)") {
      onPool(workers) { io ?=>
        val (in, out) = pipe()
        val buf = ByteBuffer.allocate(64)
        val reader = Future(io.perform(KqueueOps.read(in, buf)))
        AsyncOperations.sleep(50)
        assert(reader.poll().isEmpty)
        io.perform(KqueueOps.write(out, bytes("hello")))
        reader.await
        assertEquals(text(buf), "hello")
        io.perform(KqueueOps.close(in))
        io.perform(KqueueOps.close(out))
      }
    }

    test(s"a waiting read ends, reading nothing, when the writer closes ($workers workers)") {
      onPool(workers) { io ?=>
        val (in, out) = pipe()
        val buf = ByteBuffer.allocate(64)
        val reader = Future(io.perform(KqueueOps.read(in, buf)))
        AsyncOperations.sleep(50)
        io.perform(KqueueOps.close(out))
        reader.await
        assertEquals(buf.position(), 0)
        io.perform(KqueueOps.close(in))
      }
    }

    test(s"a waiting read fails with a closed channel's exception when its handle closes ($workers workers)") {
      onPool(workers) { io ?=>
        val (in, out) = pipe()
        val reader = Future(io.perform(KqueueOps.read(in, ByteBuffer.allocate(64))))
        AsyncOperations.sleep(50)
        io.perform(KqueueOps.close(in))
        reader.awaitResult match
          case Failure(_: ClosedChannelException) => ()
          case other                              => fail(s"not a closed channel: $other")
        io.perform(KqueueOps.close(out))
      }
    }

    test(s"a cancelled read stops waiting, and its handle reads on ($workers workers)") {
      onPool(workers) { io ?=>
        val (in, out) = pipe()
        val reader = Future(io.perform(KqueueOps.read(in, ByteBuffer.allocate(64))))
        AsyncOperations.sleep(50)
        reader.cancel()
        reader.awaitResult match
          case Failure(_: CancellationException) => ()
          case other                             => fail(s"not cancelled: $other")
        io.perform(KqueueOps.write(out, bytes("later")))
        val buf = ByteBuffer.allocate(64)
        io.perform(KqueueOps.read(in, buf))
        assertEquals(text(buf), "later")
        io.perform(KqueueOps.close(in))
        io.perform(KqueueOps.close(out))
      }
    }

  test("a read that times out read nothing, and the same op reads once there is something") {
    onPool(2) { io ?=>
      val (in, out) = pipe()
      val buf = ByteBuffer.allocate(64)
      val read = KqueueOps.read(in, buf)
      intercept[TimeoutException](io.perform(read, 50.millis))
      assertEquals(buf.position(), 0)
      val reader = Future(io.perform(read, 10.seconds))
      AsyncOperations.sleep(50)
      io.perform(KqueueOps.write(out, bytes("again")))
      reader.await
      assertEquals(text(buf), "again")
      io.perform(KqueueOps.close(in))
      io.perform(KqueueOps.close(out))
    }
  }

  test("a write to a full pipe waits for a read to make room") {
    onPool(2) { io ?=>
      val (in, out) = pipe()
      val chunk = new Array[Byte](4096)
      // Fills the pipe: a write that can't complete at once times out, having written nothing.
      var written = 0L
      var full = false
      while !full do
        val buf = ByteBuffer.wrap(chunk)
        try
          io.perform(KqueueOps.write(out, buf), 50.millis)
          written += buf.position()
        catch case _: TimeoutException => full = true
      assert(written > 0)
      val pending = ByteBuffer.wrap(chunk)
      val writer = Future(io.perform(KqueueOps.write(out, pending)))
      AsyncOperations.sleep(50)
      assert(writer.poll().isEmpty)
      var drained = 0L
      while drained < written do
        val buf = ByteBuffer.allocate(65536)
        io.perform(KqueueOps.read(in, buf))
        drained += buf.position()
      writer.await
      assert(pending.position() > 0)
      io.perform(KqueueOps.close(in))
      io.perform(KqueueOps.close(out))
    }
  }

  test("many reads and writes pass through a pipe in order") {
    onPool(4) { io ?=>
      val (in, out) = pipe()
      val total = 200_000
      val writer = Future {
        val buf = ByteBuffer.allocate(total)
        var i = 0
        while i < total do { buf.put((i % 251).toByte); i += 1 }
        buf.flip()
        val write = KqueueOps.write(out, buf)
        while buf.hasRemaining() do io.perform(write)
        io.perform(KqueueOps.close(out))
      }
      val got = ByteBuffer.allocate(total + 1)
      val read = KqueueOps.read(in, got)
      var ended = false
      while !ended do
        val before = got.position()
        io.perform(read)
        ended = got.position() == before
      writer.await
      assertEquals(got.position(), total)
      var i = 0
      while i < total do { assertEquals(got.get(i), (i % 251).toByte); i += 1 }
      io.perform(KqueueOps.close(in))
    }
  }
}
