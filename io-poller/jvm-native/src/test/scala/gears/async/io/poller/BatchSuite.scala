package gears.async.io
package poller

import gears.async.Async
import gears.async.Future
import gears.async.io.flow.Batch

import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

/** Batches: a queue of ops performed together by one future, with one wait. */
class BatchSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def pipe(io: Io[Pipes])(using Async): (io.ops.Readable, io.ops.Writable) = {
    val read = Slot[io.ops.Readable]()
    val write = Slot[io.ops.Writable]()
    io.perform(io.ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  test("an op that is ready is done by the perform that begins it, and one that waits by a later one") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val (idleIn, idleOut) = pipe(io)
      val sent = ByteBuffer.allocateDirect(8)
      val got = ByteBuffer.allocateDirect(8)
      val never = ByteBuffer.allocateDirect(8)
      val write = io.ops.write(out, sent)
      val read = io.ops.read(in, got)
      val waits = io.ops.read(idleIn, never)
      val batch = Batch(io)
      batch.add(waits, "waits")
      batch.add(read, "read")
      batch.add(write, "write")
      // The write is done as it is begun; the read was begun before it, and waits.
      assertEquals(batch.perform(), 1)
      assert(batch.op(0) eq write)
      assertEquals(batch.note(0), "write")
      assertEquals(batch.size, 2)
      assertEquals(batch.perform(), 1)
      assertEquals(batch.note(0), "read")
      assertEquals(got.position(), 8)
      assertEquals(batch.size, 1)
      batch.close()
      assertEquals(batch.size, 0)
      assertEquals(never.position(), 0)
      // Closed on, the op is its performer's again.
      io.perform(io.ops.write(idleOut, sent.clear()))
      io.perform(waits)
      assertEquals(never.position(), 8)
    }
  }

  test("one future echoes over many pipes, a perform at a time") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    val rounds = 2_000
    val many = 16
    Io.run { io ?=>
      final class Side(val from: Int, val buf: ByteBuffer, val read: io.Op, val write: io.Op, var left: Int)
      val batch = Batch(io)
      val writers = for i <- 0 until many yield
        val (requestIn, requestOut) = pipe(io)
        val (replyIn, replyOut) = pipe(io)
        val buf = ByteBuffer.allocateDirect(8)
        val side = new Side(i, buf, io.ops.read(requestIn, buf), io.ops.write(replyOut, buf), rounds)
        batch.add(side.read, side)
        Future {
          val out = ByteBuffer.allocateDirect(8)
          val in = ByteBuffer.allocateDirect(8)
          var wrong = 0
          var n = 0L
          while n < rounds do
            out.clear()
            out.putLong(0, n * many + i)
            io.perform(io.ops.write(requestOut, out))
            in.clear()
            while in.hasRemaining() do io.perform(io.ops.read(replyIn, in))
            if in.getLong(0) != n * many + i then wrong += 1
            n += 1
          wrong
        }
      var live = many
      var performs = 0
      while live > 0 do
        val done = batch.perform()
        performs += 1
        assert(done > 0)
        var i = 0
        while i < done do
          assertEquals(batch.failure(i), null)
          val side = batch.note(i).asInstanceOf[Side]
          if batch.op(i) eq side.read then
            if side.buf.hasRemaining() then batch.add(side.read, side)
            else
              side.buf.flip()
              batch.add(side.write, side)
          else if side.buf.hasRemaining() then batch.add(side.write, side)
          else
            side.left -= 1
            if side.left == 0 then live -= 1
            else
              side.buf.clear()
              batch.add(side.read, side)
          i += 1
      batch.close()
      val wrong = writers.map(_.await).sum
      assertEquals(wrong, 0)
    }
  }

  test("an op's failure and its time limit are told with it, and the others go on") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val batch = Batch(io)
      val slow = io.ops.timer(60_000)
      val quick = io.ops.timer(30)
      batch.add(slow, "slow", 10.millis)
      batch.add(quick, "quick")
      assertEquals(batch.perform(), 1)
      assertEquals(batch.note(0), "slow")
      assert(batch.failure(0).isInstanceOf[TimeoutException])
      assertEquals(batch.perform(), 1)
      assertEquals(batch.note(0), "quick")
      assertEquals(batch.failure(0), null)
      assertEquals(batch.perform(), 0) // empty
    }
  }

  test("cancelling the future suspended in a perform throws there, and its close ends the ops") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val slow = io.ops.timer(60_000)
      @volatile var closed = false
      val running = Future {
        val batch = Batch(io)
        batch.add(slow)
        try batch.perform()
        finally
          batch.close()
          closed = batch.size == 0
      }
      io.perform(io.ops.timer(30))
      running.cancel()
      val outcome = running.awaitResult // outside the assertion, which holds a monitor over its condition
      assert(outcome.failed.toOption.exists(_.isInstanceOf[CancellationException]))
      assert(closed)
      io.perform(io.ops.timer(1)) // the run ends with nothing of the batch's left behind
    }
  }

  test("a batch whose ops are always ready still gives its worker to other futures") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val buf = ByteBuffer.allocateDirect(1)
      val write = io.ops.write(out, buf)
      val read = io.ops.read(in, buf)
      @volatile var stop = false
      val busy = Future {
        val batch = Batch(io)
        var rounds = 0
        buf.clear()
        batch.add(write)
        while !stop do
          batch.perform()
          buf.clear()
          batch.add(if batch.op(0) eq write then read else write)
          rounds += 1
        batch.close()
        rounds
      }
      val others = List.fill(8)(Future(io.perform(io.ops.timer(20))))
      others.foreach(_.await)
      stop = true
      val rounds = busy.await
      assert(rounds > 0)
    }
  }
}
