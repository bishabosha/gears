package gears.async.io
package poller

import gears.async.Async
import gears.async.Future
import gears.async.io.flow.Streams
import gears.async.io.flow.Streams.*

import java.io.EOFException
import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import scala.concurrent.duration.*

/** Streams: many byte streams served by one future, a message at a time. */
class StreamsSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def pipe(io: Io[Pipes])(using Async): (io.ops.Readable, io.ops.Writable) = {
    val read = Slot[io.ops.Readable]()
    val write = Slot[io.ops.Writable]()
    io.perform(io.ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  test("one future serves an echo to many clients") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    val rounds = 2_000
    val many = 16
    Io.run { io ?=>
      final class Session(val buf: ByteBuffer, var left: Int)
      val streams = Streams[Session](io)
      val clients = for i <- 0 until many yield
        val (requestIn, requestOut) = pipe(io)
        val (replyIn, replyOut) = pipe(io)
        val session = new Session(ByteBuffer.allocateDirect(8), rounds)
        streams.open(requestIn, replyOut, session).receive(session.buf)
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
      while live > 0 do
        streams.next() match
          case Received(stream) => stream.send(stream.note.buf)
          case Sent(stream)     =>
            stream.note.left -= 1
            if stream.note.left == 0 then live -= 1 else stream.receive(stream.note.buf)
          case other => fail(s"unexpected: $other")
      assertEquals(streams.pending, 0)
      assertEquals(streams.next(), null)
      streams.close()
      val wrong = clients.map(_.await).sum
      assertEquals(wrong, 0)
    }
  }

  test("a receive is one event however many writes fill it, and a send and a receive may be under way at once") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val streams = Streams[String](io)
      val stream = streams.open(in, out, "loop")
      val four = ByteBuffer.allocateDirect(4)
      val one = ByteBuffer.allocateDirect(1)
      stream.receive(four)
      var sent = 0
      var got = false
      one.put(0, sent.toByte)
      stream.send(one)
      while !got do
        streams.next() match
          case Sent(s) =>
            assertEquals(s.note, "loop")
            sent += 1
            if sent < 4 then
              one.clear()
              one.put(0, sent.toByte)
              s.send(one)
          case Received(_) => got = true
          case other       => fail(s"unexpected: $other")
      assertEquals(sent, 4)
      assertEquals(four.remaining(), 4)
      assertEquals(four.getInt(0), 0x00010203)
      intercept[IllegalStateException] { stream.receive(four); stream.receive(four) }
      streams.close()
    }
  }

  test("the stream's end is Ended where a receive began, and a failure part way through one") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val (in2, out2) = pipe(io)
      val one = ByteBuffer.allocateDirect(1)
      io.perform(io.ops.write(out, one))
      io.perform(io.ops.close(out))
      io.perform(io.ops.close(out2))
      val streams = Streams[String](io)
      streams.open(in, out, "short").receive(ByteBuffer.allocateDirect(4))
      streams.open(in2, out2, "empty").receive(ByteBuffer.allocateDirect(4))
      var seen = Set.empty[String]
      while streams.pending > 0 do
        streams.next() match
          case Ended(stream)                   => seen += s"ended ${stream.note}"
          case Failed(stream, _: EOFException) => seen += s"failed ${stream.note}"
          case other                           => fail(s"unexpected: $other")
      assertEquals(seen, Set("ended empty", "failed short"))
      streams.close()
    }
  }

  test("cancelling the future waiting in next throws there, and close ends what was under way") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      @volatile var left = -1
      val running = Future {
        val streams = Streams[String](io)
        streams.open(in, out, "idle").receive(ByteBuffer.allocateDirect(4))
        try streams.next()
        finally
          streams.close()
          left = streams.pending
      }
      io.perform(io.ops.timer(30))
      running.cancel()
      val outcome = running.awaitResult // outside the assertion, which holds a monitor over its condition
      assert(outcome.failed.toOption.exists(_.isInstanceOf[CancellationException]))
      assertEquals(left, 0)
    }
  }
}
