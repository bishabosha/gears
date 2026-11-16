package gears.async.io
package testkit

import gears.async.Async
import gears.async.Future

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

/** Read, write, read-window and close semantics over a supplied connected byte-stream fixture. */
trait StreamContract extends StreamFixture {

  /** The bytes of `bufs` read so far, in order, as text. */
  private def readText(bufs: Array[ByteBuffer]): String =
    bufs.map { b =>
      val copy = b.duplicate()
      copy.flip()
      StandardCharsets.UTF_8.decode(copy).toString
    }.mkString

  /** A read into a buffer of `in`'s window of what `out` is sent, `text`: what it received, which the caller releases.
    */
  private def receivedOf(
      io: Io[backend.Interface],
      in: io.ops.Readable,
      out: io.ops.Writable,
      text: String
  )(using
      Async
  ): Received = {
    writeAll(io, out, bytes(text))
    val into = Slot[Received]()
    io.perform(io.ops.read(in, into))
    into.clear()
  }

  test("an op on a closed handle fails, and what reuses the handle's I/O object gets nothing of it") {
    running { io ?=>
      val (in, out) = stream(io)
      val built = io.ops.write(out, bytes("stale")) // built before the close, performed after
      close(io, in)
      close(io, out)
      val again = attempt(io, io.ops.close(out))
      assert(again.isInstanceOf[ClosedChannelException], s"closing it again: $again")
      val (in2, out2) = stream(io) // on a backend with descriptors, most likely the same two numbers
      try {
        val early = attempt(io, built)
        assert(early.isInstanceOf[ClosedChannelException], s"an op built before the close: $early")
        val late = attempt(io, io.ops.write(out, bytes("stale")))
        assert(late.isInstanceOf[ClosedChannelException], s"an op built after the close: $late")
        writeAll(io, out2, bytes("new"))
        val buf = allocate(16)
        io.perform(io.ops.read(in2, buf))
        readExactly(io, io.ops.read(in2, buf), buf, 3)
        assertEquals(text(buf), "new")
      } finally {
        close(io, in2)
        close(io, out2)
      }
    }
  }

  test("an op on a closed handle fails") {
    running { io ?=>
      val (in, out) = stream(io)
      close(io, out)
      close(io, in)
      val failure = attempt(io, io.ops.read(in, allocate(16)))
      assert(failure.isInstanceOf[ClosedChannelException], String.valueOf(failure))
    }
  }

  test("a read waits for data written later, then completes with it") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val buf = allocate(64)
        val reading = Future(io.perform(io.ops.read(in, buf)))
        settle(io)
        assert(reading.poll().isEmpty, "the read completed with nothing written")
        writeAll(io, out, bytes("hello"))
        reading.await
        readExactly(io, io.ops.read(in, buf), buf, 5)
        assertEquals(text(buf), "hello")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a read at the end of the stream completes without adding bytes") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        close(io, out)
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf))
        assertEquals(buf.position(), 0)
      } finally close(io, in)
    }
  }

  test("a read into a buffer with no room fails at once, reading nothing, whether or not bytes are waiting") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val full = allocate(4)
        full.position(full.limit())
        val read = io.ops.read(in, full)
        def refused(): Unit = {
          val failure = attempt(io, read)
          assert(failure.isInstanceOf[IllegalArgumentException], String.valueOf(failure))
        }
        refused()
        writeAll(io, out, bytes("hello"))
        refused()
        assertEquals(full.position(), full.limit())
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf))
        readExactly(io, io.ops.read(in, buf), buf, 5)
        assertEquals(text(buf), "hello", "the refused reads took none of the bytes")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a write with nothing remaining completes, writing nothing, even with the reader gone, and fails once closed") {
    running { io ?=>
      val (in, out) = stream(io)
      val empty = allocate(4)
      empty.limit(0)
      val write = io.ops.write(out, empty)
      try {
        close(io, in) // a write of bytes would fail now; one of nothing touches nothing
        io.perform(write)
        io.perform(write)
      } finally close(io, out)
      val failure = attempt(io, write)
      assert(failure.isInstanceOf[IOException], s"a closed handle fails first: $failure")
    }
  }

  test("cancelling the future performing a pending read stops it, and the op may then be performed again") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val buf = allocate(16)
        val read = io.ops.read(in, buf)
        val first = Future(io.perform(read))
        settle(io)
        first.cancel()
        val failure = failureOf(first)
        assert(failure.isInstanceOf[CancellationException], String.valueOf(failure))
        assertEquals(buf.position(), 0)
        val second = Future(io.perform(read))
        writeAll(io, out, bytes("again"))
        second.await
        readExactly(io, read, buf, 5)
        assertEquals(text(buf), "again")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a read with a time limit fails with TimeoutException when nothing comes, and may then be performed again") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val buf = allocate(16)
        val read = io.ops.read(in, buf)
        val started = System.nanoTime()
        intercept[TimeoutException](io.perform(read, 20.millis))
        assert(System.nanoTime() - started >= 20_000_000L, "the read timed out early")
        assertEquals(buf.position(), 0) // nothing was read
        val second = Future(io.perform(read, 5.seconds))
        writeAll(io, out, bytes("in time"))
        second.await
        readExactly(io, read, buf, 7)
        assertEquals(text(buf), "in time")
        // Its time limit went with it: nothing more comes of the read once it is done.
        io.perform(io.ops.timer(30))
        assertEquals(text(buf), "in time")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a repeatable op is performed again by another future") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val buf = allocate(16)
        val read = io.ops.read(in, buf)
        writeAll(io, out, bytes("a"))
        Future(io.perform(read)).await
        val second = Future(io.perform(read))
        settle(io)
        writeAll(io, out, bytes("b"))
        second.await
        readExactly(io, read, buf, 2)
        assertEquals(text(buf), "ab")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a close op completes once the handle is closed, and fails the op pending on it") {
    running { io ?=>
      val (in, out) = stream(io)
      val reading = Future(io.perform(io.ops.read(in, allocate(16))))
      settle(io)
      io.perform(io.ops.close(in))
      val failure = failureOf(reading)
      assert(failure.isInstanceOf[ClosedChannelException], String.valueOf(failure))
      val again = attempt(io, io.ops.close(in))
      assert(again.isInstanceOf[ClosedChannelException], s"closing a closed handle fails: $again")
      io.perform(io.ops.close(out))
    }
  }

  test("closing a handle another future has written to ends its peer's stream") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        Future(writeAll(io, out, bytes("x"))).await
        Future(close(io, out)).await
        val buf = allocate(16)
        assertEquals(readToEnd(io, io.ops.read(in, buf), buf), "x")
      } finally close(io, in)
    }
  }

  test("every readable handle has a window, which is its own to set while none of its buffers is held") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val window = io.ops.readWindow(in)
        assertEquals(window.bufferSize, ReadWindow.DefaultBufferSize)
        assertEquals(window.buffers, ReadWindow.DefaultBuffers)
        assertEquals(window.available, ReadWindow.DefaultBuffers)
        io.ops.setReadWindow(in, bufferSize = 16, buffers = 2)
        assertEquals((io.ops.readWindow(in).bufferSize, io.ops.readWindow(in).buffers), (16, 2))
        intercept[IllegalArgumentException](io.ops.setReadWindow(in, bufferSize = 0, buffers = 2))
        intercept[IllegalArgumentException](io.ops.setReadWindow(in, bufferSize = 16, buffers = 0))
        val held = receivedOf(io, in, out, "a")
        assertEquals(io.ops.readWindow(in).available, 1)
        intercept[IllegalStateException](io.ops.setReadWindow(in, bufferSize = 32, buffers = 4))
        assertEquals(io.ops.readWindow(in).buffers, 2, "refused, the window is as it was")
        held.release()
        io.ops.setReadWindow(in, bufferSize = 32, buffers = 4)
        assertEquals(io.ops.readWindow(in).available, 4)
      } finally {
        close(io, in)
        close(io, out)
      }
      intercept[IOException](io.ops.readWindow(in))
      intercept[IOException](io.ops.setReadWindow(in, bufferSize = 16, buffers = 2))
    }
  }

  test("a read into a buffer of the backend's fills one of its handle's window, which release gives back") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        io.ops.setReadWindow(in, bufferSize = 64, buffers = 2)
        val into = Slot[Received]()
        val reading = Future(io.perform(io.ops.read(in, into)))
        settle(io)
        assert(reading.poll().isEmpty, "the read completed with nothing written")
        assertEquals(io.ops.readWindow(in).available, 2, "a read waiting for bytes holds no buffer")
        writeAll(io, out, bytes("pooled"))
        reading.await
        val received = into.clear()
        assertEquals(received.buffer.position(), 0)
        assert(received.buffer.hasRemaining())
        assertEquals(io.ops.readWindow(in).available, 1)
        val firstPart = textOf(received)
        received.release()
        val rest = readReceivedExactly(io, io.ops.read(in, into), into, 6 - firstPart.length)
        assertEquals(firstPart + rest, "pooled")
        assertEquals(io.ops.readWindow(in).available, 2)
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("reads into the backend's buffers preserve bytes across any chunk boundaries and buffer reuse") {
    running { io ?=>
      val (in, out) = stream(io)
      io.ops.setReadWindow(in, bufferSize = 8, buffers = 1)
      val into = Slot[Received]()
      val read = io.ops.read(in, into)
      try {
        writeAll(io, out, bytes("abcdefghijklmnopqrs"))
        assertEquals(readReceivedExactly(io, read, into, 19), "abcdefghijklmnopqrs")
        assertEquals(io.ops.readWindow(in).available, 1)
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a read into a buffer of the backend's at the end of the stream completes with an empty buffer") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        close(io, out)
        val into = Slot[Received]()
        io.perform(io.ops.read(in, into))
        val received = into.clear()
        assertEquals(received.buffer.remaining(), 0)
        received.release()
        assertEquals(io.ops.readWindow(in).available, ReadWindow.DefaultBuffers)
      } finally close(io, in)
    }
  }

  test("with every buffer of its window held a read waits, taking nothing, and goes on when one is released") {
    running { io ?=>
      val (in, out) = stream(io)
      var held: Received | Null = null
      try {
        io.ops.setReadWindow(in, bufferSize = 16, buffers = 1)
        held = receivedOf(io, in, out, "a")
        assertEquals(textOf(held.nn), "a")
        assertEquals(io.ops.readWindow(in).available, 0)
        writeAll(io, out, bytes("b"))
        val into = Slot[Received]()
        val read = io.ops.read(in, into)
        // A time limit runs while it waits for the window, and leaves the bytes where they were.
        intercept[TimeoutException](io.perform(read, 20.millis))
        intercept[IllegalStateException](into.clear())
        // So does a cancel.
        val cancelled = Future(io.perform(read))
        settle(io)
        assert(cancelled.poll().isEmpty, "a read took a buffer of a window with none")
        cancelled.cancel()
        assert(failureOf(cancelled).isInstanceOf[CancellationException])
        // And a release lets the read that waits go on.
        val pending = Future(io.perform(read))
        settle(io)
        assert(pending.poll().isEmpty)
        held.nn.release()
        held = null
        pending.await
        assertEquals(readReceivedExactly(io, read, into, 1), "b", "what came while the window was full was kept")
        assertEquals(io.ops.readWindow(in).available, 1)
      } finally {
        if held != null then held.nn.release()
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a read waiting for its window fails when its handle is closed, and a buffer held outlives the handle") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        io.ops.setReadWindow(in, bufferSize = 16, buffers = 1)
        val held = receivedOf(io, in, out, "kept")
        val waiting = Future(io.perform(io.ops.read(in, Slot[Received]())))
        settle(io)
        assert(waiting.poll().isEmpty)
        close(io, in)
        val failure = failureOf(waiting)
        assert(failure.isInstanceOf[ClosedChannelException], String.valueOf(failure))
        assertEquals(textOf(held), "kept", "the buffer is the caller's until it is released")
        held.release()
        intercept[IllegalStateException](held.release())
      } finally close(io, out)
    }
  }

  test("releasing a Received twice throws IllegalStateException, leaving the window as it was") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        io.ops.setReadWindow(in, bufferSize = 16, buffers = 2)
        val received = receivedOf(io, in, out, "a")
        received.release()
        intercept[IllegalStateException](received.release())
        assertEquals(io.ops.readWindow(in).available, 2)
        val next = receivedOf(io, in, out, "b") // the window is whole
        val other = receivedOf(io, in, out, "c")
        assertEquals(io.ops.readWindow(in).available, 0)
        next.release()
        other.release()
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a scattering read fills its buffers in order, each before the next, completing once bytes have come") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        writeAll(io, out, bytes("hello world"))
        val bufs = Array(allocate(4), allocate(4), allocate(3))
        val read = io.ops.read(in, bufs)
        while bufs.exists(_.hasRemaining()) do {
          val before = bufs.map(_.position()).sum
          io.perform(read)
          assert(bufs.map(_.position()).sum > before, "unexpected EOF")
          for i <- bufs.indices if bufs(i).hasRemaining() do
            assert(bufs.drop(i + 1).forall(_.position() == 0), "later buffers filled before an earlier buffer")
        }
        assertEquals(bufs.map(_.position()).toList, List(4, 4, 3))
        assertEquals(readText(bufs), "hello world")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("a scattering read refuses buffers none of which has room, or none, and reads nothing at the end") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        intercept[IllegalArgumentException](io.ops.read(in, Array.empty[ByteBuffer]))
        val full = allocate(4)
        full.position(4)
        val refused = attempt(io, io.ops.read(in, Array(full, full.duplicate())))
        assert(refused.isInstanceOf[IllegalArgumentException], String.valueOf(refused))
        close(io, out)
        val bufs = Array(allocate(4), allocate(4))
        io.perform(io.ops.read(in, bufs))
        assertEquals(bufs.map(_.position()).toList, List(0, 0), "the end of the stream adds nothing")
      } finally close(io, in)
    }
  }

  test("a gathering write drains its buffers in order, and with nothing remaining completes at once") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val parts = Array(bytes("ab"), bytes(""), bytes("cdef"))
        writeAll(io, out, parts)
        assert(parts.forall(!_.hasRemaining()))
        io.perform(io.ops.write(out, parts)) // nothing remaining
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf))
        readExactly(io, io.ops.read(in, buf), buf, 6)
        assertEquals(text(buf), "abcdef")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("any buffer is read into and written from, on the heap or not, single or several; a read-only one is written") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        def heap(text: String) = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8))
        def received(buf: ByteBuffer) = new String(buf.array(), 0, buf.position(), StandardCharsets.UTF_8)
        val direct = ByteBuffer.allocateDirect(4)
        direct.put("dir|".getBytes(StandardCharsets.UTF_8)).flip()
        writeAll(io, out, heap("heap|"))
        writeAll(io, out, direct)
        writeAll(io, out, heap("ro|").asReadOnlyBuffer())
        writeAll(io, out, Array(heap("a"), bytes("b"), heap("c|")))
        val into = ByteBuffer.allocate(64)
        var got = ""
        while !got.endsWith("c|") do {
          io.perform(io.ops.read(in, into))
          got = received(into)
        }
        assertEquals(got, "heap|dir|ro|abc|")
        writeAll(io, out, bytes("scattered"))
        val first = ByteBuffer.allocate(4)
        val second = allocate(16)
        var total = 0
        while total < 9 do {
          io.perform(io.ops.read(in, Array(first, second)))
          total = first.position() + second.position()
        }
        assertEquals(received(first) + text(second), "scattered")
        intercept[IllegalArgumentException](io.ops.read(in, ByteBuffer.allocate(8).asReadOnlyBuffer()))
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("continuously ready reads let sibling tasks and timers progress") {
    running { io ?=>
      val (in, out) = stream(io)
      io.perform(io.ops.close(out)) // EOF stays immediately ready on every read.
      val progressed = new java.util.concurrent.atomic.AtomicBoolean
      try {
        val busy = Future {
          val buffer = arena.allocate(1)
          val read = io.ops.read(in, buffer)
          var iterations = 0
          var observed = false
          while iterations < 10000 do {
            buffer.clear()
            io.perform(read)
            observed ||= progressed.get()
            iterations += 1
          }
          observed
        }
        val sibling = Future {
          io.perform(io.ops.timer(0))
          progressed.set(true)
        }
        // MUnit's by-name assert holds a monitor: never suspend inside it, since a native continuation may migrate.
        val observedProgress = busy.await
        assert(observedProgress, "immediate readiness starved a sibling's timer")
        sibling.await
      } finally io.perform(io.ops.close(in))
    }
  }

  test("a read unlinks from a scope with a child after its buffer changes") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        // The child and its parent's pending read are distinct group members.
        val writer = Future {
          io.perform(io.ops.timer(10))
          writeAll(io, out, arena.of(Array[Byte](42)))
        }
        val buffer = arena.allocate(8)
        io.perform(io.ops.read(in, buffer))
        writer.await
        assertEquals(buffer.position(), 1)
      } finally {
        io.perform(io.ops.close(in))
        io.perform(io.ops.close(out))
      }
    } // Joining the scope must finish even when the read operation's structural hash changed.
  }

  test("two futures stream bytes through connected handles") {
    val total = 1 << 20
    val received = running { io ?=>
      val (in, out) = stream(io)
      try {
        val writer = Future {
          val buf = arena.allocate(8192)
          val write = io.ops.write(out, buf)
          var sent = 0
          while sent < total do {
            buf.clear().limit(math.min(8192, total - sent))
            while buf.hasRemaining do buf.put(((sent + buf.position()) % 251).toByte)
            buf.flip()
            while buf.hasRemaining do io.perform(write)
            sent += buf.limit()
          }
          io.perform(io.ops.close(out))
        }
        val reader = Future {
          val buf = arena.allocate(8192)
          val read = io.ops.read(in, buf)
          var received = 0
          var atEnd = false
          while !atEnd do {
            buf.clear()
            io.perform(read)
            if buf.position() == 0 then atEnd = true
            else {
              buf.flip()
              while buf.hasRemaining do {
                assertEquals(buf.get(), (received % 251).toByte)
                received += 1
              }
            }
          }
          received
        }
        writer.await
        reader.await
      } finally io.perform(io.ops.close(in))
    }
    assertEquals(received, total)
  }

  test("an op a future performs races in select, and cancelling the future stops it, the stream left usable") {
    val outcome = running { io ?=>
      val (in, out) = stream(io)
      try {
        val buf = arena.allocate(16)
        val read = Future(io.perform(io.ops.read(in, buf))) // pending: nothing is written
        val timer = Future(io.perform(io.ops.timer(20)))
        val first = Async.select(timer.handle(_ => "timer"), read.handle(_ => "read"))
        read.cancel()
        val cancelled = read.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
        // The cancelled read took nothing: what is written now is read in full.
        writeAll(io, out, arena.of("after".getBytes(StandardCharsets.UTF_8)))
        val again = arena.allocate(16)
        io.perform(io.ops.read(in, again))
        readExactly(io, io.ops.read(in, again), again, 5)
        s"$first $cancelled ${text(again)}"
      } finally {
        io.perform(io.ops.close(in))
        io.perform(io.ops.close(out))
      }
    }
    assertEquals(outcome, "timer true after")
  }

  test("a second read pending on a handle fails with IllegalArgumentException") {
    running { io ?=>
      val (in, out) = stream(io)
      try {
        val first = Future(io.perform(io.ops.read(in, allocate(16))))
        settle(io)
        val second = attempt(io, io.ops.read(in, allocate(16)))
        assert(second.isInstanceOf[IllegalArgumentException], s"a second read: $second")
        val fromAnother = failureOf(Future(io.perform(io.ops.read(in, allocate(16)))))
        assert(fromAnother.isInstanceOf[IllegalArgumentException], s"a second read from another future: $fromAnother")
        assert(first.poll().isEmpty, "the first read is still pending")
        writeAll(io, out, bytes("x"))
        first.await
        val again = Future(io.perform(io.ops.read(in, allocate(16)))) // once the first has finished
        settle(io)
        writeAll(io, out, bytes("y"))
        again.await

      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }
}
