package higherlevel

import gears.async.Async
import gears.async.Future

import java.io.IOException
import java.nio.charset.StandardCharsets

import asyncio.gears.FuturesContract

/** `FuturesContract`'s checks of the higher level, run by every backend's suite: what the lowest level is for. */
trait HigherLevelChecks extends FuturesContract {

  test("a higher level over perform: result-returning reads, accept and resolve") {
    val outcome = running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        val writer = Future {
          HigherLevel.writeAll(r, out, arena.of("wrapped".getBytes(StandardCharsets.UTF_8)))
          r.perform(r.ops.close(out))
        }
        val got = HigherLevel.read(r, in, buf)
        writer.await
        val atEnd = HigherLevel.read(r, in, buf)
        val resolved = HigherLevel.resolve(r, "localhost").nonEmpty
        s"$got ${text(buf)} $atEnd $resolved"
      } finally r.perform(r.ops.close(in))
    }
    assertEquals(outcome, "7 wrapped 0 true")
  }

  test("a higher level over perform: a stream of chunks consumed as a gears channel, with back-pressure") {
    val total = 1 << 20
    val received = running { r =>
      val pool = r.pool(bufferSize = 4096, buffers = 4) // a backlog of two, one being sent, one being read
      val (in, out) = r.handles.pipe()
      try {
        val writer = Future {
          val buf = arena.allocate(8192)
          var sent = 0
          while sent < total do {
            buf.clear().limit(math.min(8192, total - sent))
            HigherLevel.writeAll(r, out, buf)
            sent += buf.limit()
          }
          r.perform(r.ops.close(out))
        }
        val received = HigherLevel.drain(HigherLevel.chunks(r, in, pool, backlog = 2))
        writer.await
        received
      } finally {
        r.perform(r.ops.close(in))
        pool.close()
      }
    }
    assertEquals(received, total.toLong)
  }

  test("a higher level over perform: a stream closed early releases its chunks, and one that fails rethrows") {
    val outcome = running { r =>
      val pool = r.pool(bufferSize = 1024, buffers = 4)
      val (in, out) = r.handles.pipe()
      try {
        HigherLevel.writeAll(r, out, arena.of(("x" * 3000).getBytes(StandardCharsets.UTF_8)))
        val stream = HigherLevel.chunks(r, in, pool, backlog = 2)
        val first = stream.read().map { chunk =>
          val n = chunk.buffer.remaining()
          chunk.release()
          n
        }
        r.perform(r.ops.timer(20)) // lets the reader fill the window and the backlog
        stream.close() // releases what was read ahead
        val freed = pool.available == pool.buffers
        r.perform(r.ops.close(in)) // the next stream's read fails on the closed handle
        val failed =
          try {
            HigherLevel.drain(HigherLevel.chunks(r, in, pool, backlog = 2))
            false
          } catch case _: IOException => true
        s"${first.exists(_ > 0)} $freed $failed ${pool.available == pool.buffers}"
      } finally {
        r.perform(r.ops.close(out))
        pool.close()
      }
    }
    assertEquals(outcome, "true true true true")
  }

}
