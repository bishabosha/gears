package higherlevel

import gears.async.Async
import gears.async.Future

import java.io.IOException
import java.nio.charset.StandardCharsets

import gears.async.io.testkit.ProgramSuite

/** Checks of the higher level, run over every backend by the demos' suites: what the lowest level is for. */
trait HigherLevelChecks extends ProgramSuite {

  test("a higher level over perform: result-returning reads, accept and resolve") {
    val outcome = running { io ?=>
      val (in, out) = pipe(io)
      try {
        val buf = arena.allocate(16)
        val writer = Future {
          HigherLevel.writeAll(out, arena.of("wrapped".getBytes(StandardCharsets.UTF_8)))
          io.perform(io.backend.ops.close(out))
        }
        val got = HigherLevel.read(in, buf)
        writer.await
        val atEnd = HigherLevel.read(in, buf)
        val resolved = HigherLevel.resolve("localhost").nonEmpty
        s"$got ${text(buf)} $atEnd $resolved"
      } finally io.perform(io.backend.ops.close(in))
    }
    assertEquals(outcome, "7 wrapped 0 true")
  }

  test("a higher level over perform: a stream of chunks consumed as a gears channel, with back-pressure") {
    val total = 1 << 20
    val received = running { io ?=>
      val pool = io.backend.pool(bufferSize = 4096, buffers = 4) // a backlog of two, one being sent, one being read
      val (in, out) = pipe(io)
      try {
        val writer = Future {
          val buf = arena.allocate(8192)
          var sent = 0
          while sent < total do {
            buf.clear().limit(math.min(8192, total - sent))
            HigherLevel.writeAll(out, buf)
            sent += buf.limit()
          }
          io.perform(io.backend.ops.close(out))
        }
        val received = HigherLevel.drain(HigherLevel.chunks(in, pool, backlog = 2))
        writer.await
        received
      } finally {
        io.perform(io.backend.ops.close(in))
        pool.close()
      }
    }
    assertEquals(received, total.toLong)
  }

  test("a higher level over perform: a stream closed early releases its chunks, and one that fails rethrows") {
    val outcome = running { io ?=>
      val pool = io.backend.pool(bufferSize = 1024, buffers = 4)
      val (in, out) = pipe(io)
      try {
        HigherLevel.writeAll(out, arena.of(("x" * 3000).getBytes(StandardCharsets.UTF_8)))
        val stream = HigherLevel.chunks(in, pool, backlog = 2)
        val first = stream.read().map { chunk =>
          val n = chunk.buffer.remaining()
          chunk.release()
          n
        }
        io.perform(io.backend.ops.timer(20)) // lets the reader fill the window and the backlog
        stream.close() // releases what was read ahead
        val freed = pool.available == pool.buffers
        io.perform(io.backend.ops.close(in)) // the next stream's read fails on the closed handle
        val failed =
          try {
            HigherLevel.drain(HigherLevel.chunks(in, pool, backlog = 2))
            false
          } catch case _: IOException => true
        s"${first.exists(_ > 0)} $freed $failed ${pool.available == pool.buffers}"
      } finally {
        io.perform(io.backend.ops.close(out))
        pool.close()
      }
    }
    assertEquals(outcome, "true true true true")
  }

}
