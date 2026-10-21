package asyncio

import java.io.IOException
import java.nio.charset.StandardCharsets

/** What a backend with pooled reads must do (see `ReactorContract`). */
trait PooledReadContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PooledReadBackend

  private def textOf(received: Received): String = {
    val buf = received.buffer
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    new String(out, StandardCharsets.UTF_8)
  }

  test("a pooled read fills a buffer of the pool's, which release gives back") {
    val pool = backend.pool(bufferSize = 64, buffers = 2)
    try withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        val into = Slot[Received]()
        val outcome = Outcome()
        r.submit(backend.ops.read(in, pool, into), outcome)
        for _ <- 1 to 3 do r.runOnce(block = false)
        assertEquals(outcome.completed, 0)
        r.submit(backend.ops.write(out, bytes("pooled")), Outcome())
        turnUntil(r)(outcome.finished)
        assertEquals(outcome.completed, 1, String.valueOf(outcome.failure))
        val received = into.clear()
        assertEquals(received.buffer.position(), 0)
        assertEquals(textOf(received), "pooled")
        assertEquals(pool.available, 1)
        received.release()
        assertEquals(pool.available, 2)
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    } finally pool.close()
  }

  test("a pooled read at the end of the stream completes with an empty buffer") {
    val pool = backend.pool(bufferSize = 16, buffers = 1)
    try withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        r.handles.close(out)
        val into = Slot[Received]()
        val outcome = Outcome()
        r.submit(backend.ops.read(in, pool, into), outcome)
        turnUntil(r)(outcome.finished)
        assertEquals(outcome.completed, 1, String.valueOf(outcome.failure))
        val received = into.clear()
        assertEquals(received.buffer.remaining(), 0)
        received.release()
      } finally r.handles.close(in)
    } finally pool.close()
  }

  test("a pooled read fails when the pool has no buffer free, and goes on once one is released") {
    val pool = backend.pool(bufferSize = 16, buffers = 1)
    try withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        r.submit(backend.ops.write(out, bytes("one")), Outcome())
        val first = Slot[Received]()
        val got = Outcome()
        r.submit(backend.ops.read(in, pool, first), got)
        turnUntil(r)(got.finished)
        assertEquals(got.completed, 1, String.valueOf(got.failure))
        val held = first.clear()
        assertEquals(pool.available, 0)
        r.submit(backend.ops.write(out, bytes("two")), Outcome())
        val second = Slot[Received]()
        val refused = Outcome()
        r.submit(backend.ops.read(in, pool, second), refused)
        turnUntil(r)(refused.finished)
        assert(refused.failure.isInstanceOf[IOException], String.valueOf(refused.failure))
        held.release()
        val again = Outcome()
        r.submit(backend.ops.read(in, pool, second), again)
        turnUntil(r)(again.finished)
        assertEquals(again.completed, 1, String.valueOf(again.failure))
        val received = second.clear()
        assertEquals(textOf(received), "two")
        received.release()
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    } finally pool.close()
  }

  test("a pooled read and a read into the caller's buffer take turns on one socket") {
    val pool = backend.pool(bufferSize = 64, buffers = 2)
    try withReactor { r =>
      val (listener, port) = onFreePort(port => r.handles.listen(IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = HandleSlot()
        r.submit(backend.ops.accept(listener, accepted), Outcome())
        val client = r.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try {
          val connected = Outcome()
          r.submit(backend.ops.connect(client), connected)
          turnUntil(r)(connected.finished && { accepted.toString; true })
          turnUntil(r)(connected.finished)
          val server = {
            var h = -1
            turnUntil(r) { try { h = accepted.clear(); true } catch case _: IllegalStateException => false }
            h
          }
          try {
            r.submit(backend.ops.write(client, bytes("first")), Outcome())
            val into = Slot[Received]()
            val pooled = Outcome()
            r.submit(backend.ops.read(server, pool, into), pooled)
            turnUntil(r)(pooled.finished)
            assertEquals(pooled.completed, 1, String.valueOf(pooled.failure))
            val received = into.clear()
            assertEquals(textOf(received), "first")
            received.release()
            r.submit(backend.ops.write(client, bytes("second")), Outcome())
            val buf = allocate(64)
            val plain = Outcome()
            r.submit(backend.ops.read(server, buf), plain)
            turnUntil(r)(plain.finished)
            assertEquals(plain.completed, 1, String.valueOf(plain.failure))
            assertEquals(text(buf), "second")
          } finally r.handles.close(server)
        } finally r.handles.close(client)
      } finally r.handles.close(listener)
    } finally pool.close()
  }
}
