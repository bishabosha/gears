package asyncio

import java.io.IOException

/** What a backend whose handles listen and close as ops must do (see `ReactorContract`). */
trait LifecycleContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & LifecycleBackend

  test("a listener listens once its listening op completes, and accepts") {
    withReactor { r =>
      val (listener, port) = onFreePort { port =>
        val h = backend.handles.listener(IpAddress.IPv4("127.0.0.1", port))
        val listening = Outcome()
        r.submit(backend.ops.listening(h), listening)
        turnUntil(r)(listening.finished)
        if listening.failure != null then {
          r.handles.close(h)
          throw new IOException(String.valueOf(listening.failure))
        }
        h
      }
      try {
        val accepted = HandleSlot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = r.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try {
          val connected = Outcome()
          r.submit(backend.ops.connect(client), connected)
          turnUntil(r)(acceptDone.finished && connected.finished)
          assertEquals(acceptDone.completed, 1, String.valueOf(acceptDone.failure))
          r.handles.close(accepted.clear())
        } finally r.handles.close(client)
      } finally r.handles.close(listener)
    }
  }

  test("listening on an address in use fails the listening op, not the handle's creation") {
    withReactor { r =>
      val (taken, port) = onFreePort(port => r.handles.listen(IpAddress.IPv4("127.0.0.1", port)))
      try {
        val h = backend.handles.listener(IpAddress.IPv4("127.0.0.1", port), reuseAddr = false)
        try {
          val listening = Outcome()
          r.submit(backend.ops.listening(h), listening)
          turnUntil(r)(listening.finished)
          assert(listening.failure.isInstanceOf[IOException], String.valueOf(listening.failure))
        } finally r.handles.close(h)
      } finally r.handles.close(taken)
    }
  }

  test("a close op completes once the handle is closed, and fails the op pending on it") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      val read = Outcome()
      r.submit(backend.ops.read(in, allocate(16)), read)
      r.runOnce(block = false)
      val closed = Outcome()
      r.submit(backend.ops.close(in), closed)
      turnUntil(r)(closed.finished && read.finished)
      assertEquals(closed.completed, 1, String.valueOf(closed.failure))
      assert(read.failure.isInstanceOf[IOException], String.valueOf(read.failure))
      val again = Outcome()
      r.submit(backend.ops.close(in), again)
      turnUntil(r)(again.finished)
      assert(again.failure.isInstanceOf[IOException], "closing a closed handle fails")
      val outClosed = Outcome()
      r.submit(backend.ops.close(out), outClosed)
      turnUntil(r)(outClosed.finished)
      assertEquals(outClosed.completed, 1, String.valueOf(outClosed.failure))
    }
  }

  test("a closed listener's port can be listened on again once its close op completes") {
    withReactor { r =>
      val (first, port) = onFreePort(port => r.handles.listen(IpAddress.IPv4("127.0.0.1", port)))
      val closed = Outcome()
      r.submit(backend.ops.close(first), closed)
      turnUntil(r)(closed.finished)
      assertEquals(closed.completed, 1, String.valueOf(closed.failure))
      val second = backend.handles.listener(IpAddress.IPv4("127.0.0.1", port), reuseAddr = false)
      try {
        val listening = Outcome()
        r.submit(backend.ops.listening(second), listening)
        turnUntil(r)(listening.finished)
        assertEquals(listening.completed, 1, String.valueOf(listening.failure))
      } finally r.handles.close(second)
    }
  }

  test("a close op on a connection ends its peer's stream") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => r.handles.listen(IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = HandleSlot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = r.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        val connected = Outcome()
        r.submit(backend.ops.connect(client), connected)
        turnUntil(r)(acceptDone.finished && connected.finished)
        val server = accepted.clear()
        try {
          val closed = Outcome()
          r.submit(backend.ops.close(client), closed)
          turnUntil(r)(closed.finished)
          assertEquals(closed.completed, 1, String.valueOf(closed.failure))
          val buf = allocate(16)
          assertEquals(readToEnd(r, backend.ops.read(server, buf), buf), "")
        } finally r.handles.close(server)
      } finally r.handles.close(listener)
    }
  }
}
