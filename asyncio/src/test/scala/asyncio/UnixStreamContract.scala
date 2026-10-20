package asyncio

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** What a backend with Unix stream sockets must do (see `ReactorContract`). */
trait UnixStreamContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & UnixStreamBackend

  test("a socket's reads end after the bytes sent before its peer closed") {
    val dir = tempDir()
    try withReactor { r =>
      val address = UnixAddress(dir.resolve("end.sock").toString)
      val listener = backend.handles.listen(address)
      try {
        val client = backend.handles.connect(address)
        val accepted = HandleSlot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        turnUntil(r)(acceptDone.finished)
        val server = accepted.clear()
        try {
          val connected = Outcome()
          r.submit(backend.ops.connect(client), connected)
          turnUntil(r)(connected.finished)
          val sent = Outcome()
          r.submit(backend.ops.write(client, bytes("last words")), sent)
          turnUntil(r)(sent.finished)
          backend.handles.close(client)
          val buf = allocate(64)
          assertEquals(readToEnd(r, backend.ops.read(server, buf), buf), "last words")
        } finally backend.handles.close(server)
      } finally backend.handles.close(listener)
    } finally deleteAll(dir)
  }

  test("a Unix stream socket accepts, connects and echoes") {
    val dir = tempDir()
    try withReactor { r =>
      val address = UnixAddress(dir.resolve("echo.sock").toString)
      val listener = backend.handles.listen(address)
      try {
        val accepted = HandleSlot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = backend.handles.connect(address)
        val connected = Outcome()
        r.submit(backend.ops.connect(client), connected)
        turnUntil(r)(acceptDone.finished && connected.finished)
        assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        assertEquals(acceptDone.completed, 1, String.valueOf(acceptDone.failure))
        val server = accepted.clear()
        try {
          val request = bytes("ping")
          val sent = Outcome()
          r.submit(backend.ops.write(client, request), sent)
          val serverBuf = allocate(16)
          val got = Outcome()
          r.submit(backend.ops.read(server, serverBuf), got)
          turnUntil(r)(sent.finished && got.finished)
          assertEquals(text(serverBuf), "ping")
          serverBuf.flip()
          val echoed = Outcome()
          r.submit(backend.ops.write(server, serverBuf), echoed)
          val clientBuf = allocate(16)
          val reply = Outcome()
          r.submit(backend.ops.read(client, clientBuf), reply)
          turnUntil(r)(echoed.finished && reply.finished)
          assertEquals(text(clientBuf), "ping")
        } finally backend.handles.close(server)
        backend.handles.close(client)
      } finally backend.handles.close(listener)
    } finally deleteAll(dir)
  }

  test("closing a Unix listener removes its socket file") {
    val dir = tempDir()
    try withReactor { r =>
      val path = dir.resolve("gone.sock")
      val listener = backend.handles.listen(UnixAddress(path.toString))
      assert(Files.exists(path))
      backend.handles.close(listener)
      assert(!Files.exists(path))
    } finally deleteAll(dir)
  }

}
