package gears.async.io
package testkit

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** What a backend with Unix stream sockets must do (see `ReactorContract`). */
trait UnixStreamContract extends SocketContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & UnixStreamBackend

  /** A listener on a Unix socket path, listening, as `listen` for an IP address. */
  protected def listen(r: R, address: UnixAddress)(using munit.Location): Handle = {
    val into = Slot[Handle]()
    opened(r, into, backend.ops.listen(address, into))
  }

  /** Submits a `connect` op to a Unix socket path on `r`, as `startConnect` for an IP address. */
  protected def startConnect(r: R, address: UnixAddress): (Outcome, Slot[Handle]) = {
    val into = Slot[Handle]()
    val connected = Outcome()
    r.submit(backend.ops.connect(address, into), connected)
    (connected, into)
  }

  test("a socket's reads end after the bytes sent before its peer closed") {
    val dir = tempDir()
    try
      withReactor { r =>
        val address = UnixAddress(dir.resolve("end.sock").toString)
        val listener = listen(r, address)
        try {
          val (connected, into) = startConnect(r, address)
          val accepted = Slot[Handle]()
          val acceptDone = Outcome()
          r.submit(backend.ops.accept(listener, accepted), acceptDone)
          turnUntil(r)(acceptDone.finished && connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
          val client = into.clear()
          val server = accepted.clear()
          try {
            val sent = Outcome()
            r.submit(backend.ops.write(client, bytes("last words")), sent)
            turnUntil(r)(sent.finished)
            closeAlone(client)
            val buf = allocate(64)
            assertEquals(readToEnd(r, backend.ops.read(server, buf), buf), "last words")
          } finally closeAlone(server)
        } finally closeAlone(listener)
      }
    finally deleteAll(dir)
  }

  test("a Unix stream socket accepts, connects and echoes") {
    val dir = tempDir()
    try
      withReactor { r =>
        val address = UnixAddress(dir.resolve("echo.sock").toString)
        val listener = listen(r, address)
        try {
          val accepted = Slot[Handle]()
          val acceptDone = Outcome()
          r.submit(backend.ops.accept(listener, accepted), acceptDone)
          val (connected, into) = startConnect(r, address)
          turnUntil(r)(acceptDone.finished && connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
          assertEquals(acceptDone.completed, 1, String.valueOf(acceptDone.failure))
          val client = into.clear()
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
          } finally closeAlone(server)
          closeAlone(client)
        } finally closeAlone(listener)
      }
    finally deleteAll(dir)
  }

  test("closing a Unix listener removes its socket file") {
    val dir = tempDir()
    try
      withReactor { r =>
        val path = dir.resolve("gone.sock")
        val listener = listen(r, UnixAddress(path.toString))
        assert(Files.exists(path))
        closeAlone(listener)
        assert(!Files.exists(path))
      }
    finally deleteAll(dir)
  }

}
