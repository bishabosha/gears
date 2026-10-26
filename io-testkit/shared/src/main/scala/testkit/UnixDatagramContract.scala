package gears.async.io
package testkit

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** What a backend with Unix datagram sockets must do (see `ReactorContract`). */
trait UnixDatagramContract extends SocketContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & UnixDatagramBackend

  /** A Unix datagram socket from the `datagram` op, whose failure is thrown. */
  protected def unixDatagram(r: R, local: UnixAddress | Null, remote: UnixAddress | Null)(using
      munit.Location
  ): UnixDatagram = {
    val into = Slot[UnixDatagram]()
    opened(r, into, backend.ops.datagram(local, remote, into))
  }

  test("a Unix datagram goes to a bound socket, which learns its sender's path") {
    val dir = tempDir()
    try
      withReactor { r =>
        val serverPath = dir.resolve("server.sock").toString
        val clientPath = dir.resolve("client.sock").toString
        val server = unixDatagram(r, UnixAddress(serverPath), null)
        try {
          val client = unixDatagram(r, UnixAddress(clientPath), UnixAddress(serverPath))
          try {
            val buf = allocate(64)
            val from = Slot[UnixAddress]()
            val received = Outcome()
            r.submit(backend.ops.receive(server, buf, from), received)
            val sent = Outcome()
            r.submit(backend.ops.send(client, bytes("unix datagram")), sent)
            turnUntil(r)(received.finished && sent.finished)
            assertEquals(received.completed, 1, String.valueOf(received.failure))
            val copy = new Array[Byte](buf.remaining())
            buf.get(copy)
            assertEquals(new String(copy, StandardCharsets.UTF_8), "unix datagram")
            assertEquals(from.clear(), UnixAddress(clientPath))
          } finally closeAlone(client)
        } finally closeAlone(server)
        assert(!Files.exists(java.nio.file.Path.of(serverPath)), "closing a bound datagram socket removes its path")
      }
    finally deleteAll(dir)
  }
}
