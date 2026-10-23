package gearsexample

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import asyncio.Backend
import asyncio.UnixAddress
import asyncio.UnixStreamBackend
import asyncio.testkit.BufferArena
import DemoIO.{StreamProtocol, drainTo}
import asyncio.gears.ReactorFutures

/** Mirror of `example.KQueueExampleSocket`: connect, send one framed request, collect the response until EOF. */
object GearsSocket {
  def run(sock: String)(using UnixStreamBackend): Unit = run(UnixAddress(sock))

  def run(address: StreamAddress)(using UnixStreamBackend): Unit = {
    BufferArena.scoped { arena =>
      val buf = arena.allocate(8192)
      val response = new ByteArrayOutputStream()
      ReactorFutures.run { r =>
        val socket = Sockets.connect(r, address)
        try {
          println(s"connection in progress to: $address")
          r.perform(r.ops.connect(socket))
          // One repeatable op per direction, resubmitted until the request is out and the response is in.
          val write = r.ops.write(socket, buf)
          val read = r.ops.read(socket, buf)
          StreamProtocol.frame("Hello from Scala Native KQueue Example!\n", buf)
          while buf.hasRemaining() do {
            val before = buf.position()
            r.perform(write)
            println(s"Wrote ${buf.position() - before} bytes to socket.")
          }
          buf.clear() // The request is out; the buffer now collects the response.
          var eof = false
          while !eof do {
            r.perform(read)
            if buf.position() == 0 then eof = true // Nothing added: the server closed after its response.
            else {
              println(s"Read ${buf.position()} bytes from socket.")
              buf.flip()
              drainTo(buf, response)
              buf.clear()
            }
          }
          println(s"Received message: `${new String(response.toByteArray, StandardCharsets.UTF_8)}`")
        } finally r.perform(r.ops.close(socket))
      }
    }
  }
}
