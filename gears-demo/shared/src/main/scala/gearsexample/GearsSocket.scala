package gearsexample

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import asyncio.Backend
import asyncio.UnixAddress
import asyncio.UnixStreamBackend
import asyncio.testkit.BufferArena
import DemoIO.{StreamProtocol, drainTo}
import asyncio.gears.ReactorFutures
import higherlevel.HigherLevel

/** Mirror of `example.KQueueExampleSocket`: connect, send one framed request, collect the response until EOF, the
  * response as the higher level's stream of chunks, read from the backend's own buffers.
  */
object GearsSocket {
  def run(sock: String)(using UnixStreamBackend): Unit = run(UnixAddress(sock))

  def run(address: StreamAddress)(using UnixStreamBackend): Unit = {
    BufferArena.scoped { arena =>
      val buf = arena.allocate(8192)
      val response = new ByteArrayOutputStream()
      ReactorFutures.run { r =>
        val socket = Sockets.connect(r, address)
        val pool = r.pool(bufferSize = 8192, buffers = 4)
        try {
          println(s"connection in progress to: $address")
          r.perform(r.ops.connect(socket))
          // One repeatable op, resubmitted until the request is out.
          val write = r.ops.write(socket, buf)
          StreamProtocol.frame("Hello from Scala Native KQueue Example!\n", buf)
          while buf.hasRemaining() do {
            val before = buf.position()
            r.perform(write)
            println(s"Wrote ${buf.position() - before} bytes to socket.")
          }
          // The response as a stream: a chunk at a time from the pool's buffers, until the server closes.
          val chunks = HigherLevel.chunks(r, socket, pool, backlog = 2)
          var next = chunks.read()
          while next.isDefined do {
            val chunk = next.get
            println(s"Read ${chunk.buffer.remaining()} bytes from socket.")
            drainTo(chunk.buffer, response)
            chunk.release()
            next = chunks.read()
          }
          println(s"Received message: `${new String(response.toByteArray, StandardCharsets.UTF_8)}`")
        } finally {
          r.perform(r.ops.close(socket))
          pool.close()
        }
      }
    }
  }
}
