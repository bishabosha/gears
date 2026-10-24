package gearsexample

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import gears.async.io.Backend
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamBackend
import gears.async.io.testkit.BufferArena
import DemoIO.{StreamProtocol, drainTo}
import gears.async.io.ReactorFutures
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
      ReactorFutures.run { io ?=>
        println(s"connection in progress to: $address")
        val socket = Sockets.connect(address)
        val pool = io.backend.pool(bufferSize = 8192, buffers = 4)
        try {
          // One repeatable op, resubmitted until the request is out.
          val write = io.backend.ops.write(socket, buf)
          StreamProtocol.frame("Hello from Scala Native KQueue Example!\n", buf)
          while buf.hasRemaining() do {
            val before = buf.position()
            io.perform(write)
            println(s"Wrote ${buf.position() - before} bytes to socket.")
          }
          // The response as a stream: a chunk at a time from the pool's buffers, until the server closes.
          val chunks = HigherLevel.chunks(socket, pool, backlog = 2)
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
          io.perform(io.backend.ops.close(socket))
          pool.close()
        }
      }
    }
  }
}
