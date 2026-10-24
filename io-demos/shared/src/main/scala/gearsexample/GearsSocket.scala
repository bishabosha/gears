package gearsexample

import java.io.IOException

import gears.async.io.Backend
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamBackend
import gears.async.io.testkit.BufferArena
import DemoIO.{FrameParser, StreamProtocol}
import gears.async.io.ReactorFutures
import higherlevel.HigherLevel

/** Mirror of `example.KQueueExampleSocket`: connect, send one framed request, and read the framed response, as the
  * higher level's stream of chunks from the backend's own buffers, until the whole frame has arrived; a stream that
  * ends first is a reply cut short, and an error. The client shuts its side down once the request is out, so that the
  * server sees where it ends.
  */
object GearsSocket {
  def run(sock: String)(using UnixStreamBackend): Unit = run(UnixAddress(sock))

  def run(address: StreamAddress)(using UnixStreamBackend): Unit = {
    BufferArena.scoped { arena =>
      val buf = arena.allocate(8192)
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
          HigherLevel.shutdownOutput(socket) // the request ends here, and the server's reply follows to its end
          // The response as a stream: a chunk at a time from the pool's buffers, until its frame is whole.
          val parser = new FrameParser
          val chunks = HigherLevel.chunks(socket, pool, backlog = 2)
          var reply: String | Null = null
          try
            while reply == null do
              chunks.read() match {
                case Some(chunk) =>
                  println(s"Read ${chunk.buffer.remaining()} bytes from socket.")
                  parser.feed(chunk.buffer)
                  chunk.release()
                  reply = parser.next()
                case None => throw new IOException("The server closed the connection before its whole reply")
              }
          finally chunks.close()
          println(s"Received message: `$reply`")
        } finally {
          io.perform(io.backend.ops.close(socket))
          pool.close()
        }
      }
    }
  }
}
