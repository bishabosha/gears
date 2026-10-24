package gearsexample

import gears.async.Async
import gears.async.Future

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets

import gears.async.io.Io
import gears.async.io.Backend
import gears.async.io.Handle
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamBackend
import gears.async.io.UnixStreams
import gears.async.io.testkit.BufferArena
import DemoIO.{StreamProtocol, drainTo}
import gears.async.io.ReactorFutures
import higherlevel.HigherLevel

/** Mirror of `example.KQueueExampleServerSocket`. The acceptor is a loop, and each connection is its own future whose
  * state machine is now plain sequential code.
  */
object GearsServerSocket {
  private val Response = "Just pinging back!\n".getBytes(StandardCharsets.UTF_8)

  /** Reads a length-framed request through one 8 KiB buffer, replies, and closes the connection gracefully, so that
    * the client reads the whole reply before the end of the stream however the request ended.
    */
  private def serve(io: Io[UnixStreams], client: Handle)(using Async): Unit = {
    val arena = BufferArena.open()
    try {
      val buf = arena.allocate(8192)
      // One repeatable op per direction for the whole connection; the buffer's limit says how much each read takes.
      val read = io.backend.ops.read(client, buf)
      val write = io.backend.ops.write(client, buf)

      /** Reads until the buffer's limit is reached; false at an unexpected EOF. */
      def fill(): Boolean = {
        var open = true
        while open && buf.hasRemaining() do {
          val before = buf.position()
          io.perform(read)
          open = buf.position() > before // a read that adds nothing is the end of the stream
        }
        if !open then println(s"Unexpected EOF from client socket $client.")
        open
      }

      buf.clear().limit(StreamProtocol.HeaderLength)
      if fill() then {
        val size = StreamProtocol.messageLength(buf)
        if size < 0 || size > StreamProtocol.MaxMessageLength then
          println(s"Invalid message size from client socket $client: $size")
        else {
          // Gather the body one buffer's worth at a time.
          val body = new ByteArrayOutputStream()
          var remaining = size
          var open = true
          while open && remaining > 0 do {
            buf.clear().limit(math.min(remaining, buf.capacity()))
            open = fill()
            buf.flip()
            remaining -= buf.remaining()
            drainTo(buf, body)
          }
          if open then {
            buf.clear()
            buf.put(Response)
            buf.flip()
            while buf.hasRemaining() do io.perform(write)
          }
        }
      }
    } catch {
      case e: IOException =>
        // One misbehaving client must not take the server down.
        println(s"Client socket $client failed: ${e.getMessage}")
    } finally
      try HigherLevel.closeGracefully(client, arena.allocate(256))(using io)
      finally arena.close()
  }

  def run(sock: String)(using UnixStreamBackend): Unit = run(UnixAddress(sock))

  def run(address: StreamAddress)(using UnixStreamBackend): Unit =
    ReactorFutures.run { io ?=>
      val server = Sockets.listen(address)
      try {
        println("Socket is now listening for connections.")
        Sockets.serve(server)(client => serve(io, client))
      } finally io.perform(io.backend.ops.close(server))
    }
}
