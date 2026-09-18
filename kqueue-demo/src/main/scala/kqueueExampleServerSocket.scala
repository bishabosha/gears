package example

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleServerSocket {
  private val Response = "Just pinging back!\n".getBytes(StandardCharsets.UTF_8)

  private enum State {
    case ReadHeader, ReadMessage, SendResponse
  }

  /** One accepted connection: reads a length-framed request, then replies and closes. Owns a zone holding its single 8
    * KiB buffer; a body larger than that is read in chunks and gathered until complete. The zone closes with the
    * connection.
    */
  private final class Connection(val fd: Int) extends AutoCloseable {
    private var state = State.ReadHeader
    private val zone: Zone = Zone.open()
    private val buf: ByteBuffer = NativeBuffer.allocate(8192)(using zone)
    private val body = new ByteArrayOutputStream()
    private var bodyRemaining = 0
    expect(StreamProtocol.HeaderLength)

    inline val KeepOpen = true
    inline val Close = false

    /** Handles readability. Returns false when the connection should be closed. */
    def readEvent(r: Reactor): Boolean = {
      if state == State.SendResponse then return KeepOpen // Ignore stray read events while replying.
      val read = nioReadBytes(fd, buf)
      if read < 0 then
        println(s"Unexpected EOF from client socket $fd.")
        Close
      else
        state match
          case State.ReadHeader =>
            if buf.hasRemaining() then KeepOpen // Wait for the rest of the header.
            else
              val size = StreamProtocol.messageLength(buf)
              if size < 0 || size > StreamProtocol.MaxMessageLength then
                println(s"Invalid message size from client socket $fd: $size")
                Close
              else
                state = State.ReadMessage
                bodyRemaining = size
                readBody(r)
                KeepOpen
          case _ =>
            // Gather whatever arrived and ask for the rest, one buffer's worth at a time.
            buf.flip()
            bodyRemaining -= buf.remaining()
            drainTo(buf, body)
            readBody(r)
            KeepOpen
    }

    /** Expects the next chunk of the body, or responds once all of it has been gathered. */
    private def readBody(r: Reactor): Unit =
      if bodyRemaining == 0 then prepareResponse(r)
      else expect(math.min(bodyRemaining, buf.capacity()))

    /** Handles writability. Returns false once the whole response has been sent. */
    def writeEvent(): Boolean = {
      if state != State.SendResponse then return KeepOpen // ignore stray write events while not sending a response.
      nioWriteBytes(fd, buf)
      if buf.hasRemaining() then KeepOpen else Close
    }

    private def prepareResponse(r: Reactor): Unit = {
      println(s"message contents: `${body.toString(StandardCharsets.UTF_8)}`")
      state = State.SendResponse
      buf.clear()
      buf.put(Response)
      buf.flip()
      r.switchToWrite(fd)
    }

    /** Empties the buffer and limits it to the `length` bytes the current state will read. */
    private def expect(length: Int): Unit = buf.clear().limit(length)

    def close(): Unit =
      zone.close()
      PosixSockets.close(fd)
      println(s"Closed client socket: ${fd}")
  }

  private class Server(serverFd: Int) {
    private var connections: Map[Int, Connection] = Map.empty

    def accept(r: Reactor): Unit = {
      val clientFd = nioAccept(serverFd)
      if clientFd < 0 then {
        () // non blocking accept
      } else {
        connections += clientFd -> new Connection(clientFd)
        r.registerRead(clientFd)
        println(s"Accepted new client connection on serverFd: $clientFd")
      }
    }

    def connection(fd: Int): Option[Connection] = connections.get(fd)

    def drop(connection: Connection): Unit = {
      connections -= connection.fd
      // No EV_DELETE needed: kqueue(2) states that "calling close() on a file descriptor will remove any
      // kevents that reference the descriptor". The knote is keyed by descriptor number, not by the open file,
      // so this holds even if a dup() of the descriptor stays open (unlike epoll, which keys on the open file).
      connection.close()
    }

    def close(): Unit = {
      val old = connections
      connections = Map.empty
      old.values.foreach(_.close())
    }
  }

  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    Reactor.scoped { r =>
      address.withBoundSocket(Transport.Stream) { serverFd =>
        PosixSockets.listen(serverFd, PosixSockets.maxConnections)
        println("Socket is now listening for connections.")
        r.registerRead(serverFd)
        val server = new Server(serverFd)

        def serve(connection: Connection, event: Reactor.Event): Unit = {
          val keepOpen =
            try
              if r.isReadEvent(event) then connection.readEvent(r)
              else if r.isWriteEvent(event) then connection.writeEvent()
              else true
            catch {
              case e: IOException =>
                // One misbehaving client must not take the server down.
                println(s"Client socket ${connection.fd} failed: ${e.getMessage}")
                false
            }
          if !keepOpen then server.drop(connection)
        }

        try
          r.run(capacity = 255) { event =>
            val fd = r.fileIdent(event)
            if fd == serverFd then server.accept(r)
            else server.connection(fd).foreach(serve(_, event))
            true
          }
        finally server.close()
      }
    }
  }
}
