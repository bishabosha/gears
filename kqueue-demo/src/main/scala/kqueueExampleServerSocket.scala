package example

import java.io.IOException
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleServerSocket {
  private val Response = "Just pinging back!\n"

  private enum State {
    case ReadHeader, ReadMessage, SendResponse
  }

  /** One accepted connection: reads a length-framed request, then replies and closes. */
  private final class Connection(val fd: Int) {
    private var state = State.ReadHeader
    private var frame = new Frame(StreamProtocol.HeaderLength)

    inline val KeepOpen = true
    inline val Close = false

    /** Handles readability. Returns false when the connection should be closed. */
    def readEvent(kq: Int): Boolean = {
      if state == State.SendResponse then return KeepOpen // Ignore stray read events while replying.
      val read = nioReadBytes(fd, frame)
      if read < 0 then
        println(s"Unexpected EOF from client socket $fd.")
        Close
      else if frame.remaining > 0 then KeepOpen // Wait for the rest of the header or body.
      else
        // frame is fully read, process it
        state match
          case State.ReadHeader =>
            val size = StreamProtocol.messageLength(frame)
            if size < 0 || size > StreamProtocol.MaxMessageLength then
              println(s"Invalid message size from client socket $fd: $size")
              Close
            else
              state = State.ReadMessage
              frame = new Frame(size)
              if size == 0 then
                // no more message body to read
                prepareResponse(kq)
              KeepOpen
          case _ =>
            prepareResponse(kq)
            KeepOpen
    }

    /** Handles writability. Returns false once the whole response has been sent. */
    def writeEvent(): Boolean = {
      if state != State.SendResponse then return KeepOpen // ignore stray write events while not sending a response.
      nioWriteBytes(fd, frame)
      if frame.isEmpty then Close else KeepOpen
    }

    private def prepareResponse(kq: Int): Unit = {
      println(s"message contents: `${frame.text}`")
      state = State.SendResponse
      frame = Frame.of(Response)
      switchToWrite(kq, fd)
    }
  }

  private class Server(serverFd: Int) {
    private var connections: Map[Int, Connection] = Map.empty

    def accept(kq: Int): Unit = {
      val clientFd = nioAccept(serverFd)
      if clientFd < 0 then {
        () // non blocking accept
      } else {
        connections += clientFd -> new Connection(clientFd)
        registerRead(kq, clientFd)
        println(s"Accepted new client connection on serverFd: $clientFd")
      }
    }

    def connection(fd: Int): Option[Connection] = connections.get(fd)

    def close(connection: Connection): Unit = {
      connections -= connection.fd
      // No EV_DELETE needed: kqueue(2) states that "calling close() on a file descriptor will remove any
      // kevents that reference the descriptor". The knote is keyed by descriptor number, not by the open file,
      // so this holds even if a dup() of the descriptor stays open (unlike epoll, which keys on the open file).
      PosixSockets.close(connection.fd)
      println(s"Closed client socket: ${connection.fd}")
    }

    def close(): Unit = {
      connections.values.foreach(close)
    }
  }

  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    KqueueLoop.scoped { kq =>
      address.withBoundSocket(Transport.Stream) { serverFd =>
        PosixSockets.listen(serverFd, PosixSockets.maxConnections)
        println("Socket is now listening for connections.")
        registerRead(kq, serverFd)
        val server = new Server(serverFd)

        def serve(connection: Connection, event: KqueueLoop.Event): Unit = {
          val keepOpen =
            try
              if KqueueLoop.isReadEvent(event) then connection.readEvent(kq)
              else if KqueueLoop.isWriteEvent(event) then connection.writeEvent()
              else true
            catch {
              case e: IOException =>
                // One misbehaving client must not take the server down.
                println(s"Client socket ${connection.fd} failed: ${e.getMessage}")
                false
            }
          if !keepOpen then server.close(connection)
        }

        try
          pollLoop(kq, capacity = 255) { event =>
            val fd = KqueueLoop.fileIdent(event)
            if fd == serverFd then server.accept(kq)
            else server.connection(fd).foreach(serve(_, event))
            true
          }
        finally server.close()
      }
    }
  }
}
