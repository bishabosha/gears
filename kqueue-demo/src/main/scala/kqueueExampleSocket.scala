package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleSocket {

  /** A connecting stream socket; the connection is verified on its first readiness event. */
  final class ClientSock(val fd: Int) {
    private var connected = false

    def ensureConnected(): Unit = {
      if !connected then {
        PosixSockets.checkConnect(fd)
        connected = true
      }
    }
  }

  /** Sends one framed request, then reuses the same buffer to collect the response until the server closes the
    * connection.
    */
  final class Exchange(sock: ClientSock, message: String) {
    private val frame = new Frame()
    private val response = new ByteArrayOutputStream()
    StreamProtocol.frame(message, frame)

    def responseMessage: String = new String(response.toByteArray, StandardCharsets.UTF_8)

    /** Returns true once the whole request has been written; the buffer is then ready for reading. */
    def writeRequest(): Boolean = {
      val written = nioWriteBytes(sock.fd, frame)
      if written >= 0 then println(s"Wrote $written bytes to socket.")
      val sent = frame.isEmpty
      if sent then frame.clear()
      sent
    }

    /** Returns true once the server has closed the connection. */
    def readResponse(): Boolean = {
      val read = nioReadBytes(sock.fd, frame)
      if read > 0 then {
        response.write(frame.data, 0, read)
        println(s"Read $read bytes from socket.")
        frame.clear() // Each chunk is copied out, so the buffer starts over.
      }
      read < 0
    }
  }

  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    KqueueLoop.scoped { kq =>
      address.withSocket(Transport.Stream) { clientFd =>
        println(s"opened file descriptor: $clientFd (socket-type)")
        address.connect(clientFd)
        println(s"connection in progress to: $address")
        // Keep receiving write events until the entire frame has been sent.
        registerWrite(kq, clientFd)
        val sock = ClientSock(clientFd)
        val exchange = Exchange(sock, "Hello from Scala Native KQueue Example!\n")
        pollLoop(kq, capacity = 255) { event =>
          assert(KqueueLoop.fileIdent(event) == clientFd, "Unexpected file descriptor for event.")
          sock.ensureConnected()
          if KqueueLoop.isWriteEvent(event) then {
            if exchange.writeRequest() then switchToRead(kq, clientFd)
            true
          } else if KqueueLoop.isReadEvent(event) then {
            val eof = exchange.readResponse()
            if eof then println(s"Received message: `${exchange.responseMessage}`")
            !eof
          } else true
        }
      }
    }
  }
}
