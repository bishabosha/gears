package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.Bracket
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport

object KQueueExampleServerSocket {
  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    Bracket.fileResource(KqueueLoop.open())(KqueueLoop.close) { kq =>
      address.withBoundSocket(Transport.Stream) { serverFd =>
        PosixSockets.listen(serverFd, PosixSockets.maxConnections)
        println("Socket is now listening for connections.")
        KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
          KqueueLoop.addFile(events(0), serverFd, read = true, clear = false)
        }
        object states {
          type EventState = Int
          val ReadClientHeader: EventState = 0
          val ReadClientMessage: EventState = 1
          val SendResponse: EventState = 2
        }
        class Client {
          var state: states.EventState = states.ReadClientHeader
          var buffer = new Array[Byte](4)
          var offset = 0
        }
        var clients: Map[Int, Client] = Map.empty
        val response = "Just pinging back!\n".getBytes(StandardCharsets.UTF_8)

        def wouldBlock: Boolean = errno.errno == errno.EAGAIN || errno.errno == errno.EWOULDBLOCK

        def closeClient(clientFd: Int): Unit = {
          clients -= clientFd
          // Closing a descriptor also removes its kqueue events.
          PosixSockets.close(clientFd)
          println(s"Closed client socket: $clientFd")
        }

        def acceptConnection(): Unit = {
          val clientFd = socket.accept(serverFd, null, null)
          if (clientFd < 0) {
            if (!wouldBlock && errno.errno != errno.EINTR) {
              throw new IOException(s"Failed to accept connection: ${cError()}")
            }
          } else {
            clients += (clientFd -> new Client)
            PosixSockets.setNonBlocking(clientFd)
            KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
              KqueueLoop.addFile(events(0), clientFd, read = true, clear = false)
            }
            println(s"Accepted new client connection on serverFd: $clientFd")
          }
        }

        def prepareResponse(clientFd: Int, client: Client): Unit = {
          println(s"message contents: `${new String(client.buffer, StandardCharsets.UTF_8)}`")
          client.state = states.SendResponse
          client.buffer = response
          client.offset = 0
          KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
            KqueueLoop.deleteFile(events(0), clientFd, read = true)
            KqueueLoop.addFile(events(1), clientFd, read = false, clear = false)
          }
        }

        def clientRead(clientFd: Int, client: Client): Unit = {
          // Read only the remaining header or body, even if both arrived together.
          val read =
            unistd.read(clientFd, client.buffer.at(client.offset), (client.buffer.length - client.offset).toCSize)
          if (read < 0) {
            if (!wouldBlock && errno.errno != errno.EINTR) {
              println(s"Failed to read from client socket $clientFd: ${cError()}")
              closeClient(clientFd)
            }
          } else if (read == 0) {
            println(s"Unexpected EOF from client socket $clientFd.")
            closeClient(clientFd)
          } else {
            client.offset += read.toInt
            if (client.offset == client.buffer.length) {
              if (client.state == states.ReadClientHeader) {
                val buf = client.buffer
                val size = ((buf(0) & 0xff) << 24) | ((buf(1) & 0xff) << 16) |
                  ((buf(2) & 0xff) << 8) | (buf(3) & 0xff)
                if (size < 0 || size > 1024 * 1024) {
                  println(s"Invalid message size from client socket $clientFd: $size")
                  closeClient(clientFd)
                } else {
                  client.state = states.ReadClientMessage
                  client.buffer = new Array[Byte](size)
                  client.offset = 0
                  if (size == 0) prepareResponse(clientFd, client)
                }
              } else {
                prepareResponse(clientFd, client)
              }
            }
          }
        }

        def clientWrite(clientFd: Int, client: Client): Unit = {
          val written =
            unistd.write(clientFd, client.buffer.at(client.offset), (client.buffer.length - client.offset).toCSize)
          if (written < 0) {
            if (!wouldBlock && errno.errno != errno.EINTR) {
              println(s"Failed to write to client socket $clientFd: ${cError()}")
              closeClient(clientFd)
            }
          } else {
            client.offset += written.toInt
            if (client.offset == client.buffer.length) closeClient(clientFd)
          }
        }

        try {
          KqueueLoop.pollQueue(255) { polledEvents =>
            while (true) {
              val events = KqueueLoop.pollEventsForever(kq, polledEvents, 255)
              var i = 0
              while (i < events) {
                val evt = polledEvents(i)
                val fd = KqueueLoop.fileIdent(evt)
                if (KqueueLoop.isError(evt)) {
                  throw new IOException(s"Socket event error: ${KqueueLoop.errno(evt)}")
                } else if (KqueueLoop.isReadEvent(evt) && fd == serverFd) {
                  acceptConnection()
                } else {
                  val clientFd = fd
                  clients.get(clientFd).foreach { client =>
                    if (KqueueLoop.isReadEvent(evt) && client.state != states.SendResponse) {
                      clientRead(clientFd, client)
                    } else if (KqueueLoop.isWriteEvent(evt) && client.state == states.SendResponse) {
                      clientWrite(clientFd, client)
                    }
                  }
                }
                i += 1
              }
            }
          }
        } finally {
          clients.keys.foreach(PosixSockets.close)
        }
      }
    }
  }
}
