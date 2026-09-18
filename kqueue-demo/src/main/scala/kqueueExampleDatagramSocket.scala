package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.Bracket
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport

object KQueueExampleDatagramSocket {
  def run(sock: String, localSock: String, message: String): Unit = {
    require(sock != localSock, "The client and server socket paths must be different")
    // Unix datagram clients need their own bound address for the server's reply.
    KQueueExampleAddress.Unix(localSock).withBoundSocket(Transport.Datagram) { fd =>
      runConnected(fd, KQueueExampleAddress.Unix(sock), message)
    }
  }

  def run(address: KQueueExampleAddress, message: String): Unit = {
    require(address.flavor != asyncio.unsafe.Sockets.Flavor.Unix, "Unix datagrams need a local socket path")
    address.withSocket(Transport.Datagram) { fd =>
      // Connecting an IP datagram socket also assigns it an ephemeral local port.
      runConnected(fd, address, message)
    }
  }

  private def runConnected(fd: Int, address: KQueueExampleAddress, message: String): Unit = {
    address.connect(fd)
    println(s"Connected datagram socket $fd to $address")
    KqueueLoop.scoped { kq =>
      KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
        KqueueLoop.addFile(events(0), fd, read = false, clear = false)
      }
      Zone.acquire { implicit z =>
        val bytes = message.getBytes(StandardCharsets.UTF_8)
        // Allocate at least one byte so an empty message still has a valid pointer.
        val request = alloc[Byte](math.max(1, bytes.length))
        var n = 0
        while (n < bytes.length) {
          request(n) = bytes(n)
          n += 1
        }
        val response = alloc[Byte](65536)
        val sender = alloc[socket.sockaddr_storage]().asInstanceOf[Ptr[socket.sockaddr]]
        val senderLength = alloc[socket.socklen_t]()
        var sent = false
        KqueueLoop.pollQueue(1) { events =>
          var doPoll = true
          while (doPoll) {
            // UDP has no delivery guarantee; make a missing reply visible in the demo.
            if (KqueueLoop.pollEventsTimeout(kq, events, 1, 5, 0) == 0) {
              throw new IOException("Timed out waiting for datagram socket readiness or a reply")
            }
            val event = events(0)
            if (KqueueLoop.isError(event)) {
              throw new IOException(s"Datagram event error: ${KqueueLoop.errno(event)}")
            } else if (KqueueLoop.isWriteEvent(event) && !sent) {
              if (PosixSockets.sendDatagram(fd, request, bytes.length)) {
                println(s"Sent datagram of ${bytes.length} bytes.")
                sent = true
                KqueueLoop.createAndRegisterEvents(kq, 2) { changes =>
                  KqueueLoop.deleteFile(changes(0), fd, read = false)
                  KqueueLoop.addFile(changes(1), fd, read = true, clear = false)
                }
              }
            } else if (KqueueLoop.isReadEvent(event) && sent) {
              !senderLength = sizeOf[socket.sockaddr_storage].toUInt
              val read = PosixSockets.receiveDatagram(fd, response, 65536, sender, senderLength)
              if (read >= 0) {
                println(
                  s"Received datagram of $read bytes: `${fromCStringSlice(response, read.toCSize, StandardCharsets.UTF_8)}`"
                )
                doPoll = false
              }
            }
          }
        }
      }
    }
  }
}
