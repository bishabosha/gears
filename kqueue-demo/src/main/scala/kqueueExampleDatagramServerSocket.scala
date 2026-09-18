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

object KQueueExampleDatagramServerSocket {
  def run(address: KQueueExampleAddress): Unit = {
    KqueueLoop.scoped { kq =>
      address.withBoundSocket(Transport.Datagram) { fd =>
        KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
          KqueueLoop.addFile(events(0), fd, read = true, clear = false)
        }
        println("Datagram socket is now waiting for messages.")
        Zone.acquire { implicit z =>
          val buffer = alloc[Byte](65536)
          val sender = alloc[socket.sockaddr_storage]().asInstanceOf[Ptr[socket.sockaddr]]
          val senderLength = alloc[socket.socklen_t]()
          var messageSize = 0
          var sending = false
          KqueueLoop.pollQueue(1) { events =>
            while (true) {
              KqueueLoop.pollEventsForever(kq, events, 1)
              val event = events(0)
              if (KqueueLoop.isError(event)) {
                throw new IOException(s"Datagram event error: ${KqueueLoop.errno(event)}")
              } else if (KqueueLoop.isReadEvent(event) && !sending) {
                !senderLength = sizeOf[socket.sockaddr_storage].toUInt
                val read = PosixSockets.receiveDatagram(fd, buffer, 65536, sender, senderLength)
                if (read >= 0) {
                  println(
                    s"Received datagram of $read bytes: `${fromCStringSlice(buffer, read.toCSize, StandardCharsets.UTF_8)}`"
                  )
                  messageSize = read
                  sending = true
                  KqueueLoop.createAndRegisterEvents(kq, 2) { changes =>
                    KqueueLoop.deleteFile(changes(0), fd, read = true)
                    KqueueLoop.addFile(changes(1), fd, read = false, clear = false)
                  }
                }
              } else if (KqueueLoop.isWriteEvent(event) && sending) {
                // Keep this packet and its sender until the whole reply can be sent.
                if (PosixSockets.sendDatagram(fd, buffer, messageSize, sender, !senderLength)) {
                  println(s"Replied with datagram of $messageSize bytes.")
                  sending = false
                  KqueueLoop.createAndRegisterEvents(kq, 2) { changes =>
                    KqueueLoop.deleteFile(changes(0), fd, read = false)
                    KqueueLoop.addFile(changes(1), fd, read = true, clear = false)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
