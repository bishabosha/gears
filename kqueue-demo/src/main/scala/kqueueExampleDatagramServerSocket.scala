package example

import scala.scalanative.unsafe.Zone

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleDatagramServerSocket {
  def run(address: KQueueExampleAddress): Unit = {
    KqueueLoop.scoped { kq =>
      address.withBoundSocket(Transport.Datagram) { fd =>
        registerRead(kq, fd)
        println("Datagram socket is now waiting for messages.")
        Zone.acquire { implicit z =>
          // One packet and its sender are kept until the whole echo can be sent.
          val packet = new Frame(65536)
          val sender = new PeerAddress
          var replying = false
          pollLoop(kq, capacity = 1) { event =>
            if KqueueLoop.isReadEvent(event) && !replying then {
              val read = sender.receive(fd, packet)
              if read >= 0 then {
                println(s"Received datagram of $read bytes: `${packet.text}`")
                replying = true
                switchToWrite(kq, fd)
              }
            } else if KqueueLoop.isWriteEvent(event) && replying then {
              if sender.reply(fd, packet) then {
                println(s"Replied with datagram of ${packet.length} bytes.")
                replying = false
                switchToRead(kq, fd)
              }
            }
            true
          }
        }
      }
    }
  }
}
