package example

import scala.scalanative.unsafe.Zone

import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleDatagramServerSocket {

  /** Echoes each packet to its sender, keeping the packet and its sender until the whole echo can be sent. */
  private final class Echo(fd: Int)(using Zone) {
    private val packet = NativeBuffer.allocate(65536)
    private val sender = new PeerAddress
    private var replying = false

    def awaitingPacket: Boolean = !replying

    /** Receives one packet; returns true once one is held for reply. */
    def receive(): Boolean = {
      val read = sender.receive(fd, packet)
      if read >= 0 then {
        println(s"Received datagram of $read bytes: `${text(packet)}`")
        replying = true
      }
      replying
    }

    /** Sends the held packet back; returns true once it has gone. */
    def reply(): Boolean = {
      if sender.reply(fd, packet) then {
        println(s"Replied with datagram of ${packet.limit()} bytes.")
        replying = false
      }
      !replying
    }
  }

  def run(address: KQueueExampleAddress): Unit = {
    Reactor.scoped { r =>
      address.withBoundSocket(Transport.Datagram) { fd =>
        r.registerRead(fd)
        println("Datagram socket is now waiting for messages.")
        Zone.acquire { implicit z =>
          val echo = new Echo(fd)
          r.run(capacity = 1) { event =>
            if r.isReadEvent(event) && echo.awaitingPacket then {
              if echo.receive() then r.switchToWrite(fd)
            } else if r.isWriteEvent(event) && !echo.awaitingPacket then {
              if echo.reply() then r.switchToRead(fd)
            }
            true
          }
        }
      }
    }
  }
}
