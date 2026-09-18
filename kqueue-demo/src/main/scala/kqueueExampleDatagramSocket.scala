package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleDatagramSocket {

  /** Sends one packet to the connected peer and collects the echo. */
  private final class Ping(fd: Int, message: String)(using Zone) {
    private val request = NativeBuffer.of(message.getBytes(StandardCharsets.UTF_8))
    private val response = NativeBuffer.allocate(65536)
    private val server = new PeerAddress
    private var sent = false

    /** Sends the request if it has not gone yet; returns true once it has been sent. */
    def send(): Boolean = {
      if !sent && sendDatagram(fd, request) then {
        println(s"Sent datagram of ${request.limit()} bytes.")
        sent = true
      }
      sent
    }

    def awaitingReply: Boolean = sent

    /** Receives the echo; returns true once it has arrived. */
    def receive(): Boolean = {
      val read = server.receive(fd, response)
      if read >= 0 then println(s"Received datagram of $read bytes: `${text(response)}`")
      read >= 0
    }
  }

  def run(sock: String, localSock: String, message: String): Unit = {
    require(sock != localSock, "The client and server socket paths must be different")
    // Unix datagram clients need their own bound address for the server's reply.
    KQueueExampleAddress.Unix(localSock).withBoundSocket(Transport.Datagram) { fd =>
      runConnected(fd, KQueueExampleAddress.Unix(sock), message)
    }
  }

  def run(address: KQueueExampleAddress, message: String): Unit = {
    require(address.flavor != Flavor.Unix, "Unix datagrams need a local socket path")
    address.withSocket(Transport.Datagram) { fd =>
      // Connecting an IP datagram socket also assigns it an ephemeral local port.
      runConnected(fd, address, message)
    }
  }

  private def runConnected(fd: Int, address: KQueueExampleAddress, message: String): Unit = {
    address.connect(fd)
    println(s"Connected datagram socket $fd to $address")
    Reactor.scoped { r =>
      r.registerWrite(fd)
      Zone.acquire { implicit z =>
        val ping = new Ping(fd, message)
        // UDP has no delivery guarantee; make a missing reply visible in the demo.
        val timedOut = () => throw new IOException("Timed out waiting for datagram socket readiness or a reply")
        r.run(capacity = 1, timeoutSeconds = 5)(timedOut) { event =>
          if r.isWriteEvent(event) && !ping.awaitingReply then {
            if ping.send() then r.switchToRead(fd)
            true
          } else if r.isReadEvent(event) && ping.awaitingReply then {
            !ping.receive() // Stop once the reply has arrived
          } else true
        }
      }
    }
  }
}
