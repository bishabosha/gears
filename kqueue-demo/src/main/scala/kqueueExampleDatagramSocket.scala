package example

import java.io.IOException
import scala.scalanative.unsafe.Zone

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleDatagramSocket {
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
    KqueueLoop.scoped { kq =>
      registerWrite(kq, fd)
      Zone.acquire { implicit z =>
        val request = Frame.of(message)
        val response = new Frame(65536)
        val server = new PeerAddress
        var sent = false
        // UDP has no delivery guarantee; make a missing reply visible in the demo.
        val timedOut = () => throw new IOException("Timed out waiting for datagram socket readiness or a reply")
        pollLoop(kq, capacity = 1, timeoutSeconds = 5)(timedOut) { event =>
          if KqueueLoop.isWriteEvent(event) && !sent then {
            if sendDatagram(fd, request) then {
              println(s"Sent datagram of ${request.length} bytes.")
              sent = true
              switchToRead(kq, fd)
            }
            true
          } else if KqueueLoop.isReadEvent(event) && sent then {
            val read = server.receive(fd, response)
            if read >= 0 then println(s"Received datagram of $read bytes: `${response.text}`")
            read < 0 // Stop once the reply has arrived
          } else true
        }
      }
    }
  }
}
