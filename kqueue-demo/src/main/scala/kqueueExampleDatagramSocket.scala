package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Command
import asyncio.reactor.Completion
import asyncio.reactor.Interest
import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleDatagramSocket {

  /** Sends one packet to the connected peer and collects the echo. */
  private final class Ping(fd: Int, message: String)(using Zone) extends Completion[Command] {
    private val request = NativeBuffer.of(message.getBytes(StandardCharsets.UTF_8))
    private val response = NativeBuffer.allocate(65536)
    private val server = new PeerAddress
    private var sent = false

    /** Sends the request if it has not gone yet; the result is 1 once it has been sent. */
    private object Send extends Command {
      def fd: Int = Ping.this.fd
      def interest: Interest = Interest.Write
      def perform(): Int = {
        if !sent && sendDatagram(fd, request) then {
          println(s"Sent datagram of ${request.limit()} bytes.")
          sent = true
        }
        if sent then 1 else 0
      }
    }

    /** Receives the echo; the result is 1 once it has arrived. */
    private object Receive extends Command {
      def fd: Int = Ping.this.fd
      def interest: Interest = Interest.Read
      def perform(): Int = {
        val read = server.receive(fd, response)
        if read >= 0 then println(s"Received datagram of $read bytes: `${text(response)}`")
        if read >= 0 then 1 else 0
      }
    }

    def start(r: Reactor): Unit = r.submit(Send, this)

    def onComplete(r: Reactor, command: Command, result: Int): Unit = command match {
      case Send    => if result == 1 then r.submit(Receive, this) else r.submit(Send, this)
      case Receive => if result == 1 then r.stop() else r.submit(Receive, this) // Stop once the reply has arrived
      case _       => ()
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
      Zone.acquire { implicit z =>
        val ping = new Ping(fd, message)
        ping.start(r)
        // UDP has no delivery guarantee; make a missing reply visible in the demo.
        val timedOut = () => throw new IOException("Timed out waiting for datagram socket readiness or a reply")
        r.run(capacity = 1, timeoutSeconds = 5)(timedOut)
      }
    }
  }
}
