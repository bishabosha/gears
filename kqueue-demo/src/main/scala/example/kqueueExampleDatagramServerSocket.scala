package example

import scala.scalanative.unsafe.Zone

import asyncio.reactor.Command
import asyncio.reactor.Completion
import asyncio.reactor.Interest
import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleDatagramServerSocket {

  /** Echoes each packet to its sender, keeping the packet and its sender until the whole echo can be sent. */
  private final class Echo(fd: Int)(using Zone) extends Completion[Command] {
    private val packet = NativeBuffer.allocate(65536)
    private val sender = new PeerAddress

    /** Receives one packet; the result is 1 once one is held for reply. */
    private object Receive extends Command {
      def fd: Int = Echo.this.fd
      def interest: Interest = Interest.Read
      def perform(): Int = {
        val read = sender.receive(fd, packet)
        if read >= 0 then println(s"Received datagram of $read bytes: `${text(packet)}`")
        if read >= 0 then 1 else 0
      }
    }

    /** Sends the held packet back; the result is 1 once it has gone. */
    private object Reply extends Command {
      def fd: Int = Echo.this.fd
      def interest: Interest = Interest.Write
      def perform(): Int = {
        val sent = sender.reply(fd, packet)
        if sent then println(s"Replied with datagram of ${packet.limit()} bytes.")
        if sent then 1 else 0
      }
    }

    def start(r: Reactor): Unit = r.submit(Receive, this)

    def onComplete(r: Reactor, command: Command, result: Int): Unit = command match {
      case Receive => if result == 1 then r.submit(Reply, this) else r.submit(Receive, this)
      case Reply   => if result == 1 then r.submit(Receive, this) else r.submit(Reply, this)
      case _       => ()
    }
  }

  def run(address: KQueueExampleAddress): Unit = {
    Reactor.scoped { r =>
      address.withBoundSocket(Transport.Datagram) { fd =>
        println("Datagram socket is now waiting for messages.")
        Zone.acquire { implicit z =>
          val echo = new Echo(fd)
          echo.start(r)
          r.run(capacity = 1)
        }
      }
    }
  }
}
