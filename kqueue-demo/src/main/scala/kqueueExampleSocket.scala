package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Command
import asyncio.reactor.Completion
import asyncio.reactor.Connect
import asyncio.reactor.Reactor
import asyncio.reactor.ReadIntoBuffer
import asyncio.reactor.WriteFromBuffer
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleSocket {

  /** Connects, sends one framed request, then reuses the same buffer to collect the response until the server closes
    * the connection. Built entirely from the library's commands.
    */
  final class Exchange(fd: Int, message: String)(using Zone) extends Completion[Command] {
    private val buf = NativeBuffer.allocate(8192)
    private val response = new ByteArrayOutputStream()

    def responseMessage: String = new String(response.toByteArray, StandardCharsets.UTF_8)

    def start(r: Reactor): Unit = r.submit(Connect(fd), this)

    def onComplete(r: Reactor, command: Command, result: Int): Unit = command match {
      case Connect(_) =>
        StreamProtocol.frame(message, buf)
        r.submit(WriteFromBuffer(fd, buf), this)
      case write: WriteFromBuffer =>
        if result >= 0 then println(s"Wrote $result bytes to socket.")
        if buf.hasRemaining() then r.submit(write, this)
        else {
          buf.clear() // The request is out; the buffer now collects the response.
          r.submit(ReadIntoBuffer(fd, buf), this)
        }
      case read: ReadIntoBuffer =>
        if result < 0 then {
          println(s"Received message: `$responseMessage`") // The server closes the connection after its response.
          r.stop()
        } else {
          if result > 0 then {
            buf.flip()
            drainTo(buf, response)
            println(s"Read $result bytes from socket.")
            buf.clear() // Each chunk is copied out, so the buffer starts over.
          }
          r.submit(read, this)
        }
      case _ => ()
    }
  }

  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    Reactor.scoped { r =>
      address.withSocket(Transport.Stream) { clientFd =>
        println(s"opened file descriptor: $clientFd (socket-type)")
        address.connect(clientFd)
        println(s"connection in progress to: $address")
        Zone.acquire { implicit z =>
          val exchange = Exchange(clientFd, "Hello from Scala Native KQueue Example!\n")
          exchange.start(r)
          r.run(capacity = 255)
        }
      }
    }
  }
}
