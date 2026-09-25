package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Completion
import asyncio.reactor.Connect
import asyncio.reactor.Op
import asyncio.reactor.Reactor
import asyncio.reactor.ReadIntoBuffer
import asyncio.reactor.Resolve
import asyncio.reactor.WriteFromBuffer
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

/** Pulls a real reply off the internet with the WHOIS protocol (RFC 3912): resolve the host, connect to port 43, send
  * one query line, and print whatever comes back until the server closes the connection. Nothing is parsed.
  */
object KQueueExampleWhois {

  /** Resolves, connects, sends the query, then prints each chunk of the reply as it arrives. Built from library
    * commands only; the socket is opened once the address family is known.
    */
  private final class Lookup(host: String, port: Int, query: String)(using Zone) extends Completion[Op] {
    private val buf = NativeBuffer.allocate(8192)
    private var fd = -1
    private var received = 0L

    def start(r: Reactor): Unit = r.submit(new Resolve(host), this)

    def onComplete(r: Reactor, op: Op, result: Int): Unit = op match {
      case resolve: Resolve =>
        if result < 0 then throw new IOException(s"Failed to resolve $host: ${resolve.error}")
        val resolved = resolve.addresses.head
        val address = resolved.flavor match {
          case Flavor.IPv6 => KQueueExampleAddress.IPv6(resolved.host, port)
          case _           => KQueueExampleAddress.IPv4(resolved.host, port)
        }
        println(s"resolved $host to $address (${resolve.addresses.length} addresses)")
        fd = PosixSockets.open(address.flavor, Transport.Stream)
        PosixSockets.setNonBlocking(fd)
        address.connect(fd)
        r.submit(Connect(fd), this)
      case Connect(_) =>
        println(s"connected; sending query `$query`")
        buf.clear()
        buf.put(s"$query\r\n".getBytes(StandardCharsets.UTF_8))
        buf.flip()
        r.submit(WriteFromBuffer(fd, buf), this)
      case write: WriteFromBuffer =>
        if buf.hasRemaining() then r.submit(write, this)
        else {
          buf.clear()
          r.submit(ReadIntoBuffer(fd, buf), this)
        }
      case read: ReadIntoBuffer =>
        if result < 0 then {
          println(s"--- server closed the connection after $received bytes")
          PosixSockets.close(fd)
          r.stop()
        } else {
          if result > 0 then {
            received += result
            buf.flip()
            print(text(buf)) // Raw reply text, exactly as received.
            buf.clear()
          }
          r.submit(read, this)
        }
      case _ => ()
    }
  }

  def run(host: String, port: Int, query: String): Unit = {
    Reactor.scoped { r =>
      Zone.acquire { implicit z =>
        new Lookup(host, port, query).start(r)
        r.run(capacity = 1)
      }
    }
  }

  /** Resolves a host on the reactor and prints every address, as a check of the resolver itself. */
  def resolve(host: String): Unit = {
    Reactor.scoped { r =>
      r.submit(
        new Resolve(host),
        (r, resolve: Resolve, result) => {
          if result < 0 then throw new IOException(s"Failed to resolve $host: ${resolve.error}")
          resolve.addresses.foreach { a =>
            val family = if a.flavor == Flavor.IPv4 then "IPv4" else "IPv6"
            println(s"$family ${a.host}")
          }
          println(s"resolved $host to ${resolve.addresses.length} addresses")
          r.stop()
        }
      )
      r.run(capacity = 1)
    }
  }
}
