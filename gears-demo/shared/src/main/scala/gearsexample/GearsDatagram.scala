package gearsexample

import gears.async.Async
import gears.async.Future

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import asyncio.Backend
import asyncio.IpAddress
import asyncio.UnixAddress
import asyncio.UnixDatagramBackend
import asyncio.Slot
import asyncio.testkit.BufferArena
import DemoIO.text
import asyncio.gears.ReactorFutures
import asyncio.gears.ReactorFutures.*

/** Mirrors of `example.KQueueExampleDatagramSocket` and `example.KQueueExampleDatagramServerSocket`, using the
  * reactor's `receive` and `send` ops.
  */
object GearsDatagram {

  // Client

  /** Exchanges a packet with a Unix datagram server; a Unix datagram client needs its own bound path for the reply. */
  def run(sock: String, localSock: String, message: String)(using UnixDatagramBackend): Unit = {
    require(sock != localSock, "The client and server socket paths must be different")
    BufferArena.scoped { arena =>
      val request = arena.of(message.getBytes(StandardCharsets.UTF_8))
      val response = arena.allocate(65536)
      ReactorFutures.run { r =>
        val socket = r.handles.datagram(UnixAddress(localSock), UnixAddress(sock))
        try exchange(r, sock, r.ops.send(socket, request), r.ops.receive(socket, response, Slot[UnixAddress]()), request, response)
        finally r.handles.close(socket)
      }
    }
  }

  /** Exchanges a packet with an IP datagram server; connecting the socket also assigns it an ephemeral local port. */
  def run(address: IpAddress, message: String)(using Backend): Unit = {
    BufferArena.scoped { arena =>
      val request = arena.of(message.getBytes(StandardCharsets.UTF_8))
      val response = arena.allocate(65536)
      ReactorFutures.run { r =>
        val socket = r.handles.datagram(null, address)
        try exchange(r, address, r.ops.send(socket, request), r.ops.receive(socket, response, Slot[IpAddress]()), request, response)
        finally r.handles.close(socket)
      }
    }
  }

  /** Sends `request` with `send`, then waits for the reply with `receive`, both repeatable ops resubmitted until done. */
  private def exchange(r: Backend, remote: Any, send: r.Op, receive: r.Op, request: ByteBuffer, response: ByteBuffer)(
      using Async
  ): Unit = {
    println(s"Connected datagram socket to $remote")
    // Scoped, so the exchange is cancelled and settled before the socket is closed.
    Async.group {
      val ping = Future {
        perform(r, send) // completes once the whole packet has gone
        println(s"Sent datagram of ${request.limit()} bytes.")
        perform(r, receive) // completes once a packet has arrived
        println(s"Received datagram of ${response.remaining()} bytes: `${text(response)}`")
      }
      // UDP has no delivery guarantee; make a missing reply visible in the demo.
      val deadline = Future(perform(r, r.ops.timer(5000)))
      Async.select(
        ping.handle(_.get),
        deadline.handle(_ => throw new IOException("Timed out waiting for datagram socket readiness or a reply"))
      )
    }
  }

  // Server

  /** Echoes each packet on a Unix datagram socket to its sender. */
  def serve(address: UnixAddress)(using UnixDatagramBackend): Unit =
    BufferArena.scoped { arena =>
      val packet = arena.allocate(65536)
      ReactorFutures.run { r =>
        val socket = r.handles.datagram(address, null)
        val sender = Slot[UnixAddress]() // each packet's sender, written alongside the packet itself
        try echo(r, r.ops.receive(socket, packet, sender), packet, () => r.ops.send(socket, packet, sender.clear()))
        finally r.handles.close(socket)
      }
    }

  /** Echoes each packet on an IP datagram socket to its sender. */
  def serve(address: IpAddress)(using Backend): Unit =
    BufferArena.scoped { arena =>
      val packet = arena.allocate(65536)
      ReactorFutures.run { r =>
        val socket = r.handles.datagram(address, null)
        val sender = Slot[IpAddress]()
        try echo(r, r.ops.receive(socket, packet, sender), packet, () => r.ops.send(socket, packet, sender.clear()))
        finally r.handles.close(socket)
      }
    }

  /** Receives packets with `receive`, one repeatable op, and echoes each, keeping it until the whole echo has gone,
    * with a new op per sender from `reply`.
    */
  private def echo(r: Backend, receive: r.Op, packet: ByteBuffer, reply: () => r.Op)(using Async): Unit = {
    println("Datagram socket is now waiting for messages.")
    while true do {
      perform(r, receive)
      val size = packet.remaining()
      println(s"Received datagram of $size bytes: `${text(packet)}`")
      perform(r, reply())
      println(s"Replied with datagram of $size bytes.")
    }
  }
}
