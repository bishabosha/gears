package gears.async.io
package testkit

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException

/** What a backend with IP datagram sockets must do (see `ReactorContract`), with or without IP stream sockets. */
trait IpDatagramContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & IpDatagramBackend

  /** A datagram socket bound to `local`, from the `bindDatagram` op, whose failure is thrown. */
  protected def bindDatagram(r: R, local: IpAddress)(using munit.Location): BoundDatagram = {
    val into = Slot[BoundDatagram]()
    opened(r, into, backend.ops.bindDatagram(local, into))
  }

  /** A datagram socket connected to `remote`, from the `connectDatagram` op, whose failure is thrown. */
  protected def connectDatagram(r: R, local: IpAddress | Null, remote: IpAddress)(using
      munit.Location
  ): ConnectedDatagram = {
    val into = Slot[ConnectedDatagram]()
    opened(r, into, backend.ops.connectDatagram(local, remote, into))
  }

  test("an IPv4 datagram goes to a bound socket, which learns its sender, and its reply to the connected client") {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val client = connectDatagram(r, null, IpAddress.IPv4("127.0.0.1", port))
        try {
          val buf = allocate(64)
          val from = Slot[IpAddress]()
          val received = Outcome()
          r.submit(backend.ops.receive(server, buf, from), received)
          val sent = Outcome()
          r.submit(backend.ops.send(client, bytes("datagram")), sent)
          turnUntil(r)(received.finished && sent.finished)
          assertEquals(received.completed, 1, String.valueOf(received.failure))
          val copy = new Array[Byte](buf.remaining())
          buf.get(copy)
          assertEquals(new String(copy, StandardCharsets.UTF_8), "datagram")
          val sender = from.clear()
          sender match {
            case IpAddress.IPv4(host, senderPort) =>
              assertEquals(host, "127.0.0.1")
              assert(senderPort > 0)
            case other => fail(s"unexpected sender $other")
          }
          assertEquals(run(r, backend.ops.send(server, bytes("reply"), sender)), null)
          val reply = allocate(64)
          assertEquals(run(r, backend.ops.receive(client, reply)), null)
          assertEquals(StandardCharsets.UTF_8.decode(reply).toString, "reply")
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("datagram sends: never empty when connected, empty ones from a bound socket, and no lost packets") {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      val serverAddress = IpAddress.IPv4("127.0.0.1", port)
      try {
        val connected = connectDatagram(r, null, serverAddress)
        val unconnected = bindDatagram(r, IpAddress.IPv4("127.0.0.1", 0))
        try {
          def refused(failure: Throwable | Null)(using munit.Location): Unit =
            assert(failure.isInstanceOf[IllegalArgumentException], String.valueOf(failure))

          /** The next packet the server receives, as text. */
          def received()(using munit.Location): String = {
            val buf = allocate(16)
            assertEquals(run(r, backend.ops.receive(server, buf, Slot[IpAddress]())), null)
            val copy = new Array[Byte](buf.remaining())
            buf.get(copy)
            new String(copy, StandardCharsets.UTF_8)
          }
          val empty = allocate(4)
          empty.limit(0)
          refused(run(r, backend.ops.send(connected, empty)))
          assertEquals(run(r, backend.ops.send(connected, bytes("to-peer"))), null)
          assertEquals(received(), "to-peer")
          assertEquals(run(r, backend.ops.send(unconnected, empty, serverAddress)), null)
          assertEquals(received(), "", "an empty packet from a socket that is not connected")
          assertEquals(run(r, backend.ops.send(connected, bytes("kept"))), null)
          val none = allocate(4)
          none.limit(0)
          refused(run(r, backend.ops.receive(server, none.slice(), Slot[IpAddress]())))
          assertEquals(received(), "kept", "the refused receive took no packet")
        } finally {
          close(r, connected)
          close(r, unconnected)
        }
      } finally close(r, server)
    }
  }

  test("a bound socket sends to peers that have not sent first from its own port, on its own port 0 or not") {
    withReactor { r =>
      val (first, firstPort) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      val (second, secondPort) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        def senderOf(to: BoundDatagram)(using munit.Location): Int = {
          val buf = allocate(16)
          val from = Slot[IpAddress]()
          assertEquals(run(r, backend.ops.receive(to, buf, from)), null)
          from.clear() match {
            case IpAddress.IPv4(_, port) => port
            case other                   => fail(s"unexpected sender $other")
          }
        }
        val (fixed, fixedPort) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
        try {
          assertEquals(run(r, backend.ops.send(fixed, bytes("one"), IpAddress.IPv4("127.0.0.1", firstPort))), null)
          assertEquals(senderOf(first), fixedPort)
          assertEquals(run(r, backend.ops.send(fixed, bytes("two"), IpAddress.IPv4("127.0.0.1", secondPort))), null)
          assertEquals(senderOf(second), fixedPort)
        } finally close(r, fixed)
        val any = bindDatagram(r, IpAddress.IPv4("127.0.0.1", 0))
        try {
          assertEquals(run(r, backend.ops.send(any, bytes("one"), IpAddress.IPv4("127.0.0.1", firstPort))), null)
          val port = senderOf(first)
          assertEquals(run(r, backend.ops.send(any, bytes("two"), IpAddress.IPv4("127.0.0.1", secondPort))), null)
          assertEquals(senderOf(second), port, "one port for every peer")
        } finally close(r, any)
      } finally {
        close(r, first)
        close(r, second)
      }
    }
  }

  test("a packet lost on a socket that is not connected fails no send, to its peer or another") {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        // A port nothing listens on, which loopback answers with port unreachable.
        val (gone, gonePort) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
        close(r, gone)
        val nowhere = IpAddress.IPv4("127.0.0.1", gonePort)
        val unconnected = bindDatagram(r, IpAddress.IPv4("127.0.0.1", 0))
        try {
          for _ <- 1 to 3 do {
            assertEquals(run(r, backend.ops.send(unconnected, bytes("lost"), nowhere)), null)
            assertEquals(run(r, backend.ops.timer(20)), null) // for the unreachable to come back
          }
          assertEquals(run(r, backend.ops.send(unconnected, bytes("after"), IpAddress.IPv4("127.0.0.1", port))), null)
          val buf = allocate(16)
          assertEquals(run(r, backend.ops.receive(server, buf, Slot[IpAddress]())), null)
          assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "after")
          assertEquals(run(r, backend.ops.send(unconnected, bytes("again"), nowhere)), null)
        } finally close(r, unconnected)
      } finally close(r, server)
    }
  }

  test(
    "over IPv6 a datagram's sender is written as inet_ntop does, whatever the spelling its peer was connected by"
  ) {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv6("::1", port)))
      try {
        val client = connectDatagram(r, null, IpAddress.IPv6("0:0:0:0:0:0:0:1", port))
        try {
          assertEquals(run(r, backend.ops.send(client, bytes("v6"))), null)
          val buf = allocate(16)
          val from = Slot[IpAddress]()
          assertEquals(run(r, backend.ops.receive(server, buf, from)), null)
          assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "v6")
          from.clear() match {
            case IpAddress.IPv6(host, _) => assertEquals(host, "::1")
            case other                   => fail(s"an IPv6 sender: $other")
          }
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("a packet larger than the receive buffer fills it, the rest dropped, and the next packet arrives whole") {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val client = connectDatagram(r, null, IpAddress.IPv4("127.0.0.1", port))
        try {
          assertEquals(run(r, backend.ops.send(client, bytes("0123456789abcdef"))), null)
          assertEquals(run(r, backend.ops.send(client, bytes("next"))), null)
          def received(capacity: Int)(using munit.Location): String = {
            val buf = allocate(capacity)
            assertEquals(run(r, backend.ops.receive(server, buf, Slot[IpAddress]())), null)
            val copy = new Array[Byte](buf.remaining())
            buf.get(copy)
            new String(copy, StandardCharsets.UTF_8)
          }
          assertEquals(received(4), "0123", "the bytes that fitted, the rest dropped")
          assertEquals(received(16), "next", "the next packet, whole")
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("datagrams not received are dropped once the socket's buffer is full, not queued without bound") {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val client = connectDatagram(r, null, IpAddress.IPv4("127.0.0.1", port))
        try {
          val sent = 2000 // of 8 KiB: 16 MiB, beyond any socket's receive buffer
          val packet = allocate(8192)
          for _ <- 1 to sent do {
            packet.clear()
            assertEquals(run(r, backend.ops.send(client, packet)), null)
          }
          val buf = allocate(8192)
          var received = 0
          var drained = false
          while !drained do {
            buf.clear()
            val outcome = Outcome()
            r.submit(backend.ops.receive(server, buf, Slot[IpAddress]()), outcome, timeoutMillis = 200)
            turnUntil(r)(outcome.finished)
            if outcome.failure != null then {
              assert(outcome.failure.isInstanceOf[TimeoutException], String.valueOf(outcome.failure))
              drained = true
            } else received += 1
          }
          assert(received > 0 && received < sent, s"$received of $sent received")
          assertEquals(run(r, backend.ops.send(client, bytes("after"))), null)
          buf.clear()
          assertEquals(run(r, backend.ops.receive(server, buf, Slot[IpAddress]())), null)
          assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "after", "the socket goes on once drained")
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("a datagram socket's op completes once it is bound, and fails, leaving its slot empty, on an address in use") {
    withReactor { r =>
      val (server, port) = onFreePort(port => bindDatagram(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val into = Slot[BoundDatagram]()
        val failure = run(r, backend.ops.bindDatagram(IpAddress.IPv4("127.0.0.1", port), into))
        assert(failure.isInstanceOf[IOException], String.valueOf(failure))
        intercept[IllegalStateException](into.clear())
        val client = connectDatagram(r, null, IpAddress.IPv4("127.0.0.1", port))
        close(r, client)
      } finally close(r, server)
    }
  }

  /** The bytes a pooled read received, as text, consuming them. */
  test("datagram sockets know their addresses: a bound one on port 0 its port, a connected one its own and its peer") {
    withReactor { r =>
      val bound = bindDatagram(r, IpAddress.IPv4("127.0.0.1", 0))
      try {
        val local = backend.ops.localAddress(bound)
        local match {
          case IpAddress.IPv4("127.0.0.1", port) => assert(port > 0, local)
          case other                             => fail(s"a bound socket on port 0's address: $other")
        }
        val client = connectDatagram(r, null, local)
        try {
          assertEquals(backend.ops.peerAddress(client), local)
          val clientAddress = backend.ops.localAddress(client)
          assertEquals(run(r, backend.ops.send(client, bytes("who"))), null)
          val from = Slot[IpAddress]()
          assertEquals(run(r, backend.ops.receive(bound, allocate(16), from)), null)
          assertEquals(from.clear(), clientAddress, "its sender is the address it reports")
        } finally close(r, client)
      } finally close(r, bound)
      intercept[IOException](backend.ops.localAddress(bound))
    }
  }

  test("a datagram socket refuses a stream socket's options, and takes a buffer size where the backend has it") {
    intercept[IllegalArgumentException](
      backend.ops
        .bindDatagram(IpAddress.IPv4("127.0.0.1", 0), SocketOptions.default.set(SocketOption.NoDelay, true), Slot())
    )
    if backend.ops.supports(SocketOption.ReceiveBuffer) then
      withReactor { r =>
        val into = Slot[BoundDatagram]()
        val options = SocketOptions.default.set(SocketOption.ReceiveBuffer, 65536)
        val bound = opened(r, into, backend.ops.bindDatagram(IpAddress.IPv4("127.0.0.1", 0), options, into))
        try {
          assert(backend.ops.option(bound, SocketOption.ReceiveBuffer) >= 65536)
          intercept[IllegalArgumentException](backend.ops.option(bound, SocketOption.NoDelay))
        } finally close(r, bound)
      }
    else
      intercept[IllegalArgumentException](
        backend.ops.bindDatagram(
          IpAddress.IPv4("127.0.0.1", 0),
          SocketOptions.default.set(SocketOption.ReceiveBuffer, 65536),
          Slot()
        )
      )
  }
}
