package gears.async.io
package testkit

import java.io.IOException
import java.nio.charset.StandardCharsets

/** What a backend with IP sockets must do (see `ReactorContract`). */
trait SocketContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & SocketBackend

  /** A listener on `address`, listening, from the `listen` op, whose failure is thrown. */
  protected def listen(r: R, address: IpAddress, reuseAddr: Boolean = true)(using munit.Location): Handle = {
    val into = Slot[Handle]()
    opened(r, into, backend.ops.listen(address, into, reuseAddr))
  }

  /** A datagram socket from the `datagram` op, whose failure is thrown. */
  protected def datagram(r: R, local: IpAddress | Null, remote: IpAddress | Null)(using munit.Location): Handle = {
    val into = Slot[Handle]()
    opened(r, into, backend.ops.datagram(local, remote, into))
  }

  /** Submits a `connect` op to `address` on `r`: what it reports, and the slot the socket is written into. */
  protected def startConnect(r: R, address: IpAddress): (Outcome, Slot[Handle]) = {
    val into = Slot[Handle]()
    val connected = Outcome()
    r.submit(backend.ops.connect(address, into), connected)
    (connected, into)
  }

  /** Opens something on a free port: no interface tells which port was bound, so it tries a few. */
  protected def onFreePort(open: Int => Handle): (Handle, Int) = {
    val random = new java.util.Random()
    var attempts = 0
    var result: (Handle, Int) | Null = null
    while result == null do {
      val port = 20000 + random.nextInt(30000)
      try result = (open(port), port)
      catch {
        case e: IOException =>
          attempts += 1
          if attempts > 10 then throw e
      }
    }
    result
  }

  test("an IPv4 stream socket accepts, connects and echoes") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Handle]()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        turnUntil(r)(acceptDone.finished && connected.finished)
        assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        val client = into.clear()
        try {
          val server = accepted.clear()
          val sent = Outcome()
          r.submit(backend.ops.write(client, bytes("tcp ping")), sent)
          val buf = allocate(16)
          val got = Outcome()
          r.submit(backend.ops.read(server, buf), got)
          turnUntil(r)(sent.finished && got.finished)
          assertEquals(text(buf), "tcp ping")
          buf.flip()
          val echoed = Outcome()
          r.submit(backend.ops.write(server, buf), echoed)
          val reply = allocate(16)
          val replied = Outcome()
          r.submit(backend.ops.read(client, reply), replied)
          turnUntil(r)(echoed.finished && replied.finished)
          assertEquals(text(reply), "tcp ping")
          close(r, server)
          assertEquals(readToEnd(r, backend.ops.read(client, reply), reply), "tcp ping")
        } finally close(r, client)
      } finally close(r, listener)
    }
  }

  test("a stream shut down for writing sends what was written, then its end, and still reads") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Handle]()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        turnUntil(r)(acceptDone.finished && connected.finished)
        assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        val client = into.clear()
        val server = accepted.clear()
        try {
          // The client's request, then its end: the server reads it to the end, as it would a closed peer's.
          assertEquals(run(r, backend.ops.write(client, bytes("request"))), null)
          assertEquals(run(r, backend.ops.shutdownOutput(client)), null)
          val buf = allocate(32)
          assertEquals(readToEnd(r, backend.ops.read(server, buf), buf), "request")
          // The reply goes back on the half-closed connection, which the client still reads, to the server's end.
          assertEquals(run(r, backend.ops.write(server, bytes("reply"))), null)
          assertEquals(run(r, backend.ops.shutdownOutput(server)), null)
          val reply = allocate(32)
          assertEquals(readToEnd(r, backend.ops.read(client, reply), reply), "reply")
          val late = run(r, backend.ops.write(client, bytes("late")))
          assert(late.isInstanceOf[IOException], s"a write after shutting down: $late")
        } finally {
          close(r, server)
          close(r, client)
        }
        val notConnected = run(r, backend.ops.shutdownOutput(listener))
        assert(notConnected.isInstanceOf[IOException], s"shutting down a listener: $notConnected")
      } finally close(r, listener)
    }
  }

  test("an IPv4 datagram goes to a bound socket, which learns its sender") {
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val client = datagram(r, null, IpAddress.IPv4("127.0.0.1", port))
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
          from.clear() match {
            case IpAddress.IPv4(host, senderPort) =>
              assertEquals(host, "127.0.0.1")
              assert(senderPort > 0)
            case other => fail(s"unexpected sender $other")
          }
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("datagram sends: to the peer alone and never empty when connected, empty ones when not, and no lost packets") {
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      val serverAddress = IpAddress.IPv4("127.0.0.1", port)
      try {
        val connected = datagram(r, null, serverAddress)
        val unconnected = datagram(r, IpAddress.IPv4("127.0.0.1", 0), null)
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
          refused(run(r, backend.ops.send(connected, empty, serverAddress)))
          refused(run(r, backend.ops.send(connected, bytes("elsewhere"), IpAddress.IPv4("127.0.0.1", port + 1))))
          refused(run(r, backend.ops.send(unconnected, bytes("nowhere"))))
          assertEquals(run(r, backend.ops.send(connected, bytes("to-peer"), serverAddress)), null)
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

  test("a datagram socket's op completes once it is bound, and fails, leaving its slot empty, on an address in use") {
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val into = Slot[Handle]()
        val failure = run(r, backend.ops.datagram(IpAddress.IPv4("127.0.0.1", port), null, into))
        assert(failure.isInstanceOf[IOException], String.valueOf(failure))
        intercept[IllegalStateException](into.clear())
        val client = datagram(r, null, IpAddress.IPv4("127.0.0.1", port))
        close(r, client)
      } finally close(r, server)
    }
  }

  /** The bytes a pooled read received, as text, consuming them. */
  private def textOf(received: Received): String = {
    val buf = received.buffer
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    new String(out, StandardCharsets.UTF_8)
  }

  test("a pooled read and a read into the caller's buffer take turns on one socket") {
    val pool = backend.pool(bufferSize = 64, buffers = 2)
    try
      withReactor { r =>
        val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
        try {
          val accepted = Slot[Handle]()
          r.submit(backend.ops.accept(listener, accepted), Outcome())
          val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
          turnUntil(r)(connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
          val client = into.clear()
          try {
            val server = {
              var h: Handle | Null = null
              turnUntil(r) {
                try { h = accepted.clear(); true }
                catch case _: IllegalStateException => false
              }
              h.nn
            }
            try {
              r.submit(backend.ops.write(client, bytes("first")), Outcome())
              val into = Slot[Received]()
              val pooled = Outcome()
              r.submit(backend.ops.read(server, pool, into), pooled)
              turnUntil(r)(pooled.finished)
              assertEquals(pooled.completed, 1, String.valueOf(pooled.failure))
              val received = into.clear()
              assertEquals(textOf(received), "first")
              received.release()
              r.submit(backend.ops.write(client, bytes("second")), Outcome())
              val buf = allocate(64)
              val plain = Outcome()
              r.submit(backend.ops.read(server, buf), plain)
              turnUntil(r)(plain.finished)
              assertEquals(plain.completed, 1, String.valueOf(plain.failure))
              assertEquals(text(buf), "second")
            } finally close(r, server)
          } finally close(r, client)
        } finally close(r, listener)
      }
    finally pool.close()
  }

  // ---- Opening and closing as ops

  test("a listener listens once its listen op completes, and accepts") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Handle]()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        turnUntil(r)(acceptDone.finished && connected.finished)
        assertEquals(acceptDone.completed, 1, String.valueOf(acceptDone.failure))
        assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        close(r, accepted.clear())
        close(r, into.clear())
      } finally close(r, listener)
    }
  }

  test("listening on an address in use fails the listen op, leaving its slot empty") {
    withReactor { r =>
      val (taken, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val into = Slot[Handle]()
        val failure = run(r, backend.ops.listen(IpAddress.IPv4("127.0.0.1", port), into, reuseAddr = false))
        assert(failure.isInstanceOf[IOException], String.valueOf(failure))
        intercept[IllegalStateException](into.clear())
      } finally close(r, taken)
    }
  }

  test("acceptAll takes the pending connections, at most max at a time, until each is accepted") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val connecting = (1 to 3).map(_ => startConnect(r, IpAddress.IPv4("127.0.0.1", port)))
        turnUntil(r)(connecting.forall(_._1.finished))
        val clients = connecting.map { (connected, into) =>
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
          into.clear()
        }
        val accepted = Slot[Array[Handle]]()
        val accept = backend.ops.acceptAll(listener, accepted, max = 2)
        val servers = scala.collection.mutable.ArrayBuffer.empty[Handle]
        while servers.length < 3 do {
          val failure = run(r, accept)
          assert(failure == null, String.valueOf(failure))
          val batch = accepted.clear()
          assert(batch.length >= 1 && batch.length <= 2, s"accepted ${batch.length} at once with max 2")
          servers ++= batch
        }
        assertEquals(servers.distinct.length, 3)
        (servers ++ clients).foreach(close(r, _))
      } finally close(r, listener)
    }
  }

  test("a connect to a port nothing listens on fails, leaving its slot empty") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      close(r, listener)
      val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
      turnUntil(r)(connected.finished)
      assert(connected.failure.isInstanceOf[IOException], String.valueOf(connected.failure))
      intercept[IllegalStateException](into.clear())
    }
  }

  test("a closed listener's port can be listened on again once its close op completes") {
    withReactor { r =>
      val (first, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      val closed = Outcome()
      r.submit(backend.ops.close(first), closed)
      turnUntil(r)(closed.finished)
      assertEquals(closed.completed, 1, String.valueOf(closed.failure))
      close(r, listen(r, IpAddress.IPv4("127.0.0.1", port), reuseAddr = false))
    }
  }

  test("a close op on a connection ends its peer's stream") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Handle]()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        turnUntil(r)(acceptDone.finished && connected.finished)
        val client = into.clear()
        val server = accepted.clear()
        try {
          val closed = Outcome()
          r.submit(backend.ops.close(client), closed)
          turnUntil(r)(closed.finished)
          assertEquals(closed.completed, 1, String.valueOf(closed.failure))
          val buf = allocate(16)
          assertEquals(readToEnd(r, backend.ops.read(server, buf), buf), "")
        } finally close(r, server)
      } finally close(r, listener)
    }
  }
}
