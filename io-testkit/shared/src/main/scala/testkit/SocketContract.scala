package gears.async.io
package testkit

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException

/** What a backend with IP sockets must do (see `ReactorContract`). */
trait SocketContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & SocketBackend

  /** A listener on `address`, listening, from the `listen` op, whose failure is thrown. */
  protected def listen(r: R, address: IpAddress)(using munit.Location): Listener = {
    val into = Slot[Listener]()
    opened(r, into, backend.ops.listen(address, into))
  }

  /** A datagram socket from the `datagram` op, whose failure is thrown. */
  protected def datagram(r: R, local: IpAddress | Null, remote: IpAddress | Null)(using munit.Location): Datagram = {
    val into = Slot[Datagram]()
    opened(r, into, backend.ops.datagram(local, remote, into))
  }

  /** Submits a `connect` op to `address` on `r`: what it reports, and the slot the socket is written into. */
  protected def startConnect(r: R, address: IpAddress): (Outcome, Slot[Connection]) = {
    val into = Slot[Connection]()
    val connected = Outcome()
    r.submit(backend.ops.connect(address, into), connected)
    (connected, into)
  }

  /** Opens something on a free port: no interface tells which port was bound, so it tries a few. */
  protected def onFreePort[K <: Handle](open: Int => K): (K, Int) = {
    val random = new java.util.Random()
    var attempts = 0
    var result: (K, Int) | Null = null
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
        val accepted = Slot[Connection]()
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
        val accepted = Slot[Connection]()
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
        // Shutting down a listener does not compile: `shutdownOutput` takes a `Connection`.
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

  test("a packet lost on a socket that is not connected fails no send, to its peer or another") {
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      try {
        // A port nothing listens on, which loopback answers with port unreachable.
        val (gone, gonePort) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
        close(r, gone)
        val nowhere = IpAddress.IPv4("127.0.0.1", gonePort)
        val unconnected = datagram(r, IpAddress.IPv4("127.0.0.1", 0), null)
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

  test("an address is checked as the op is built: numeric, of its case's family, with no scope, its port in range") {
    val ops = backend.ops
    val bad = List(
      IpAddress.IPv4("1.2.3", 80),
      IpAddress.IPv4("1.2.3.256", 80),
      IpAddress.IPv4("localhost", 80),
      IpAddress.IPv4("::1", 80),
      IpAddress.IPv6("127.0.0.1", 80),
      IpAddress.IPv6("fe80::1%en0", 80),
      IpAddress.IPv6("[::1]", 80),
      IpAddress.IPv4("127.0.0.1", 65536),
      IpAddress.IPv4("127.0.0.1", -1)
    )
    for address <- bad do {
      intercept[IllegalArgumentException](ops.connect(address, Slot[Connection]()))
      intercept[IllegalArgumentException](ops.listen(address, Slot[Listener]()))
      intercept[IllegalArgumentException](ops.datagram(address, null, Slot[Datagram]()))
    }
    intercept[IllegalArgumentException](
      ops.datagram(IpAddress.IPv4("127.0.0.1", 0), IpAddress.IPv6("::1", 9), Slot[Datagram]())
    )
  }

  test(
    "over IPv6 a datagram's sender is written as inet_ntop does, and a connected socket's peer compared by meaning"
  ) {
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv6("::1", port), null))
      try {
        val client = datagram(r, null, IpAddress.IPv6("0:0:0:0:0:0:0:1", port))
        try {
          assertEquals(run(r, backend.ops.send(client, bytes("v6"), IpAddress.IPv6("::0:1", port))), null)
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

  test(
    "an IPv6 socket on :: also takes IPv4: a listener accepts an IPv4 client, and a datagram socket replies to one"
  ) {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv6("::", port)))
      try {
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        val accepted = Slot[Connection]()
        val acceptedOutcome = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptedOutcome)
        turnUntil(r)(connected.finished && acceptedOutcome.finished)
        assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        close(r, into.clear())
        close(r, accepted.clear())
      } finally close(r, listener)

      val (server, serverPort) = onFreePort(port => datagram(r, IpAddress.IPv6("::", port), null))
      try {
        val client = datagram(r, IpAddress.IPv4("127.0.0.1", 0), null)
        try {
          assertEquals(run(r, backend.ops.send(client, bytes("v4"), IpAddress.IPv4("127.0.0.1", serverPort))), null)
          val from = Slot[IpAddress]()
          assertEquals(run(r, backend.ops.receive(server, allocate(16), from)), null)
          val sender = from.clear()
          sender match {
            case IpAddress.IPv4(host, _) => assertEquals(host, "127.0.0.1", "an IPv4 sender, not IPv4-mapped")
            case other                   => fail(s"an IPv4 sender, not IPv4-mapped: $other")
          }
          assertEquals(run(r, backend.ops.send(server, bytes("back"), sender)), null)
          val buf = allocate(16)
          assertEquals(run(r, backend.ops.receive(client, buf, Slot[IpAddress]())), null)
          assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "back")
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("a packet larger than the receive buffer fills it, the rest dropped, and the next packet arrives whole") {
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val client = datagram(r, null, IpAddress.IPv4("127.0.0.1", port))
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
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val client = datagram(r, null, IpAddress.IPv4("127.0.0.1", port))
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
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val into = Slot[Datagram]()
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
          val accepted = Slot[Connection]()
          r.submit(backend.ops.accept(listener, accepted), Outcome())
          val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
          turnUntil(r)(connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
          val client = into.clear()
          try {
            val server = {
              var h: Connection | Null = null
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

  /** Runs `body` with `count` connections over loopback, each as its client and server ends, closed afterwards. */
  private def withConnections(r: R, count: Int)(body: Seq[(Connection, Connection)] => Unit): Unit = {
    val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
    val pairs = scala.collection.mutable.ArrayBuffer.empty[(Connection, Connection)]
    try {
      for _ <- 1 to count do {
        val accepted = Slot[Connection]()
        val acceptedOutcome = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptedOutcome)
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        turnUntil(r)(connected.finished && acceptedOutcome.finished)
        assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        assertEquals(acceptedOutcome.completed, 1, String.valueOf(acceptedOutcome.failure))
        pairs += ((into.clear(), accepted.clear()))
      }
      body(pairs.toSeq)
    } finally {
      for (client, server) <- pairs do {
        close(r, client)
        close(r, server)
      }
      close(r, listener)
    }
  }

  /** A pooled read of `handle` run to its end: what it received, which the caller releases, or its failure. */
  private def readPooled(r: R, handle: Readable, pool: BufferPool)(using munit.Location): Received | Throwable = {
    val into = Slot[Received]()
    val outcome = Outcome()
    r.submit(backend.ops.read(handle, pool, into), outcome)
    turnUntil(r)(outcome.finished)
    if outcome.failure != null then outcome.failure.nn else into.clear()
  }

  test("a pooled read on a socket fails when the pool has no buffer free, and goes on once one is released") {
    val pool = backend.pool(bufferSize = 16, buffers = 1)
    try
      withReactor { r =>
        withConnections(r, 1) { case Seq((client, server)) =>
          r.submit(backend.ops.write(client, bytes("one")), Outcome())
          val held = readPooled(r, server, pool) match {
            case received: Received => received
            case failure: Throwable => fail("the first pooled read failed", failure)
          }
          assertEquals(textOf(held), "one")
          val refused = Slot[Received]()
          val refusedOutcome = Outcome()
          r.submit(backend.ops.read(server, pool, refused), refusedOutcome)
          r.submit(backend.ops.write(client, bytes("two")), Outcome())
          turnUntil(r)(refusedOutcome.finished)
          assert(refusedOutcome.failure.isInstanceOf[IOException], String.valueOf(refusedOutcome.failure))
          held.release()
          readPooled(r, server, pool) match {
            case received: Received =>
              assertEquals(textOf(received), "two", "the refused read left no read pending, and took nothing")
              received.release()
            case failure: Throwable => fail("the pooled read after the release failed", failure)
          }
        }
      }
    finally pool.close()
  }

  test("a pool's buffers go to the reads that take them: with a pool of two, two connections' reads hold both") {
    val pool = backend.pool(bufferSize = 16, buffers = 2)
    try
      withReactor { r =>
        withConnections(r, 2) { case Seq((client1, server1), (client2, server2)) =>
          r.submit(backend.ops.write(client1, bytes("first")), Outcome())
          r.submit(backend.ops.write(client2, bytes("second")), Outcome())
          val held = readPooled(r, server1, pool) match {
            case received: Received => received
            case failure: Throwable => fail("the first connection's pooled read failed", failure)
          }
          readPooled(r, server2, pool) match {
            case received: Received =>
              assertEquals(textOf(received), "second")
              received.release()
            case failure: Throwable => fail("the second connection's read found no buffer free", failure)
          }
          assertEquals(textOf(held), "first")
          held.release()
          assertEquals(pool.available, 2, "no buffer is kept for reads not yet made")
        }
      }
    finally pool.close()
  }

  test("a pooled read that timed out holds no buffer: another connection's read takes the pool's one") {
    val pool = backend.pool(bufferSize = 16, buffers = 1)
    try
      withReactor { r =>
        withConnections(r, 2) { case Seq((client1, server1), (client2, server2)) =>
          val idle = Outcome()
          r.submit(backend.ops.read(server1, pool, Slot[Received]()), idle, timeoutMillis = 20)
          turnUntil(r)(idle.finished)
          assert(idle.failure.isInstanceOf[TimeoutException], String.valueOf(idle.failure))
          assertEquals(pool.available, 1, "the read that timed out holds no buffer")
          r.submit(backend.ops.write(client2, bytes("busy")), Outcome())
          readPooled(r, server2, pool) match {
            case received: Received =>
              assertEquals(textOf(received), "busy")
              received.release()
            case failure: Throwable => fail("the other connection's read found no buffer free", failure)
          }
          r.submit(backend.ops.write(client1, bytes("late")), Outcome())
          readPooled(r, server1, pool) match {
            case received: Received =>
              assertEquals(textOf(received), "late", "what came for the read that timed out is the next read's")
              received.release()
            case failure: Throwable => fail("the read after the timeout failed", failure)
          }
          assertEquals(pool.available, 1)
        }
      }
    finally pool.close()
  }

  test("a write that timed out, its peer not reading, wrote nothing: the buffer is as it was") {
    withReactor { r =>
      withConnections(r, 1) { case Seq((client, _)) =>
        val big = allocate(16 << 20)
        val write = backend.ops.write(client, big)
        var timedOut = false
        while !timedOut && big.hasRemaining() do {
          val before = big.position()
          val outcome = Outcome()
          r.submit(write, outcome, timeoutMillis = 50)
          turnUntil(r)(outcome.finished)
          if outcome.failure != null then {
            assert(outcome.failure.isInstanceOf[TimeoutException], String.valueOf(outcome.failure))
            assertEquals(big.position(), before, "a write that timed out wrote nothing")
            timedOut = true
          }
        }
        assert(timedOut, "the peer's buffers took everything")
      }
    }
  }

  test("closing a connection whose peer stopped reading completes at once, though bytes written have not gone") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Connection]()
        val acceptedOutcome = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptedOutcome)
        val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
        turnUntil(r)(connected.finished && acceptedOutcome.finished)
        val (client, server) = (into.clear(), accepted.clear())
        try {
          // More than the kernel's and the framework's buffers hold, to a peer that never reads.
          val big = allocate(16 << 20)
          big.limit(big.capacity())
          val write = backend.ops.write(client, big)
          var written = Outcome()
          r.submit(write, written)
          val until = System.nanoTime() + 200_000_000L
          while System.nanoTime() < until do {
            r.runOnce(block = false)
            if written.finished && written.failure == null && big.hasRemaining() then {
              written = Outcome()
              r.submit(write, written)
            }
          }
          assert(big.hasRemaining(), "the peer's buffers took everything")
          val closed = Outcome()
          r.submit(backend.ops.close(client), closed)
          turnUntil(r)(closed.finished && written.finished)
          assertEquals(closed.completed, 1, String.valueOf(closed.failure))
        } finally close(r, server)
      } finally close(r, listener)
    }
  }

  // ---- Opening and closing as ops

  test("a listener listens once its listen op completes, and accepts") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Connection]()
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
        val into = Slot[Listener]()
        val failure = run(r, backend.ops.listen(IpAddress.IPv4("127.0.0.1", port), into))
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
        val accepted = Slot[Array[Connection]]()
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

  test("a port whose connection lingers in TIME_WAIT can be listened on again at once") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      val (connected, into) = startConnect(r, IpAddress.IPv4("127.0.0.1", port))
      val accepted = Slot[Connection]()
      val acceptedOutcome = Outcome()
      r.submit(backend.ops.accept(listener, accepted), acceptedOutcome)
      turnUntil(r)(connected.finished && acceptedOutcome.finished)
      val client = into.clear()
      close(r, accepted.clear()) // the server's side closes first, so its end of the connection is left in TIME_WAIT
      val buf = allocate(8)
      assertEquals(run(r, backend.ops.read(client, buf)), null) // the server's end
      close(r, client)
      close(r, listener)
      close(r, listen(r, IpAddress.IPv4("127.0.0.1", port)))
    }
  }

  test("connect, listen, datagram and shutdownOutput are one-shot: submitted again, they throw IllegalStateException") {
    withReactor { r =>
      val into = Slot[Listener]()
      val (probe, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      close(r, probe)
      val listening = backend.ops.listen(IpAddress.IPv4("127.0.0.1", port), into)
      assertEquals(run(r, listening), null)
      val listener = into.clear()
      intercept[IllegalStateException](r.submit(listening, Outcome()))
      val connected = Slot[Connection]()
      val connecting = backend.ops.connect(IpAddress.IPv4("127.0.0.1", port), connected)
      val accepted = Slot[Connection]()
      r.submit(backend.ops.accept(listener, accepted), Outcome())
      assertEquals(run(r, connecting), null)
      intercept[IllegalStateException](r.submit(connecting, Outcome()))
      val client = connected.clear()
      val shutting = backend.ops.shutdownOutput(client)
      assertEquals(run(r, shutting), null)
      intercept[IllegalStateException](r.submit(shutting, Outcome()))
      val bound = Slot[Datagram]()
      val binding = backend.ops.datagram(IpAddress.IPv4("127.0.0.1", 0), null, bound)
      assertEquals(run(r, binding), null)
      intercept[IllegalStateException](r.submit(binding, Outcome()))
      close(r, bound.clear())
      close(r, client)
      turnUntil(r)(try {
        close(r, accepted.clear()); true
      } catch case _: IllegalStateException => false)
      close(r, listener)
    }
  }

  test("an accept on a closed listener fails, as does one waiting when it is closed") {
    withReactor { r =>
      val (listener, _) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      val waiting = Outcome()
      r.submit(backend.ops.accept(listener, Slot[Connection]()), waiting)
      r.runOnce(block = false)
      close(r, listener)
      turnUntil(r)(waiting.finished)
      assert(waiting.failure.isInstanceOf[IOException], String.valueOf(waiting.failure))
      val after = run(r, backend.ops.accept(listener, Slot[Connection]()))
      assert(after.isInstanceOf[IOException], String.valueOf(after))
    }
  }

  test("a closed listener's port can be listened on again once its close op completes") {
    withReactor { r =>
      val (first, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      val closed = Outcome()
      r.submit(backend.ops.close(first), closed)
      turnUntil(r)(closed.finished)
      assertEquals(closed.completed, 1, String.valueOf(closed.failure))
      close(r, listen(r, IpAddress.IPv4("127.0.0.1", port)))
    }
  }

  test("a close op on a connection ends its peer's stream") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Slot[Connection]()
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
