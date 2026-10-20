package asyncio.gears

import gears.async.Async
import gears.async.Future
import gears.async.Future.awaitAll
import gears.async.default.given

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CancellationException
import munit.FunSuite

import asyncio.HandleSlot
import asyncio.Interest
import asyncio.IpAddress
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.UnixAddress
import asyncio.nio.VThreadBackend
import asyncio.testkit.BufferArena
import ReactorFutures.*

/** Gears programs over `VThreadBackend`: ops performed on the futures' own virtual threads, each waiting on its
  * handle's own selector, with no reactor.
  */
class VThreadBackendSuite extends FunSuite {
  given VThreadBackend.type = VThreadBackend

  // The buffers of every test, which their futures share across threads.
  private lazy val arena = BufferArena.open()

  override def afterAll(): Unit = {
    arena.close()
    super.afterAll()
  }

  private def running(body: VThreadBackend.type => Async.Spawn ?=> Unit): Unit = ReactorFutures.run(body)

  private def text(buf: ByteBuffer): String = {
    val copy = buf.duplicate().flip()
    val out = new Array[Byte](copy.remaining())
    copy.get(out)
    new String(out, StandardCharsets.UTF_8)
  }

  private def withTempDir(body: java.nio.file.Path => Unit): Unit = {
    val dir = Files.createTempDirectory("gears")
    try body(dir)
    finally {
      Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
      Files.deleteIfExists(dir): Unit
    }
  }

  test("a program waits on a timer") {
    var waited = 0L
    running { r =>
      val started = System.nanoTime()
      perform(r, r.ops.timer(20))
      waited = System.nanoTime() - started
    }
    assert(waited >= 20_000_000L, s"waited only ${waited / 1000} µs")
  }

  test("two futures stream bytes through a pipe") {
    val total = 1 << 20
    var received = 0
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val writer = Future {
          val buf = arena.allocate(8192)
          val write = r.ops.write(out, buf)
          var sent = 0
          while sent < total do {
            buf.clear().limit(math.min(8192, total - sent))
            while buf.hasRemaining do perform(r, write)
            sent += buf.limit()
          }
          r.handles.close(out)
        }
        val reader = Future {
          val buf = arena.allocate(8192)
          val read = r.ops.read(in, buf)
          var atEnd = false
          while !atEnd do {
            buf.clear()
            perform(r, read)
            if buf.position() == 0 then atEnd = true else received += buf.position()
          }
        }
        Seq(writer, reader).awaitAll
      } finally r.handles.close(in)
    }
    assertEquals(received, total)
  }

  test("clients and a server echo over Unix stream sockets") {
    val clients = 16
    val trips = 200
    var echoed = 0
    withTempDir { dir =>
      running { r =>
        val address = UnixAddress(dir.resolve("echo.sock").toString)
        val listener = r.handles.listen(address)
        try {
          val server = Future {
            val accepted = HandleSlot()
            val accept = r.ops.accept(listener, accepted)
            val connections = (1 to clients).map { _ =>
              perform(r, accept)
              val socket = accepted.clear()
              Future {
                val buf = arena.allocate(64)
                val read = r.ops.read(socket, buf)
                val write = r.ops.write(socket, buf)
                try
                  for _ <- 1 to trips do {
                    buf.clear().limit(5)
                    while buf.hasRemaining do perform(r, read)
                    buf.flip()
                    while buf.hasRemaining do perform(r, write)
                  }
                finally r.handles.close(socket)
              }
            }
            connections.awaitAll
          }
          val done = (1 to clients).map { _ =>
            Future {
              val socket = r.handles.connect(address)
              try {
                perform(r, r.ops.connect(socket))
                val buf = arena.allocate(64)
                val read = r.ops.read(socket, buf)
                val write = r.ops.write(socket, buf)
                for _ <- 1 to trips do {
                  buf.clear().put("hello".getBytes(StandardCharsets.UTF_8)).flip()
                  while buf.hasRemaining do perform(r, write)
                  buf.clear().limit(5)
                  while buf.hasRemaining do perform(r, read)
                  assertEquals(text(buf), "hello")
                }
                1
              } finally r.handles.close(socket)
            }
          }
          echoed = done.awaitAll.sum
          server.await
        } finally r.handles.close(listener)
      }
    }
    assertEquals(echoed, clients)
  }

  test("a datagram goes to a socket's address and the reply to its sender") {
    var outcome = ""
    running { r =>
      // A free port of the test's choosing, since handles do not report the one a socket was given.
      val port = {
        val socket = new java.net.DatagramSocket(0, java.net.InetAddress.getByName("127.0.0.1"))
        try socket.getLocalPort
        finally socket.close()
      }
      val bound = r.handles.datagram(IpAddress.IPv4("127.0.0.1", port), null)
      val client = r.handles.datagram(null, IpAddress.IPv4("127.0.0.1", port))
      try {
        val echo = Future {
          val buf = arena.allocate(64)
          val from = Slot[IpAddress]()
          perform(r, r.ops.receive(bound, buf, from))
          perform(r, r.ops.send(bound, buf, from.clear()))
        }
        perform(r, r.ops.send(client, arena.of("ping".getBytes(StandardCharsets.UTF_8))))
        val reply = arena.allocate(64)
        val from = Slot[IpAddress]()
        perform(r, r.ops.receive(client, reply, from))
        echo.await
        val copy = new Array[Byte](reply.remaining())
        reply.get(copy)
        outcome = s"${new String(copy, StandardCharsets.UTF_8)} from ${from.clear()}"
      } finally {
        r.handles.close(bound)
        r.handles.close(client)
      }
    }
    assert(outcome.startsWith("ping from IPv4(127.0.0.1,"), outcome)
  }

  test("a host name resolves") {
    var addresses: List[ResolvedAddress] = Nil
    running { r =>
      val into = Slot[List[ResolvedAddress]]()
      perform(r, r.ops.resolve("localhost", into))
      addresses = into.clear()
    }
    assert(addresses.nonEmpty)
  }

  test("cancelling a future cancels the op it waits on, which leaves the handle open and may be performed again") {
    var outcome = ""
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        val read = r.ops.read(in, buf)
        val waiting = Future(perform(r, read))
        perform(r, r.ops.timer(20)) // so that the read is waiting
        waiting.cancel()
        val cancelled = waiting.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
        r.handles.writeNow(out, arena.of("again".getBytes(StandardCharsets.UTF_8)))
        perform(r, read)
        outcome = s"$cancelled ${text(buf)}"
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
    assertEquals(outcome, "true again")
  }

  test("a read waiting on a socket is cancelled with the connection left open") {
    var outcome = ""
    withTempDir { dir =>
      running { r =>
        val address = UnixAddress(dir.resolve("open.sock").toString)
        val listener = r.handles.listen(address)
        val client = r.handles.connect(address)
        try {
          perform(r, r.ops.connect(client))
          val accepted = HandleSlot()
          perform(r, r.ops.accept(listener, accepted))
          val server = accepted.clear()
          try {
            val buf = arena.allocate(16)
            val read = r.ops.read(client, buf)
            val waiting = Future(perform(r, read))
            perform(r, r.ops.timer(20))
            waiting.cancel()
            val cancelled = waiting.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
            perform(r, r.ops.write(server, arena.of("still".getBytes(StandardCharsets.UTF_8))))
            perform(r, read)
            outcome = s"$cancelled ${text(buf)}"
          } finally r.handles.close(server)
        } finally {
          r.handles.close(client)
          r.handles.close(listener)
        }
      }
    }
    assertEquals(outcome, "true still")
  }

  test("an op waiting on a handle that is closed fails, and closing releases the handle's selectors") {
    var failure: Throwable | Null = null
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val waiting = Future(perform(r, r.ops.read(in, arena.allocate(4))))
        perform(r, r.ops.timer(20))
        r.handles.close(in)
        failure = waiting.awaitResult.failed.toOption.orNull
      } finally r.handles.close(out)
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("whenReady runs its body once the handle is ready, on the performing thread") {
    var outcome = ""
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        var runs = 0
        val ready = r.ops.whenReady(in, Interest.Read) { () =>
          runs += 1
          r.handles.readNow(in, buf) != 0
        }
        val waiting = Future(perform(r, ready))
        perform(r, r.ops.timer(20))
        r.handles.writeNow(out, arena.of("ready".getBytes(StandardCharsets.UTF_8)))
        waiting.await
        outcome = s"${text(buf)} after $runs"
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
    assertEquals(outcome, "ready after 1")
  }

  test("a cancelled timer stops sleeping, and the timer may be performed again") {
    var waited = 0L
    running { r =>
      val timer = r.ops.timer(60_000)
      val sleeping = Future(perform(r, timer))
      perform(r, r.ops.timer(20))
      val started = System.nanoTime()
      sleeping.cancel()
      sleeping.awaitResult
      waited = System.nanoTime() - started
      Future(perform(r, r.ops.timer(1))).await
    }
    assert(waited < 1_000_000_000L, s"the cancelled timer slept on for ${waited / 1_000_000} ms")
  }

  test("an op that is not Repeatable is performed only once") {
    var failure: Throwable | Null = null
    running { r =>
      val into = Slot[List[ResolvedAddress]]()
      val resolve = r.ops.resolve("localhost", into)
      perform(r, resolve)
      into.clear()
      try perform(r, resolve)
      catch case t: Throwable => failure = t
    }
    assert(failure.isInstanceOf[IllegalStateException], String.valueOf(failure))
  }

  test("an op that fails fails perform") {
    var failure: Throwable | Null = null
    running { r =>
      val (in, out) = r.handles.pipe()
      r.handles.close(in)
      r.handles.close(out)
      try perform(r, r.ops.read(in, arena.allocate(4)))
      catch case t: Throwable => failure = t
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("a connection refused after waiting for it fails perform") {
    var failure: Throwable | Null = null
    // A port nothing listens on: one just given up.
    val port = {
      val socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))
      try socket.getLocalPort
      finally socket.close()
    }
    running { r =>
      try {
        val socket = r.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try perform(r, r.ops.connect(socket))
        finally r.handles.close(socket)
      } catch case t: Throwable => failure = t
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("perform refuses a platform thread, which it would block outright") {
    val backend = VThreadBackend
    intercept[IllegalStateException](Async.blocking(backend.perform(backend.ops.timer(1))))
  }

  test("there are no reactors to carry its futures") {
    intercept[IllegalArgumentException](ReactorFutures.run(1, carriers = Carriers.Reactors)(_ => ()))
  }

  test("a failure of the program is rethrown") {
    intercept[IllegalStateException](running(_ => throw new IllegalStateException("boom")))
  }
}
