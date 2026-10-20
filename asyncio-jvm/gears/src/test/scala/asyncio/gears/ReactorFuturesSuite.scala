package asyncio.gears

import gears.async.Async
import gears.async.Future
import gears.async.Future.awaitAll

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
import asyncio.nio.NioBackend
import asyncio.testkit.BufferArena
import ReactorFutures.*

/** Gears programs over NIO's reactors, carried by `carriers`, on `workers` reactors: the same calls to `perform` either
  * way.
  */
abstract class ReactorFuturesSuite(carriers: Carriers, workers: Int) extends FunSuite {
  given NioBackend.type = NioBackend

  // The buffers of every test, which their futures share across threads.
  private lazy val arena = BufferArena.open()

  override def afterAll(): Unit = {
    arena.close()
    super.afterAll()
  }

  private def running(body: NioBackend.type => Async.Spawn ?=> Unit): Unit =
    ReactorFutures.run(workers, carriers = carriers)(body)

  private def withTempDir(body: java.nio.file.Path => Unit): Unit = {
    val dir = Files.createTempDirectory("gears")
    try body(dir)
    finally if Files.exists(dir) then {
      Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
      Files.deleteIfExists(dir): Unit
    }
  }

  private def text(buf: ByteBuffer): String = {
    val copy = buf.duplicate().flip()
    val out = new Array[Byte](copy.remaining())
    copy.get(out)
    new String(out, StandardCharsets.UTF_8)
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
    val dir = Files.createTempDirectory("gears")
    val clients = 16
    val trips = 200
    var echoed = 0
    try running { r =>
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
    } finally if Files.exists(dir) then {
      Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
      Files.deleteIfExists(dir): Unit
    }
    assertEquals(echoed, clients)
  }

  test("cancelling a future cancels the op it waits on, which may then be performed again") {
    var outcome = ""
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        val read = r.ops.read(in, buf)
        val waiting = Future(perform(r, read))
        perform(r, r.ops.timer(20)) // so that the read is pending
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

  test("a cancelled timer stops waiting, and may be performed again") {
    var outcome = ""
    running { r =>
      val timer = r.ops.timer(60_000)
      val sleeping = Future(perform(r, timer))
      perform(r, r.ops.timer(20)) // so that the long timer is waiting
      val started = System.nanoTime()
      sleeping.cancel()
      val cancelled = sleeping.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
      val waited = System.nanoTime() - started
      perform(r, r.ops.timer(1)) // the op again, now for a moment
      outcome = s"$cancelled ${waited < 1_000_000_000L}"
    }
    assertEquals(outcome, "true true")
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

  // A caller off the pool waits on the handle's own selector, which closing the handle wakes; an op pending on a
  // reactor stays pending, a handle with ops pending being the caller's to keep open (see `Reactor.close`).
  if carriers == Carriers.Jdk then test("an op a caller off the pool waits for fails once its handle is closed") {
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

  test("whenReady runs its body once the handle is ready") {
    var outcome = ""
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        val runs = new java.util.concurrent.atomic.AtomicInteger
        val ready = r.ops.whenReady(in, Interest.Read) { () =>
          runs.incrementAndGet()
          r.handles.readNow(in, buf) != 0
        }
        val waiting = Future(perform(r, ready))
        perform(r, r.ops.timer(20))
        r.handles.writeNow(out, arena.of("ready".getBytes(StandardCharsets.UTF_8)))
        waiting.await
        outcome = s"${text(buf)} after ${runs.get()}"
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
    assertEquals(outcome, "ready after 1")
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

  test("a failure of the program is rethrown") {
    intercept[IllegalStateException](running(_ => throw new IllegalStateException("boom")))
  }
}

class JdkCarriersSuite extends ReactorFuturesSuite(Carriers.Jdk, workers = 1)
class JdkCarriersOnTwoSuite extends ReactorFuturesSuite(Carriers.Jdk, workers = 2)
class ReactorCarriersSuite extends ReactorFuturesSuite(Carriers.Reactors, workers = 1) {
  test("futures run on the pool's workers, whose reactors their ops go to") {
    var onWorker = false
    ReactorFutures.run(1, carriers = Carriers.Reactors) { r =>
      onWorker = summon[Async].scheduler match {
        case s: ReactorScheduler => s.currentWorker != null
        case _                   => false
      }
    }
    assert(onWorker)
  }
}
class ReactorCarriersOnFourSuite extends ReactorFuturesSuite(Carriers.Reactors, workers = 4)
