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

import asyncio.Handle
import asyncio.Interest
import asyncio.IpAddress
import asyncio.Backend
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.UnixAddress
import asyncio.UnixStreamBackend
import asyncio.testkit.BufferArena
import ReactorAsync.*

/** What gears programs over any backend must do, checked through `perform` alone, so that the same program runs the
  * same on every backend and every way of carrying its futures: a backend's suite extends this with the backend and
  * `running`, which starts a program the platform's way (`ReactorFutures.run`); NIO's does so in each carrier mode and
  * on one and several reactors, kqueue's on a pool of one and of several workers, Darwin's likewise. The checks with
  * stream sockets use Unix ones on a backend that has them, and TCP on the loopback otherwise.
  */
trait FuturesContract extends FunSuite {

  /** The backend under test. */
  val backend: Backend

  /** A listener for a test's server, and a way to connect to it: on a Unix socket in `dir` where the backend has
    * them, and on TCP at a free port of the loopback otherwise.
    */
  protected def listenAndConnect(dir: java.nio.file.Path)(using Async): (Handle, () => Handle) = backend match {
    case unix: UnixStreamBackend =>
      val address = UnixAddress(dir.resolve("echo.sock").toString)
      val listener = unix.handles.listener(address)
      perform(unix, unix.ops.listening(listener))
      (listener, () => unix.handles.connect(address))
    case _ =>
      // A port nothing listens on: the first the listening op does not fail on.
      val random = new java.util.Random()
      var attempts = 0
      var found: (Handle, () => Handle) | Null = null
      while found == null do {
        val address = IpAddress.IPv4("127.0.0.1", 20000 + random.nextInt(30000))
        val listener = backend.handles.listener(address)
        try {
          perform(backend, backend.ops.listening(listener))
          found = (listener, () => backend.handles.connect(address))
        } catch {
          case e: IOException =>
            perform(backend, backend.ops.close(listener))
            attempts += 1
            if attempts > 10 then throw e
        }
      }
      found.nn
  }

  /** Runs `body` as a program over `backend`, the platform's way. */
  protected def running(body: backend.type => Async.Spawn ?=> Unit): Unit

  /** A port nothing listens on, found by listening on it with the backend and letting it go. */
  protected def freePort(): Int = {
    var port = -1
    running { r =>
      val random = new java.util.Random()
      var attempts = 0
      while port < 0 do {
        val candidate = 20000 + random.nextInt(30000)
        val listener = r.handles.listener(IpAddress.IPv4("127.0.0.1", candidate))
        try {
          perform(r, r.ops.listening(listener))
          port = candidate
        } catch {
          case e: IOException =>
            attempts += 1
            if attempts > 10 then throw e
        } finally perform(r, r.ops.close(listener))
      }
    }
    port
  }

  // The buffers of every test, which their futures share across threads.
  protected lazy val arena: BufferArena = BufferArena.open()

  override def afterAll(): Unit = {
    arena.close()
    super.afterAll()
  }

  private def withTempDir(body: java.nio.file.Path => Unit): Unit = {
    val dir = Files.createTempDirectory("gears")
    try body(dir)
    finally if Files.exists(dir) then {
      Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
      Files.deleteIfExists(dir): Unit
    }
  }

  protected def text(buf: ByteBuffer): String = {
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
          perform(r, r.ops.close(out))
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
      } finally perform(r, r.ops.close(in))
    }
    assertEquals(received, total)
  }

  test("clients and a server echo over stream sockets") {
    val dir = Files.createTempDirectory("gears")
    val clients = 16
    val trips = 200
    var echoed = 0
    try running { r =>
      val (listener, connect) = listenAndConnect(dir)
      try {
        val server = Future {
          val accepted = Handle.Slot()
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
              finally perform(r, r.ops.close(socket))
            }
          }
          connections.awaitAll
        }
        val done = (1 to clients).map { _ =>
          Future {
            val socket = connect()
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
            } finally perform(r, r.ops.close(socket))
          }
        }
        echoed = done.awaitAll.sum
        server.await
      } finally perform(r, r.ops.close(listener))
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
        perform(r, r.ops.write(out, arena.of("again".getBytes(StandardCharsets.UTF_8))))
        perform(r, read)
        outcome = s"$cancelled ${text(buf)}"
      } finally {
        perform(r, r.ops.close(in))
        perform(r, r.ops.close(out))
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
        val (listener, connect) = listenAndConnect(dir)
        val client = connect()
        try {
          perform(r, r.ops.connect(client))
          val accepted = Handle.Slot()
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
          } finally perform(r, r.ops.close(server))
        } finally {
          perform(r, r.ops.close(client))
          perform(r, r.ops.close(listener))
        }
      }
    }
    assertEquals(outcome, "true still")
  }

  test("an op waiting on a handle that is closed fails") {
    var failure: Throwable | Null = null
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val waiting = Future(perform(r, r.ops.read(in, arena.allocate(4))))
        perform(r, r.ops.timer(20))
        perform(r, r.ops.close(in))
        failure = waiting.awaitResult.failed.toOption.orNull
      } finally perform(r, r.ops.close(out))
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("a connection refused after waiting for it fails perform") {
    var failure: Throwable | Null = null
    val port = freePort() // nothing listens on it: one just given up
    running { r =>
      try {
        val socket = r.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try perform(r, r.ops.connect(socket))
        finally perform(r, r.ops.close(socket))
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
      perform(r, r.ops.close(in))
      perform(r, r.ops.close(out))
      try perform(r, r.ops.read(in, arena.allocate(4)))
      catch case t: Throwable => failure = t
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("a failure of the program is rethrown") {
    intercept[IllegalStateException](running(_ => throw new IllegalStateException("boom")))
  }
}
