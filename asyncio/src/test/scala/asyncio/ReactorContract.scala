package asyncio

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import munit.FunSuite

/** What every backend's reactors must do, as `Reactor`, `Ops` and `Handles` describe it, checked through the interface
  * alone, so that code written against it runs the same on any backend. A backend's suite extends this, and the
  * contracts of the capabilities its backend has (`UnixStreamContract`, `UnixDatagramContract`, `FifoContract`), with
  * the backend and how to allocate buffers its ops can use. Each check drives one reactor, or two, from the test's
  * thread.
  */
trait ReactorContract extends FunSuite {

  /** The backend under test. */
  val backend: Backend

  /** A buffer of `capacity` bytes that this backend's ops can fill and drain, live until the suite ends. */
  def allocate(capacity: Int): ByteBuffer

  /** Checks this backend is known to fail, by name: they are expected to fail, so one that starts passing is reported,
    * to be taken off the list.
    */
  def knownFailures: Set[String] = Set.empty

  override def test(options: munit.TestOptions)(body: => Any)(using loc: munit.Location): Unit =
    super.test(if knownFailures(options.name) then options.fail else options)(body)

  protected type R = Reactor.Of[backend.Op]

  protected def withReactor(body: R => Unit): Unit = {
    val reactor = backend.open()
    try body(reactor)
    finally reactor.close()
  }

  /** Turns `reactors` without blocking until `cond` holds, failing after a few seconds. */
  protected def turnUntil(reactors: Reactor*)(cond: => Boolean)(using loc: munit.Location): Unit = {
    val deadline = System.nanoTime() + 5_000_000_000L
    while !cond do {
      if System.nanoTime() > deadline then fail(s"timed out waiting for the reactor at line ${loc.line}")
      reactors.foreach(_.runOnce(block = false))
    }
  }

  protected def bytes(text: String): ByteBuffer = {
    val encoded = text.getBytes(StandardCharsets.UTF_8)
    val buf = allocate(math.max(encoded.length, 1))
    buf.put(encoded).flip()
    buf
  }

  protected def text(buf: ByteBuffer): String = {
    val copy = buf.duplicate()
    copy.flip()
    val out = new Array[Byte](copy.remaining())
    copy.get(out)
    new String(out, StandardCharsets.UTF_8)
  }

  protected def tempDir(): Path = Files.createTempDirectory("asyncio")

  protected def deleteAll(dir: Path): Unit = {
    val entries = Files.list(dir)
    try entries.forEach(Files.deleteIfExists(_): Unit)
    finally entries.close()
    Files.deleteIfExists(dir): Unit
  }

  /** Completions that record what happened to an op. */
  protected final class Outcome extends Completion[backend.Op] {
    @volatile var completed = 0
    @volatile var failure: Throwable | Null = null
    @volatile var cancelled = 0
    def onComplete(op: backend.Op): Unit = completed += 1
    override def onFailure(op: backend.Op, t: Throwable): Unit = failure = t
    override def onCancel(op: backend.Op): Unit = cancelled += 1
    def finished: Boolean = completed > 0 || failure != null || cancelled > 0
  }

  test("a timer completes after its delay, and never inside submit") {
    withReactor { r =>
      val outcome = Outcome()
      val started = System.nanoTime()
      r.submit(backend.ops.timer(20), outcome)
      assertEquals(outcome.completed, 0)
      turnUntil(r)(outcome.completed > 0)
      assert(System.nanoTime() - started >= 20_000_000L, "the timer fired early")
    }
  }

  test("timers fire in deadline order, and a blocking turn wakes for the earliest") {
    withReactor { r =>
      val fired = scala.collection.mutable.ArrayBuffer.empty[Int]
      for delay <- List(30, 10, 20) do r.submit(backend.ops.timer(delay), _ => fired += delay)
      while fired.size < 3 do r.runOnce(block = true)
      assertEquals(fired.toList, List(10, 20, 30))
    }
  }

  test("a read waits for data written later, then completes with it") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = allocate(64)
        val outcome = Outcome()
        r.submit(backend.ops.read(in, buf), outcome)
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals(outcome.completed, 0)
        assertEquals(r.handles.writeNow(out, bytes("hello")), 5)
        turnUntil(r)(outcome.finished)
        assertEquals(outcome.completed, 1)
        assertEquals(text(buf), "hello")
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
  }

  test("a read at the end of the stream completes without adding bytes") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        r.handles.close(out)
        val buf = allocate(16)
        val outcome = Outcome()
        r.submit(backend.ops.read(in, buf), outcome)
        turnUntil(r)(outcome.finished)
        assertEquals(outcome.completed, 1)
        assertEquals(buf.position(), 0)
        assertEquals(r.handles.readNow(in, buf), -1)
      } finally r.handles.close(in)
    }
  }

  /** Submits `read` until it completes without adding bytes, returning what `buf` then holds. */
  protected def readToEnd(r: R, read: backend.Op, buf: ByteBuffer)(using munit.Location): String = {
    var atEnd = false
    while !atEnd do {
      val before = buf.position()
      val outcome = Outcome()
      r.submit(read, outcome)
      turnUntil(r)(outcome.finished)
      assertEquals(outcome.completed, 1, String.valueOf(outcome.failure))
      atEnd = buf.position() == before
    }
    text(buf)
  }

  test("a pipe's reads end after the bytes written before its writer closed") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        r.handles.writeNow(out, bytes("then the end"))
        r.handles.close(out)
        val buf = allocate(64)
        assertEquals(readToEnd(r, backend.ops.read(in, buf), buf), "then the end")
      } finally r.handles.close(in)
    }
  }

  test("trySubmit finishes a read of data already waiting without calling its completion") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        r.handles.writeNow(out, bytes("ready"))
        val buf = allocate(16)
        val outcome = Outcome()
        assert(r.trySubmit(backend.ops.read(in, buf), outcome))
        assertEquals(text(buf), "ready")
        for _ <- 1 to 3 do r.runOnce(block = false)
        assertEquals(outcome.completed, 0)
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
  }

  test("writes fill a pipe, wait while it is full, and go on as it drains") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        val size = 1 << 20
        val source = allocate(size)
        while source.hasRemaining do source.put((source.position() % 251).toByte)
        source.flip()
        val sink = allocate(size)
        val write = backend.ops.write(out, source)
        val read = backend.ops.read(in, sink)
        lazy val onWrite: Completion[backend.Op] = _ => if source.hasRemaining then r.submit(write, onWrite)
        lazy val onRead: Completion[backend.Op] = _ => if sink.hasRemaining then r.submit(read, onRead)
        r.submit(write, onWrite)
        r.submit(read, onRead)
        turnUntil(r)(!sink.hasRemaining)
        sink.flip()
        var i = 0
        while i < size do {
          if sink.get(i) != (i % 251).toByte then fail(s"byte $i is wrong")
          i += 1
        }
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
  }

  test("cancelling a pending read runs onCancel, and the op may then be submitted again") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = allocate(16)
        val read = backend.ops.read(in, buf)
        val first = Outcome()
        r.submit(read, first)
        r.runOnce(block = false)
        assert(r.cancel(read))
        assertEquals(first.cancelled, 1)
        assertEquals(first.completed, 0)
        assert(!r.cancel(read), "a cancelled op is no longer pending")
        val second = Outcome()
        r.submit(read, second)
        r.handles.writeNow(out, bytes("again"))
        turnUntil(r)(second.finished)
        assertEquals(second.completed, 1)
        assertEquals(first.completed, 0)
        assertEquals(text(buf), "again")
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
  }

  test("an op that is not Repeatable cannot be submitted twice") {
    withReactor { r =>
      val op = backend.ops.blocking(() => ())
      val outcome = Outcome()
      r.submit(op, outcome)
      turnUntil(r)(outcome.finished)
      intercept[IllegalStateException](r.submit(op, Outcome()))
    }
  }

  test("a blocking op runs off the reactor thread and writes its value into its slot") {
    withReactor { r =>
      val into = Slot[String]()
      val reactorThread = Thread.currentThread()
      val outcome = Outcome()
      r.submit(backend.ops.blocking(() => if Thread.currentThread() eq reactorThread then "same" else "other", into), outcome)
      turnUntil(r)(outcome.finished)
      assertEquals(outcome.completed, 1)
      assertEquals(into.clear(), "other")
    }
  }

  test("a blocking op's failure fails the op") {
    withReactor { r =>
      val outcome = Outcome()
      r.submit(backend.ops.blocking(() => throw new IllegalArgumentException("boom")), outcome)
      turnUntil(r)(outcome.finished)
      assert(outcome.failure.isInstanceOf[IllegalArgumentException], String.valueOf(outcome.failure))
    }
  }

  test("cancelling a running blocking op interrupts it, and onCancel waits for it to return") {
    withReactor { r =>
      val started = new CountDownLatch(1)
      val returned = new AtomicBoolean
      val op = backend.ops.blocking { () =>
        started.countDown()
        try Thread.sleep(10_000)
        catch case _: InterruptedException => ()
        returned.set(true)
      }
      val outcome = Outcome()
      r.submit(op, outcome)
      started.await()
      assert(r.cancel(op))
      turnUntil(r)(outcome.cancelled > 0)
      assert(returned.get(), "onCancel ran before the body returned")
      assertEquals(outcome.completed, 0)
    }
  }

  test("resolve finds localhost") {
    withReactor { r =>
      val into = Slot[List[ResolvedAddress]]()
      val outcome = Outcome()
      r.submit(backend.ops.resolve("localhost", into), outcome)
      turnUntil(r)(outcome.finished)
      assertEquals(outcome.completed, 1, String.valueOf(outcome.failure))
      val found = into.clear()
      assert(found.exists(a => a.host == "127.0.0.1" || a.family == AddressFamily.IPv6), found.toString)
    }
  }

  /** Opens something on a free port: no interface tells which port was bound, so it tries a few. */
  protected def onFreePort(open: Int => Int): (Int, Int) = {
    val random = new java.util.Random()
    var attempts = 0
    var result: (Int, Int) | Null = null
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
      val (listener, port) = onFreePort(port => r.handles.listen(IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = HandleSlot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = r.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try {
          val connected = Outcome()
          r.submit(backend.ops.connect(client), connected)
          turnUntil(r)(acceptDone.finished && connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
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
          r.handles.close(server)
          assertEquals(readToEnd(r, backend.ops.read(client, reply), reply), "tcp ping")
        } finally r.handles.close(client)
      } finally r.handles.close(listener)
    }
  }

  test("openFile refuses what is not a regular file") {
    val dir = tempDir()
    try intercept[IOException](backend.handles.openFile(dir.toString, write = false))
    finally deleteAll(dir)
  }

  test("an IPv4 datagram goes to a bound socket, which learns its sender") {
    withReactor { r =>
      val (server, port) = onFreePort(port => r.handles.datagram(IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val client = r.handles.datagram(null, IpAddress.IPv4("127.0.0.1", port))
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
        } finally r.handles.close(client)
      } finally r.handles.close(server)
    }
  }

  test("whenReady runs its body only once the handle is ready, and waits again while it returns false") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = allocate(64)
        var runs = 0
        val ready = backend.ops.whenReady(in, Interest.Read) { () =>
          runs += 1
          r.handles.readNow(in, buf)
          text(buf).endsWith("!")
        }
        val outcome = Outcome()
        r.submit(ready, outcome)
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals(runs, 0)
        r.handles.writeNow(out, bytes("part"))
        turnUntil(r)(runs > 0)
        assertEquals(outcome.completed, 0)
        r.handles.writeNow(out, bytes("!"))
        turnUntil(r)(outcome.finished)
        assertEquals(outcome.completed, 1)
        assertEquals(text(buf), "part!")
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
  }

  test("a repeatable op moves between two reactors of the backend") {
    withReactor { a =>
      withReactor { b =>
        val (in, out) = a.handles.pipe()
        try {
          val buf = allocate(16)
          val read = backend.ops.read(in, buf)
          val first = Outcome()
          a.submit(read, first)
          a.handles.writeNow(out, bytes("a"))
          turnUntil(a)(first.finished)
          val second = Outcome()
          b.submit(read, second)
          for _ <- 1 to 3 do b.runOnce(block = false)
          b.handles.writeNow(out, bytes("b"))
          turnUntil(a, b)(second.finished)
          assertEquals((first.completed, second.completed), (1, 1))
          assertEquals(text(buf), "ab")
        } finally {
          a.handles.close(in)
          a.handles.close(out)
        }
      }
    }
  }

  test("closing a handle that a reactor blocked on another thread has used ends its peer's stream") {
    val (in, out) = backend.handles.pipe()
    val registered = new CountDownLatch(1)
    @volatile var other: Reactor | Null = null
    val blocked = new Thread(() => {
      val b = backend.open()
      other = b
      b.submit(backend.ops.whenReady(out, Interest.Write)(() => true), _ => registered.countDown())
      b.run() // blocks between turns until stopped
      b.close()
    })
    blocked.start()
    try {
      registered.await()
      Thread.sleep(20) // so that the other reactor is blocked again
      backend.handles.close(out)
      withReactor { r =>
        val buf = allocate(16)
        val outcome = Outcome()
        r.submit(backend.ops.read(in, buf), outcome)
        turnUntil(r)(outcome.finished)
        assertEquals((outcome.completed, buf.position()), (1, 0))
      }
    } finally {
      val b = other
      if b != null then {
        b.stop()
        b.wakeup()
      }
      blocked.join()
      backend.handles.close(in)
    }
  }

  test("closing a reactor cancels its pending ops") {
    val (in, out) = backend.handles.pipe()
    try {
      val reactor = backend.open()
      val read = Outcome()
      val timer = Outcome()
      reactor.submit(backend.ops.read(in, allocate(16)), read)
      reactor.submit(backend.ops.timer(60_000), timer)
      reactor.runOnce(block = false)
      reactor.close()
      assertEquals((read.cancelled, timer.cancelled), (1, 1))
      assertEquals((read.completed, timer.completed), (0, 0))
    } finally {
      backend.handles.close(in)
      backend.handles.close(out)
    }
  }

  test("an op on a closed handle fails") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      r.handles.close(out)
      r.handles.close(in)
      val outcome = Outcome()
      r.submit(backend.ops.read(in, allocate(16)), outcome)
      turnUntil(r)(outcome.finished)
      assert(outcome.failure.isInstanceOf[IOException], String.valueOf(outcome.failure))
    }
  }

  test("a regular file reads to its end") {
    val dir = tempDir()
    try withReactor { r =>
      val path = dir.resolve("contents.txt")
      Files.write(path, "file contents".getBytes(StandardCharsets.UTF_8))
      val file = r.handles.openFile(path.toString, write = false)
      try {
        val buf = allocate(64)
        assertEquals(readToEnd(r, backend.ops.read(file, buf), buf), "file contents")
      } finally r.handles.close(file)
    } finally deleteAll(dir)
  }

}
