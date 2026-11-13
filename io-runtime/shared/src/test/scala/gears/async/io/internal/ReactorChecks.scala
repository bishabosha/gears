package gears.async.io
package internal

import gears.async.io.testkit.BufferArena

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeoutException

import munit.FunSuite

/** What a reactor backend's reactors do that only the `Reactor` API shows, and no program through `perform` can see:
  * when completions run (never inside `submit`), a completion that throws, turns shared fairly between completions, a
  * closed reactor, and a deadline move reaching a reactor the timer has left. Everything a program sees is the
  * testkit's contracts' (`gears.async.io.testkit`); a reactor backend's suite runs those, and extends this with the
  * backend. Each check drives one reactor, or two, from the test's thread, with pipes, the simplest stream a backend
  * has.
  */
trait ReactorChecks extends FunSuite {

  /** The backend under test: a reactor backend with blocking ops and pipes. */
  val backend: ReactorBackend { type Interface <: Blocking & Pipes }

  /** Where the checks' buffers come from, freed when the suite ends. */
  private lazy val arena = BufferArena.open()

  /** A buffer of `capacity` bytes that this backend's ops can fill and drain, live until the suite ends. */
  protected def allocate(capacity: Int): ByteBuffer = arena.allocate(capacity)

  override def afterAll(): Unit = {
    arena.close()
    super.afterAll()
  }

  protected type R = Reactor.Of[backend.ops.Op, backend.ops.Handle]

  /** The backend's handles. */
  protected type Handle = backend.ops.Handle
  protected type Readable = backend.ops.Readable
  protected type Writable = backend.ops.Writable

  protected def withReactor(body: R => Unit): Unit = {
    val reactor = backend.open()
    try body(reactor)
    finally reactor.close()
  }

  /** Runs `op` on `r`, turning until it is done, and returns its failure, if any. */
  protected def run(r: R, op: backend.ops.Op)(using munit.Location): Throwable | Null = {
    val outcome = Outcome()
    r.submit(op, outcome)
    turnUntil(r)(outcome.finished)
    outcome.failure
  }

  /** Closes `handle` through `r`, as the interface has it, and throws the close's failure. */
  protected def close(r: R, handle: Handle)(using munit.Location): Unit = {
    val failure = run(r, backend.ops.close(handle))
    if failure != null then throw failure
  }

  /** Closes `handle` from a thread with no reactor, on a reactor opened for it. */
  protected def closeAlone(handle: Handle)(using munit.Location): Unit = withReactor(r => close(r, handle))

  /** Runs `op`, which opens a handle into `into`, on `r`, and returns the handle; throws the op's failure. */
  protected def opened[K <: Handle](r: R, into: Slot[K], op: backend.ops.Op)(using munit.Location): K = {
    val failure = run(r, op)
    if failure != null then throw failure
    into.clear()
  }

  /** Opens a pipe through `r`, as the interface has it: its read end and its write end. */
  protected def pipe(r: R)(using munit.Location): (Readable, Writable) = {
    val read = Slot[Readable]()
    val write = Slot[Writable]()
    val failure = run(r, backend.ops.pipe(read, write))
    if failure != null then throw failure
    (read.clear(), write.clear())
  }

  /** Opens a pipe from a thread with no reactor, on a reactor opened for it. */
  protected def pipeAlone()(using munit.Location): (Readable, Writable) = {
    var ends: (Readable, Writable) | Null = null
    withReactor(r => ends = pipe(r))
    ends.nn
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

  protected def tempDir(): Path = Files.createTempDirectory("gears-io")

  protected def deleteAll(dir: Path): Unit = {
    val entries = Files.list(dir)
    try entries.forEach(Files.deleteIfExists(_): Unit)
    finally entries.close()
    Files.deleteIfExists(dir): Unit
  }

  /** Completions that record what happened to an op. */
  protected final class Outcome extends Completion[backend.ops.Op] {
    @volatile var completed = 0
    @volatile var failure: Throwable | Null = null
    @volatile var cancelled = 0
    def onComplete(op: backend.ops.Op): Unit = completed += 1
    override def onFailure(op: backend.ops.Op, t: Throwable): Unit = failure = t
    override def onCancel(op: backend.ops.Op): Unit = cancelled += 1
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

  for oldHeapOccupied <- List(false, true) do
    test(s"a queued deadline move cannot touch reuse on another poller (old heap occupied: $oldHeapOccupied)") {
      withReactor { first =>
        withReactor { second =>
          val timer = backend.ops.deadlineTimer(System.nanoTime() + 60_000_000_000L)
          val previous = Outcome()
          first.submit(timer, previous)
          val mover = new Thread(() => timer.moveTo(System.nanoTime() + 30_000_000_000L))
          mover.start()
          mover.join(5000)
          assert(!mover.isAlive)
          assert(first.cancel(timer))
          assertEquals(previous.cancelled, 1)

          val sibling = backend.ops.timer(60000)
          val siblingOutcome = Outcome()
          if oldHeapOccupied then first.submit(sibling, siblingOutcome)
          val current = Outcome()
          second.submit(timer, current)
          first.runOnce(block = false) // Processes the stale move, whose heap index now names the second heap.
          assert(!current.finished)
          assert(!siblingOutcome.finished)
          if oldHeapOccupied then {
            assert(first.cancel(sibling))
            assertEquals(siblingOutcome.cancelled, 1)
          }
          timer.moveTo(System.nanoTime() - 1)
          turnUntil(second)(current.finished)
          assertEquals(current.completed, 1)
          assertEquals(current.cancelled, 0)
          assertEquals(previous.cancelled, 1)
        }
      }
    }

  test("a completion that throws is reported, with its op, and the turn goes on to the other completions") {
    withReactor { r =>
      val reported = scala.collection.mutable.ArrayBuffer.empty[Throwable]
      r.reportFailure = reported += _
      val throwing = backend.ops.timer(0)
      val outcome = Outcome()
      r.submit(throwing, _ => throw new IllegalStateException("a bug in the completion"))
      r.submit(backend.ops.timer(0), outcome) // due in the same turn, after the throwing one
      turnUntil(r)(outcome.completed > 0)
      assertEquals(reported.size, 1, reported.mkString)
      reported.head match {
        case failed: CompletionFailed =>
          assert(failed.op.asInstanceOf[AnyRef] eq throwing.asInstanceOf[AnyRef])
          assert(failed.getCause.isInstanceOf[IllegalStateException], failed.getCause)
        case other => fail(s"reported $other")
      }
      // A completion that handles none of a failure: the default onFailure throws it, and that is reported the same way.
      reported.clear()
      val unhandled = backend.ops.timer(50)
      r.submit(unhandled, _ => (), 0) // its time limit is up before it fires, so it fails with TimeoutException
      turnUntil(r)(reported.nonEmpty)
      reported.head match {
        case failed: CompletionFailed =>
          assert(failed.op.asInstanceOf[AnyRef] eq unhandled.asInstanceOf[AnyRef])
          assert(failed.getCause.isInstanceOf[TimeoutException], failed.getCause)
        case other => fail(s"reported $other")
      }
    }
  }

  test("an immediate completion that resubmits itself leaves the next batch and a sibling timer their turns") {
    withReactor { r =>
      val (in, out) = pipe(r)
      close(r, out)
      try {
        val read = backend.ops.read(in, allocate(1)) // EOF is immediately ready on every submission.
        var completed = 0
        lazy val again: Completion[backend.ops.Op] = _ => {
          completed += 1
          if completed < 16 then r.submit(read, again)
        }
        val sibling = Outcome()
        r.submit(read, again)
        r.submit(backend.ops.timer(0), sibling)
        for turn <- 1 to 16 do {
          r.runOnce(block = false)
          assertEquals(completed, turn)
          assertEquals(sibling.completed, 1)
        }
      } finally close(r, in)
    }
  }

  test("closing a reactor gives an op that already finished its outcome, not a cancel") {
    val (in, out) = pipeAlone()
    try {
      withReactor(r => assertEquals(run(r, backend.ops.write(out, bytes("kept"))), null))
      val reactor = backend.open()
      val buf = allocate(16)
      val read = Outcome()
      reactor.submit(backend.ops.read(in, buf), read) // finishes as it is submitted, its completion left for a turn
      reactor.close()
      assertEquals((read.completed, read.cancelled), (1, 0))
      assertEquals(text(buf), "kept")
    } finally {
      closeAlone(in)
      closeAlone(out)
    }
  }

  test("submitting to a closed reactor throws IllegalStateException") {
    val reactor = backend.open()
    reactor.close()
    intercept[IllegalStateException](reactor.submit(backend.ops.timer(0), Outcome()))
  }

  test("a close op completes after the failure of the op pending on the handle, on its reactor or another") {
    withReactor { a =>
      withReactor { b =>
        val (in, out) = pipe(a)
        val order = scala.collection.mutable.ArrayBuffer.empty[String]
        val read = new Completion[backend.ops.Op] {
          def onComplete(op: backend.ops.Op): Unit = order += "read completed"
          override def onFailure(op: backend.ops.Op, t: Throwable): Unit =
            order += (if t.isInstanceOf[ClosedChannelException] then "read failed" else s"read failed with $t")
        }
        a.submit(backend.ops.read(in, allocate(16)), read)
        a.runOnce(block = false)
        val closed = Outcome()
        a.submit(backend.ops.close(in), op => { order += "closed"; closed.onComplete(op) })
        turnUntil(a)(closed.finished)
        assertEquals(order.toList, List("read failed", "closed"))
        order.clear()
        val (in2, out2) = pipe(a)
        b.submit(backend.ops.read(in2, allocate(16)), read) // waiting on the other reactor
        b.runOnce(block = false)
        val closed2 = Outcome()
        a.submit(backend.ops.close(in2), op => { order += "closed"; closed2.onComplete(op) })
        turnUntil(a, b)(closed2.finished && order.contains("read failed"))
        close(a, out)
        close(a, out2)
      }
    }
  }
}
