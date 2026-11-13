package gears.async.io
package testkit

import gears.async.Async

import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Try

/** What a backend does with one handle used by two programs at once, each run on a thread of its own: handles and ops
  * are the backend's, not a run's, so either run may perform them, and what one run does to a handle, the other sees.
  * On a backend whose runs each drive reactors of their own, the other run's ops are on another reactor, and one with
  * nothing else to do waits in its poll, so these are the checks that a close, a refusal or a move reaches it there.
  * For runtimes that can create host threads, as `ThreadedTimerContract` is.
  */
trait ConcurrentRunsContract extends PipeFixture {

  /** A program run on a thread of its own, started at once: `join` waits for it, and returns what it returned. */
  protected final class Elsewhere[T](body: SharedIo ?=> Async.Spawn ?=> T) {
    @volatile private var outcome: Try[T] | Null = null
    private val thread = new Thread(() => outcome = Try(runningShared(body)))
    thread.start()

    def join()(using munit.Location): T = {
      thread.join(10_000)
      assert(!thread.isAlive, "the other run did not end")
      outcome.nn.get
    }
  }

  protected def elsewhere[T](body: SharedIo ?=> Async.Spawn ?=> T): Elsewhere[T] = Elsewhere(body)

  /** A pipe opened by a run of its own, for runs to share. */
  protected def sharedPipe(): (shared.ops.Readable, shared.ops.Writable) = runningShared(io ?=> pipe(io))

  protected def closeAll(handles: shared.ops.Handle*): Unit = runningShared(io ?=> handles.foreach(close(io, _)))

  test("a repeatable op performed in one run is performed again in another") {
    val (in, out) = sharedPipe()
    try {
      val buf = allocate(16)
      val read = shared.ops.read(in, buf)
      runningShared { io ?=>
        writeAll(io, out, bytes("a"))
        io.perform(read)
      }
      elsewhere { io ?=>
        writeAll(io, out, bytes("b"))
        io.perform(read)
      }.join()
      assertEquals(text(buf), "ab")
    } finally closeAll(in, out)
  }

  test("closing a handle in one run fails the read pending on it in another, which waits on nothing else") {
    val (in, out) = sharedPipe()
    try {
      val started = new CountDownLatch(1)
      val reading = elsewhere { io ?=>
        started.countDown()
        attempt(io, io.ops.read(in, allocate(16)))
      }
      started.await()
      Thread.sleep(50) // so that the read waits, and its run with it
      runningShared(io ?=> close(io, in))
      val failure = reading.join()
      assert(failure.isInstanceOf[IOException], String.valueOf(failure))
    } finally closeAll(out)
  }

  test("closing a handle another run wrote to, while that run waits on a timer, ends its peer's stream") {
    val (in, out) = sharedPipe()
    try {
      val timer = shared.ops.deadlineTimer(System.nanoTime() + 60_000_000_000L)
      val wrote = new CountDownLatch(1)
      val writing = elsewhere { io ?=>
        writeAll(io, out, bytes("x"))
        wrote.countDown()
        io.perform(timer)
      }
      wrote.await()
      Thread.sleep(30) // so that the other run waits on its timer
      val received = runningShared { io ?=>
        close(io, out)
        val buf = allocate(16)
        readToEnd(io, io.ops.read(in, buf), buf)
      }
      timer.moveTo(System.nanoTime())
      writing.join()
      assertEquals(received, "x")
    } finally closeAll(in)
  }

  test("a read refused while another run's read is pending on the handle leaves that read, and then may be performed") {
    val (in, out) = sharedPipe()
    try {
      val started = new CountDownLatch(1)
      val first = elsewhere { io ?=>
        started.countDown()
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf))
        text(buf)
      }
      started.await()
      Thread.sleep(100) // so that the first read is pending
      runningShared { io ?=>
        val failure =
          try {
            io.perform(io.ops.read(in, allocate(16)), 1.second)
            null
          } catch case e: Exception => e
        assert(!failure.isInstanceOf[TimeoutException], "the other run's read was not yet pending")
        assert(failure.isInstanceOf[IllegalArgumentException], String.valueOf(failure))
        writeAll(io, out, bytes("x"))
      }
      assertEquals(first.join(), "x")
      val second = runningShared { io ?=>
        writeAll(io, out, bytes("y"))
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf))
        text(buf)
      }
      assertEquals(second, "y")
    } finally closeAll(in, out)
  }

  test("a one-shot op performed by two runs at once opens once, the other refused") {
    for _ <- 1 to 20 do {
      val read = Slot[shared.ops.Readable]()
      val write = Slot[shared.ops.Writable]()
      val opening = shared.ops.pipe(read, write)
      val start = new CountDownLatch(1)
      val both = List.fill(2)(elsewhere { io ?=>
        start.await()
        attempt(io, opening)
      })
      start.countDown()
      val failures = both.map(_.join())
      assertEquals(failures.count(_ == null), 1, failures.mkString(", "))
      assert(failures.exists(_.isInstanceOf[IllegalStateException]), failures.mkString(", "))
      closeAll(read.clear(), write.clear())
    }
  }
}
