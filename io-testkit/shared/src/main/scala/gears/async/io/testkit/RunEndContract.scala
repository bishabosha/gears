package gears.async.io
package testkit

import gears.async.Future

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

/** What a run leaves when its program returns: the ops its futures still had under way are cancelled, at once, and the
  * handles they used left for the next run; a blocking op is interrupted, and the run waits for its body to return; and
  * a time limit met in time leaves nothing to wait for. For runtimes that can create host threads, as
  * `ThreadedTimerContract` is.
  */
trait RunEndContract extends PipeFixture {
  val backend: Backend { type Interface <: Blocking & Pipes }

  private def seconds(since: Long): Double = (System.nanoTime() - since) / 1e9

  test("a run that returns with a read and a long timer pending cancels them at once, and leaves the handle usable") {
    val started = System.nanoTime()
    val (in, out) = runningShared { io ?=>
      val ends = pipe(io)
      Future(io.perform(io.ops.read(ends._1, allocate(16))))
      Future(io.perform(io.ops.timer(60_000)))
      settle(io)
      ends
    }
    assert(seconds(started) < 5, s"the run took ${seconds(started)} s to end")
    val received = runningShared { io ?=>
      try {
        writeAll(io, out, bytes("y"))
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf)) // no read of the ended run is pending on the handle
        text(buf)
      } finally {
        close(io, in)
        close(io, out)
      }
    }
    assertEquals(received, "y")
  }

  test("a run that returns while a blocking op runs interrupts it, and ends once its body has returned") {
    val entered = new CountDownLatch(1)
    val returned = new AtomicBoolean
    val started = System.nanoTime()
    running { io ?=>
      Future(
        io.perform(
          io.ops.blocking { () =>
            entered.countDown()
            try Thread.sleep(10_000)
            catch case _: InterruptedException => ()
            returned.set(true)
          },
          60.seconds
        )
      )
      while entered.getCount() > 0 do io.perform(io.ops.timer(5))
    }
    assert(returned.get(), "the run ended before the blocking op's body returned")
    assert(seconds(started) < 5, s"the run took ${seconds(started)} s to end: the body was not interrupted")
  }

  test("ops performed with long time limits, met in time, leave nothing for the run's end to wait for") {
    val started = System.nanoTime()
    running { io ?=>
      for _ <- 1 to 3 do io.perform(io.ops.blocking(() => ()), 60.seconds)
      val (in, out) = pipe(io)
      try {
        writeAll(io, out, bytes("z"))
        val buf = allocate(16)
        io.perform(io.ops.read(in, buf), 60.seconds)
        assertEquals(text(buf), "z")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
    assert(seconds(started) < 5, s"the run took ${seconds(started)} s to end")
  }
}
