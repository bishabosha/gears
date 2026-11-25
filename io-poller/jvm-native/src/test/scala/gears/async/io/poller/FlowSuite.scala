package gears.async.io
package poller

import gears.async.Async
import gears.async.Future
import gears.async.io.flow.Flow
import gears.async.io.flow.Flow.*

import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

/** Flows: programs of ops performed without suspending anyone, run from within one future. */
class FlowSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def pipe(io: Io[Pipes])(using Async): (io.ops.Readable, io.ops.Writable) = {
    val read = Slot[io.ops.Readable]()
    val write = Slot[io.ops.Writable]()
    io.perform(io.ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  test("two flows of one future echo to each other over pipes") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    val rounds = 20_000
    Io.run { io ?=>
      val (requestIn, requestOut) = pipe(io)
      val (replyIn, replyOut) = pipe(io)
      var echoed = 0
      var wrong = 0
      val echo = Flow(io) {
        val buf = ByteBuffer.allocateDirect(8)
        val read = io.ops.read(requestIn, buf)
        val write = io.ops.write(replyOut, buf)
        repeat(rounds) {
          exec(buf.clear())
          loop(buf.hasRemaining())(perform(read))
          exec(buf.flip())
          loop(buf.hasRemaining())(perform(write))
          exec { echoed += 1 }
        }
      }
      val client = Flow(io) {
        val out = ByteBuffer.allocateDirect(8)
        val in = ByteBuffer.allocateDirect(8)
        val write = io.ops.write(requestOut, out)
        val read = io.ops.read(replyIn, in)
        var i = 0L
        loop(i < rounds) {
          exec { out.clear(); out.putLong(i); out.flip() }
          loop(out.hasRemaining())(perform(write))
          exec(in.clear())
          loop(in.hasRemaining())(perform(read))
          exec { if in.getLong(0) != i then wrong += 1; i += 1 }
        }
      }
      Flow.run(io)(echo, client)
      assertEquals(echoed, rounds)
      assertEquals(wrong, 0)
    }
  }

  test("a flow's timers take their time, one after the other") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val started = System.nanoTime()
      Flow.run(io)(Flow(io)(repeat(3)(perform(io.ops.timer(10)))))
      assert(System.nanoTime() - started >= 30_000_000L)
    }
  }

  test("a flow's failure is thrown from its run, and ends the flows run with it") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val boom = new IllegalStateException("boom")
      val failing = Flow(io) {
        perform(io.ops.timer(10))
        exec(throw boom)
      }
      var after = false
      val waiting = Flow(io) {
        perform(io.ops.timer(60_000))
        exec { after = true }
      }
      val started = System.nanoTime()
      assertEquals(intercept[IllegalStateException](Flow.run(io)(waiting, failing)), boom)
      assert(System.nanoTime() - started < 30_000_000_000L)
      assert(!after)
    }
  }

  test("a time limit on a flow's op fails the flow as it fails a perform") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      intercept[TimeoutException](Flow.run(io)(Flow(io)(perform(io.ops.timer(60_000), 10.millis))))
    }
  }

  test("cancelling the future a flow is run from ends the flow where it waits") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      var after = false
      val running = Future {
        Flow.run(io)(Flow(io) {
          perform(io.ops.timer(60_000))
          exec { after = true }
        })
      }
      io.perform(io.ops.timer(20))
      running.cancel()
      val outcome = running.awaitResult // outside the assertion, which holds a monitor over its condition
      assert(outcome.failed.toOption.exists(_.isInstanceOf[CancellationException]))
      assert(!after)
    }
  }

  test("a flow whose ops never wait still gives its worker to other futures") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val buf = ByteBuffer.allocateDirect(1)
      val write = io.ops.write(out, buf)
      val read = io.ops.read(in, buf)
      var rounds = 0
      @volatile var stop = false
      val busy = Future {
        Flow.run(io)(Flow(io) {
          loop(!stop) {
            exec(buf.clear())
            perform(write)
            exec(buf.clear())
            perform(read)
            exec { rounds += 1 }
          }
        })
      }
      // Tasks of its own worker's, which only run if the flow lets go of it.
      val others = List.fill(8)(Future(io.perform(io.ops.timer(20))))
      others.foreach(_.await)
      stop = true
      busy.await
      assert(rounds > 0)
    }
  }

  test("the flows of one run share plain variables, whichever workers their ops are settled on") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val runs = List.fill(16)(Future {
        var count = 0
        val flows = List.fill(4)(Flow(io) {
          repeat(200) {
            perform(io.ops.timer(0))
            exec { count += 1 }
          }
        })
        Flow.run(io)(flows*)
        count
      })
      assertEquals(runs.map(_.await), List.fill(16)(800))
    }
  }
}
