package gears.async.io
package kqueue

import gears.async.Future

import scala.concurrent.duration.*

/** Timers that complete as soon as they are scheduled, so that completion races the performer's wait for it, and
  * submissions race each worker going to sleep.
  */
class KqueueStressSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  test("many short timers all complete") {
    given KqueueBackend.type = KqueueBackend
    Io.run { io ?=>
      for round <- 1 to 3000 do {
        val futures = for _ <- 1 to 16 yield Future(io.perform(io.backend.ops.timer(0)))
        futures.foreach(_.await)
      }
    }
  }

  test("a timer cancelled as it fires leaves its next performance to run its full time") {
    given KqueueBackend.type = KqueueBackend
    Io.run { io ?=>
      val delay = 300.micros
      val timer = io.backend.ops.timer(delay)
      for round <- 1 to 2000 do {
        val waiting = Future(io.perform(timer))
        val cancelAt = System.nanoTime() + delay.toNanos - 100_000L + (round % 20) * 10_000L // either side of it firing
        while System.nanoTime() < cancelAt do Thread.onSpinWait()
        waiting.cancel()
        waiting.awaitResult: Unit
        val started = System.nanoTime()
        io.perform(timer)
        assert(System.nanoTime() - started >= delay.toNanos, s"round $round fired early")
      }
    }
  }

  test("a timer cancelled by a waiter resumed from the same batch of timers still finishes") {
    given KqueueBackend.type = KqueueBackend
    Io.run { io ?=>
      for round <- 1 to 200 do {
        val deadline = System.nanoTime() + 5_000_000L
        // More futures than workers, so that some share a heap: the first resumed cancels the rest of its batch.
        val waiters = new Array[Future[Unit]](4 * Runtime.getRuntime.availableProcessors)
        for i <- waiters.indices do
          waiters(i) = Future {
            io.perform(io.backend.ops.deadlineTimer(deadline))
            for j <- waiters.indices if j != i && waiters(j) != null do waiters(j).cancel()
          }
        waiters.foreach(_.awaitResult)
      }
    }
  }

  test("timers shorter than a millisecond complete, the worker sleeping for less than one") {
    given KqueueBackend.type = KqueueBackend
    Io.run { io ?=>
      for round <- 1 to 300 do {
        val futures = for _ <- 1 to 4 yield Future(io.perform(io.backend.ops.timer(300.micros)))
        futures.foreach(_.await)
      }
    }
  }
}
