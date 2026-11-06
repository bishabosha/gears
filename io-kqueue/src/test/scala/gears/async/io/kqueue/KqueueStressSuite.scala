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
