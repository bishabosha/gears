package gears.async.io.testkit

import gears.async.io.{Interface, Io}
import gears.async.{Async, AsyncOperations, DeadlineTimer}

/** The same workload over the backend primitive or a timer using only the public scheduler API. */
object DeadlineTimers {
  type Timer = DeadlineTimer

  private val scheduled = new AsyncOperations {}

  enum Mode {
    case Primitive, Scheduled

    def timer(initial: Long)(using io: Io[Interface], async: Async): Timer = this match {
      case Primitive => io.deadlineTimer(initial)
      case Scheduled => scheduled.deadlineTimer(initial)
    }
  }
}
