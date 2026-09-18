package example

import asyncio.reactor.Reactor
import KQueueExampleIO.*

object KQueueExampleTimer {
  def run(): Unit = {
    Reactor.scoped { r =>
      println("Creating and registering timer event...")
      r.registerTimerOneShot(id = 1, milliseconds = 5000)
      println("Event registered successfully. Waiting for it to trigger...")
      println("calling kevent to wait for events...")
      var seconds = 0
      val tick = () => {
        seconds += 1
        println(s"waited for $seconds seconds")
        println("No events triggered within the timeout period.")
        true
      }
      r.run(capacity = 1, timeoutSeconds = 1)(tick) { event =>
        println(r.describe(event))
        false // Exit after handling the event
      }
    }
  }
}
