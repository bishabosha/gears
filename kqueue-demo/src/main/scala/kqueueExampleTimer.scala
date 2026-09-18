package example

import asyncio.unsafe.KqueueLoop
import KQueueExampleIO.*

object KQueueExampleTimer {
  def run(): Unit = {
    KqueueLoop.scoped { kq =>
      println("Creating and registering timer event...")
      registerTimerOneShot(kq, id = 1, milliseconds = 5000)
      println("Event registered successfully. Waiting for it to trigger...")
      println("calling kevent to wait for events...")
      var seconds = 0
      val tick = () => {
        seconds += 1
        println(s"waited for $seconds seconds")
        println("No events triggered within the timeout period.")
        true
      }
      pollLoop(kq, capacity = 1, timeoutSeconds = 1)(tick) { event =>
        println(describe(event))
        false // Exit after handling the event
      }
    }
  }
}
