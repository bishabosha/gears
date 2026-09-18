package example

import scala.scalanative.unsigned.UnsignedRichInt

import asyncio.unsafe.Bracket
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.pollEventsTimeout

object KQueueExampleTimer {
  def run(): Unit = {
    KqueueLoop.scoped { kq =>
      KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
        println("Creating and registering timer event...")
        KqueueLoop.addTimerOneShot(
          events(0),
          1.toUSize, // timer ID
          5000 // timeout in milliseconds
        )
      }
      println("Event registered successfully. Waiting for it to trigger...")
      println("calling kevent to wait for events...")
      var seconds = 0
      var doLoop = true
      KqueueLoop.pollQueue(1) { events =>
        while (doLoop) {
          val nev = pollEventsTimeout(kq, events, nEvents = 1, seconds = 1, 0)
          seconds += 1
          println(s"waited for $seconds seconds")
          if (nev == 0) {
            println("No events triggered within the timeout period.")
          } else {
            val event = events(0)
            println(
              s"Event triggered: ID = ${KqueueLoop.ident(event)}, Filter = ${KqueueLoop.filter(event)}, Data = ${KqueueLoop.data(event)}"
            )
            doLoop = false // Exit after handling the event
          }
        }
      }
    }
  }
}
