package asyncio.reactor

import java.io.IOException
import scala.scalanative.unsigned.*

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event

/** A kqueue handle with the registration calls and polling loop the demos share. Each registration change is its own
  * kevent call, and `run` dispatches every polled event to one handler, exactly as the demos did inline.
  */
final class Reactor private (val kq: Int) {

  def registerRead(fd: Int, clear: Boolean = false): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addFile(events(0), fd, read = true, clear = clear)
    }

  def registerWrite(fd: Int, clear: Boolean = false): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addFile(events(0), fd, read = false, clear = clear)
    }

  /** Stops watching for writability and starts watching for readability. */
  def switchToRead(fd: Int): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
      KqueueLoop.deleteFile(events(0), fd, read = false)
      KqueueLoop.addFile(events(1), fd, read = true, clear = false)
    }

  /** Stops watching for readability and starts watching for writability. */
  def switchToWrite(fd: Int): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
      KqueueLoop.deleteFile(events(0), fd, read = true)
      KqueueLoop.addFile(events(1), fd, read = false, clear = false)
    }

  def registerTimerOneShot(id: Int, milliseconds: Int): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addTimerOneShot(events(0), id.toUSize, milliseconds)
    }

  // Polled events

  def isReadEvent(event: Event): Boolean = KqueueLoop.isReadEvent(event)
  def isWriteEvent(event: Event): Boolean = KqueueLoop.isWriteEvent(event)
  def isEOF(event: Event): Boolean = KqueueLoop.isEOF(event)

  /** The descriptor a read or write event is for. */
  def fileIdent(event: Event): Int = KqueueLoop.fileIdent(event)

  /** The bytes that can be read or written without blocking, as reported with the event. */
  def available(event: Event): Int = KqueueLoop.rwAvailable(event)

  def describe(event: Event): String =
    s"Event triggered: ID = ${KqueueLoop.ident(event)}, Filter = ${KqueueLoop.filter(event)}, Data = ${KqueueLoop.data(event)}"

  /** Polls until `handle` returns false, passing up to `capacity` events per poll. Error events throw. */
  def run(capacity: Int)(handle: Event => Boolean): Unit =
    run(capacity, timeoutSeconds = -1)(() => true)(handle)

  /** Like the untimed `run`, but `onTimeout` runs whenever `timeoutSeconds` pass without an event and decides whether
    * polling continues.
    */
  def run(capacity: Int, timeoutSeconds: Int)(onTimeout: () => Boolean)(handle: Event => Boolean): Unit =
    KqueueLoop.pollQueue(capacity) { events =>
      var doPoll = true
      while doPoll do {
        val polled =
          if timeoutSeconds < 0 then KqueueLoop.pollEventsForever(kq, events, capacity)
          else KqueueLoop.pollEventsTimeout(kq, events, capacity, timeoutSeconds, 0)
        if polled == 0 then doPoll = onTimeout()
        var i = 0
        while i < polled && doPoll do {
          val event = events(i)
          if KqueueLoop.isError(event) then throw new IOException(s"Event error: ${KqueueLoop.errno(event)}")
          doPoll = handle(event)
          i += 1
        }
      }
    }
}

object Reactor {
  type Event = KqueueLoop.Event

  /** Opens a kqueue for `body` and closes it afterwards. */
  def scoped(body: Reactor => Unit): Unit =
    KqueueLoop.scoped(kq => body(new Reactor(kq)))
}
