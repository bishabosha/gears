package asyncio.reactor

import java.io.IOException
import scala.scalanative.unsigned.*

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event

enum Interest {
  case Read, Write
}

/** What to do with a descriptor once it is ready. `perform` runs a non-blocking operation, such as `NonBlocking.read`,
  * and its return value is the command's result.
  */
trait Command {
  def fd: Int
  def interest: Interest
  def perform(): Int
}

/** What to do with a command's result. The command comes back with it, so one completion can own many commands. */
@FunctionalInterface
trait Completion[-C <: Command] {
  def onComplete(r: Reactor, command: C, result: Int): Unit
}

/** A kqueue handle with the polling loop the demos share. Objects submit a `Command` with a `Completion`; the reactor
  * waits for readiness with a one-shot filter, performs the command, and hands the completion the command and its
  * result. Each submission is its own kevent call.
  */
final class Reactor private (val kq: Int) {

  private final class Pending[C <: Command](val command: C, val completion: Completion[C]) {
    def complete(r: Reactor, result: Int): Unit = completion.onComplete(r, command, result)
  }

  private final class Slot {
    var read: Pending[?] | Null = null
    var write: Pending[?] | Null = null
  }

  private var slots = new Array[Slot | Null](64)
  private var timers = Map.empty[Int, Event => Unit]
  private var running = false

  /** Performs `command` once its descriptor is ready for its interest, then hands `completion` the result. One pending
    * command per descriptor and interest at a time.
    */
  def submit[C <: Command](command: C, completion: Completion[C]): Unit = {
    val slot = slotFor(command.fd)
    val pending = new Pending(command, completion)
    command.interest match {
      case Interest.Read =>
        require(slot.read == null, s"A read command is already pending on descriptor ${command.fd}")
        slot.read = pending
      case Interest.Write =>
        require(slot.write == null, s"A write command is already pending on descriptor ${command.fd}")
        slot.write = pending
    }
    arm(command.fd, read = command.interest == Interest.Read)
  }

  private def registerTimerOneShot(id: Int, milliseconds: Int): Unit =
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

  /** Runs `onFire` with the event when a one-shot timer expires. */
  def submitTimer(id: Int, milliseconds: Int)(onFire: Event => Unit): Unit = {
    timers = timers.updated(id, onFire)
    registerTimerOneShot(id, milliseconds)
  }

  /** Forgets any pending commands on `fd`. Closing the descriptor removes its kqueue filters. */
  def forget(fd: Int): Unit =
    if fd < slots.length then slots(fd) = null

  def stop(): Unit = running = false

  private def slotFor(fd: Int): Slot = {
    require(fd >= 0, "Descriptor must be non-negative")
    if fd >= slots.length then slots = java.util.Arrays.copyOf(slots, math.max(slots.length * 2, fd + 1))
    var slot = slots(fd)
    if slot == null then {
      slot = new Slot
      slots(fd) = slot
    }
    slot.nn
  }

  /** One kevent call per command: the filter is one-shot, so nothing fires without a command waiting for it. */
  private def arm(fd: Int, read: Boolean): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addFile(events(0), fd, read = read, clear = false, oneShot = true)
    }

  /** Finds the command an event is for, performs it, and hands its completion the result. The slot is cleared first so
    * the completion may submit the next command.
    */
  private def dispatch(event: Event): Unit = {
    val ident = fileIdent(event)
    if KqueueLoop.isTimerEvent(event) then
      timers.get(ident).foreach { onFire =>
        timers = timers.removed(ident)
        onFire(event)
      }
    else {
      val slot = if ident < slots.length then slots(ident) else null
      if slot != null then {
        val write = isWriteEvent(event)
        val pending = if write then slot.write else slot.read
        if pending != null then {
          if write then slot.write = null else slot.read = null
          val p = pending.nn
          p.complete(this, p.command.perform())
        }
      }
    }
  }

  /** Dispatches events to their commands until `stop` is called, passing up to `capacity` events per poll. Error events
    * throw.
    */
  def run(capacity: Int = 255): Unit =
    run(capacity, timeoutSeconds = -1)(() => true)

  /** Like the untimed `run`, but `onTimeout` runs whenever `timeoutSeconds` pass without an event and decides whether
    * polling continues.
    */
  def run(capacity: Int, timeoutSeconds: Int)(onTimeout: () => Boolean): Unit =
    KqueueLoop.pollQueue(capacity) { events =>
      running = true
      while running do {
        val polled =
          if timeoutSeconds < 0 then KqueueLoop.pollEventsForever(kq, events, capacity)
          else KqueueLoop.pollEventsTimeout(kq, events, capacity, timeoutSeconds, 0)
        if polled == 0 then running = onTimeout()
        var i = 0
        while i < polled && running do {
          val event = events(i)
          if KqueueLoop.isError(event) then throw new IOException(s"Event error: ${KqueueLoop.errno(event)}")
          dispatch(event)
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
