package asyncio.reactor

import java.io.IOException
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets

enum Interest {
  case Read, Write
}

/** Anything that can be submitted to the reactor and later completes with an `Int` result. */
trait Op

/** What to do with a descriptor once it is ready. `perform` runs a non-blocking operation, such as `NonBlocking.read`,
  * and its return value is the command's result.
  */
trait Command extends Op {
  def fd: Int
  def interest: Interest
  def perform(): Int
}

/** An op completed from outside the loop. Whoever holds it calls `complete` once, from any thread; the reactor then
  * runs the completion on its own thread, interleaved with polled events.
  */
abstract class Promise extends Op {
  @volatile private var _result = 0
  private[reactor] var reactor: Reactor | Null = null
  private[reactor] var pending: Reactor#Pending[?] | Null = null

  def result: Int = _result

  def complete(result: Int): Unit = {
    _result = result
    reactor.nn.resolved(this)
  }
}

/** A promise the reactor completes itself, by running `block` on its managed blocker pool. A thrown exception is kept
  * in `failure` and completes the promise with -1.
  */
abstract class Blocking extends Promise {
  @volatile var failure: Throwable | Null = null
  def block(): Int
}

/** What to do with an op's result. The op comes back with it, so one completion can own many ops. */
@FunctionalInterface
trait Completion[-C <: Op] {
  def onComplete(r: Reactor, op: C, result: Int): Unit
}

/** A kqueue handle with the polling loop the demos share. Objects submit a `Command` with a `Completion`; the reactor
  * waits for readiness with a one-shot filter, performs the command, and hands the completion the command and its
  * result. Each submission is its own kevent call.
  */
final class Reactor private (val kq: Int) {

  private[reactor] final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(r: Reactor, result: Int): Unit = completion.onComplete(r, op, result)
  }

  // Promises resolve on other threads; they queue here and a byte on the pipe interrupts a blocked poll.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[Promise]()
  private val (wakeRead, wakeWrite) = {
    val ends = stackalloc[CInt](2)
    if unistd.pipe(ends) < 0 then throw new IOException(s"Failed to create wake pipe: ${cError()}")
    PosixSockets.setNonBlocking(ends(0))
    PosixSockets.setNonBlocking(ends(1))
    (ends(0), ends(1))
  }
  KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
    KqueueLoop.addFile(events(0), wakeRead, read = true, clear = true) // edge-triggered, registered once
  }

  private final class Slot {
    var read: Pending[?] | Null = null
    var write: Pending[?] | Null = null
  }

  private var slots = new Array[Slot | Null](64)
  private var timers = Map.empty[Int, Event => Unit]
  private var running = false

  /** Submits an op with the completion to run when it finishes. A command is performed once its descriptor is ready for
    * its interest, one pending per descriptor and interest at a time. A promise is completed by its holder, or by the
    * blocker pool for a `Blocking`, and its completion runs on the loop thread afterwards.
    */
  def submit[C <: Op](op: C, completion: Completion[C]): Unit = {
    val pending = new Pending(op, completion)
    op match {
      case command: Command =>
        val slot = slotFor(command.fd)
        command.interest match {
          case Interest.Read =>
            require(slot.read == null, s"A read command is already pending on descriptor ${command.fd}")
            slot.read = pending
          case Interest.Write =>
            require(slot.write == null, s"A write command is already pending on descriptor ${command.fd}")
            slot.write = pending
        }
        arm(command.fd, read = command.interest == Interest.Read)
      case promise: Promise =>
        promise.reactor = this
        promise.pending = pending
        promise match {
          case blocking: Blocking =>
            Reactor.blockers.execute { () =>
              val result =
                try blocking.block()
                catch {
                  case t: Throwable =>
                    blocking.failure = t
                    -1
                }
              blocking.complete(result)
            }
          case _ => () // Completed by whoever holds it.
        }
      case other => throw new IllegalArgumentException(s"Unknown kind of op: $other")
    }
  }

  /** Called from any thread when a promise has its result: queue it and wake the loop. */
  private[reactor] def resolved(promise: Promise): Unit = {
    resolvedQueue.add(promise)
    val signal = stackalloc[Byte]()
    !signal = 1
    unistd.write(wakeWrite, signal, 1.toCSize) // A full pipe already guarantees a wake-up.
    ()
  }

  /** Runs the completions of every promise resolved so far, on the loop thread. */
  private def deliverResolved(): Unit = {
    var promise = resolvedQueue.poll()
    while promise != null do {
      val pending = promise.pending
      promise.pending = null
      if pending != null then pending.nn.complete(this, promise.result)
      promise = resolvedQueue.poll()
    }
  }

  /** Empties the wake pipe after its edge fired. */
  private def drainWakePipe(): Unit = {
    val signals = stackalloc[Byte](64)
    while unistd.read(wakeRead, signals, 64.toCSize) > 0 do ()
  }

  private def closeWakePipe(): Unit = {
    PosixSockets.close(wakeRead)
    PosixSockets.close(wakeWrite)
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
    if ident == wakeRead && isReadEvent(event) then drainWakePipe() // promises are delivered by the loop itself
    else if KqueueLoop.isTimerEvent(event) then
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
          p.op match {
            case command: Command => p.complete(this, command.perform())
            case _                => () // Only commands live in descriptor slots.
          }
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
        deliverResolved() // Interleave resolved promises with polled events.
        if running then {
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
}

object Reactor {
  type Event = KqueueLoop.Event

  /** The managed blocker pool: a few daemon threads that run `Blocking` promises off the loop thread. */
  private[reactor] object blockers {
    private val queue = new java.util.concurrent.LinkedBlockingQueue[Runnable]()
    private lazy val threads = (1 to 4).map { i =>
      val thread = new Thread(() => while true do queue.take().run(), s"reactor-blocker-$i")
      thread.setDaemon(true)
      thread.start()
      thread
    }

    def execute(task: Runnable): Unit = {
      threads
      queue.put(task)
    }
  }

  /** Opens a kqueue for `body` and closes it afterwards. */
  def scoped(body: Reactor => Unit): Unit =
    KqueueLoop.scoped { kq =>
      val reactor = new Reactor(kq)
      try body(reactor)
      finally reactor.closeWakePipe()
    }
}
