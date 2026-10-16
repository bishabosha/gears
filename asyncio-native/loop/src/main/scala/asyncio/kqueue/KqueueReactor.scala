package asyncio.kqueue

import java.io.IOException
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Completion
import asyncio.Handles
import asyncio.Interest
import asyncio.Ops
import asyncio.Reactor
import asyncio.Repeatable
import asyncio.unsafe.DescriptorHooks
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event

/** The kqueue implementation of `Reactor`, for ops built by `KqueueOps`. A command is performed as soon as it is
  * submitted when its descriptor is known to be ready, or when readiness is not known yet and the command allows it;
  * otherwise, or when it would block, it waits for readiness and is performed on the loop thread. The reactor's own
  * reads (`Command.edgeTriggered`) wait on a persistent edge-triggered filter, added once per descriptor, so waiting
  * again costs no syscall; other reads, and writes, which rarely have to wait, use a one-shot filter. Descriptors must
  * be closed through `PosixSockets.close` (as `KqueueHandles.close` does) on the reactor's thread, which tells the
  * reactor to forget them before their numbers are reused; a timer is a kqueue timer; `wakeup`, and blocking tasks
  * finishing on other threads, wake the loop with a user event. Each submission is its own kevent call, and each poll
  * returns up to `maxEvents` events.
  */
final class KqueueReactor private (val kq: Int, maxEvents: Int) extends Reactor {

  /** Every kqueue reactor runs the same kinds of op, so kqueue-specific code may build them directly. Code that only
    * sees the `Reactor` interface still cannot mix ops between reactors.
    */
  type Op = KqueueOp

  def ops: Ops[Op] = KqueueOps

  /** Kqueue watches POSIX file descriptors, so a handle is the descriptor itself. */
  def handles: Handles = KqueueHandles

  private final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(): Unit = completion.onComplete(op)
    def fail(failure: Throwable): Unit = completion.onFailure(op, failure)
    def cancelled(): Unit = completion.onCancel(op)

    /** For a command that finished when submitted: its failure, or null if it completed. */
    var failure: Throwable | Null = null
  }

  // Commands that finished as soon as they were submitted, whose completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[Pending[?]]()

  // Turns in a row that delivered completions without polling. A turn that delivered something skips the poll, since
  // the completions it ran will likely submit more work; every `KqueueReactor.PollEvery` turns it polls anyway, so
  // descriptors that only kqueue can report are not starved. Loop thread only.
  private var turnsWithoutPoll = 0

  // Promises resolve on other threads; they queue here and a user event interrupts a blocked poll.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[KqueuePromise]()
  private val WakeEvent = 0.toUSize
  KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
    KqueueLoop.addUser(events(0), WakeEvent) // edge-triggered, registered once
  }

  // Set by the first `wakeup` since the loop last saw the wake event, so later ones skip the syscall. The loop clears it
  // when it sees the event, before it runs anything, so whatever a merged wake-up was for is seen in that same turn.
  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  private final class Slot {
    var read: Pending[?] | Null = null
    var write: Pending[?] | Null = null

    /** Whether the descriptor's persistent read filter is registered. */
    var readRegistered = false

    /** What is known of the descriptor's read readiness: `Unknown`, `Ready` or `NotReady` (see `KqueueReactor`). */
    var readReadiness = KqueueReactor.Unknown

    /** Whether kqueue reported the end of the stream. It lasts, so every later read returns at once, but an
      * edge-triggered filter reports it only once; after it, the descriptor stays `Ready`.
      */
    var readEof = false

    def settleReadiness(readiness: Int): Unit = readReadiness = if readEof then KqueueReactor.Ready else readiness
  }

  // Forgets a descriptor closed on this thread: kqueue drops its filters, and its number may be reused.
  private val forgetClosed: Int => Unit = fd =>
    if fd >= 0 && fd < slots.length && slots(fd) != null then {
      val slot = slots(fd).nn
      slot.readRegistered = false
      slot.readReadiness = KqueueReactor.Unknown
      slot.readEof = false
    }
  DescriptorHooks.onClose(forgetClosed)

  private var slots = new Array[Slot | Null](64)
  private val timers = scala.collection.mutable.HashMap.empty[Int, Pending[?]]
  private var nextTimerId = 1
  private var running = false

  // Promises submitted here whose completion has not run, so `close` can cancel them. Loop thread only.
  private val pendingPromises = scala.collection.mutable.HashSet.empty[KqueuePromise]

  // Cancelled blockers whose `block` was still running, so their `onCancel` waits for it to return. Loop thread only.
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[KqueueBlocking]

  // Guards triggering the wake event against `close`, after which the kqueue's descriptor number may be reused.
  private object wakeLock {}

  // The thread that runs the loop: the one that opened the reactor until `runOnce` is called. A promise resolved on it
  // needs no wake-up, because the loop delivers every resolved promise before it polls again.
  @volatile private var loopThread: Thread = Thread.currentThread()
  @volatile private var closed = false

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = {
    op match {
      case _: Repeatable => ()
      case one: KqueueOp =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    val pending = new Pending(op, completion)
    op match {
      case command: Command =>
        val slot = slotFor(command.fd)
        val read = command.interest == Interest.Read
        // Checked before trying, so that a new command cannot take data or buffer space ahead of a waiting one.
        if read then require(slot.read == null, s"A read command is already pending on descriptor ${command.fd}")
        else require(slot.write == null, s"A write command is already pending on descriptor ${command.fd}")
        if read then {
          val attempt =
            slot.readReadiness == KqueueReactor.Ready ||
              (slot.readReadiness == KqueueReactor.Unknown && command.tryFirst)
          if !(attempt && performedAtOnce(command, pending, slot)) then {
            slot.read = pending
            waitToRead(command, slot)
          }
        } else if !(command.tryFirst && performedAtOnce(command, pending, null)) then {
          slot.write = pending
          arm(command.fd, read = false)
        }
      case timer: KqueueTimer =>
        require(timer.id < 0 || !timers.contains(timer.id), "This timer is already pending")
        if timer.id < 0 then
          timer.assign(nextTimerId)
          nextTimerId += 1
        timers(timer.id) = pending
        KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
          KqueueLoop.addTimerOneShot(events(0), timer.id.toUSize, timer.milliseconds)
        }
      case promise: KqueuePromise =>
        promise match {
          case blocking: KqueueBlocking if blocking.cancelling != null =>
            throw new IllegalStateException(s"$op is still running after being cancelled")
          case _ => ()
        }
        promise.reactor = this
        promise.pending = pending
        pendingPromises += promise
        promise match {
          case blocking: KqueueBlocking => Blockers.run(blocking)
        }
    }
    op.submitted = true
  }

  /** Performs `command` before waiting for readiness. If it finishes, or fails, its completion is queued for the next
    * turn rather than run here, and this returns true; if it would block, this returns false. For a read, `slot`
    * records what that taught about the descriptor's readiness.
    */
  private def performedAtOnce(command: Command, pending: Pending[?], slot: Slot | Null): Boolean = {
    val done =
      try command.perform()
      catch {
        case t: Throwable =>
          pending.failure = t
          true
      }
    if slot != null then
      slot.settleReadiness(
        if !command.edgeTriggered || pending.failure != null then KqueueReactor.Unknown
        else if !done then KqueueReactor.NotReady
        else readinessAfter(command, available = -1)
      )
    if done then ready.add(pending)
    done
  }

  /** The read readiness after `command` finished: from how much it read against the bytes the event reported available,
    * when both are known, or else from the command's own guess.
    */
  private def readinessAfter(command: Command, available: Long): Int = {
    val taken = command.transferred
    if available >= 0 && taken >= 0 then if taken >= available then KqueueReactor.NotReady else KqueueReactor.Ready
    else if command.mayStillBeReady then KqueueReactor.Ready
    else KqueueReactor.NotReady
  }

  /** Makes a pending read wait for readiness. An edge-triggered command uses the descriptor's persistent filter,
    * registered once; if data is already waiting, kqueue reports it on the next poll. Any other command arms a one-shot
    * filter, which replaces a persistent one, and is gone once it fires.
    */
  private def waitToRead(command: Command, slot: Slot): Unit =
    if !command.edgeTriggered then {
      slot.readRegistered = false
      arm(command.fd, read = true)
    } else if !slot.readRegistered then {
      KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
        KqueueLoop.addFile(events(0), command.fd, read = true, clear = true)
      }
      slot.readRegistered = true
    }

  /** Runs the completions of the commands that finished when submitted, as many as were queued when it started: a
    * completion that submits a command which finishes at once is left for the next turn, so one busy descriptor cannot
    * keep the loop from polling. Returns whether there were any.
    */
  private def deliverReady(): Boolean = {
    var count = ready.size
    val any = count > 0
    while count > 0 do {
      val pending = ready.poll().nn
      count -= 1
      val failure = pending.failure
      if failure != null then {
        pending.failure = null
        pending.fail(failure)
      } else pending.complete()
    }
    any
  }

  /** Called from any thread when a promise has its result: queue it and wake the loop. On the loop thread itself the
    * loop is not polling, so queueing is enough.
    */
  private[kqueue] def resolved(promise: KqueuePromise): Unit =
    if !closed then { // otherwise its completion was dropped by `close`
      resolvedQueue.add(promise)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  def wakeup(): Unit =
    if wakePending.compareAndSet(false, true) then
      wakeLock.synchronized {
        if !closed then
          KqueueLoop.createAndRegisterEvents(kq, 1)(events => KqueueLoop.triggerUser(events(0), WakeEvent))
      }

  /** Runs the completions of the promises resolved when it starts, on the loop thread. One resolved meanwhile, such as
    * by a completion, waits for the next turn, so completions that keep resolving promises cannot keep the loop from
    * polling. Returns whether there were any.
    */
  private def deliverResolved(): Boolean = {
    var remaining = resolvedQueue.size() // traverses the queue, which stays short
    var promise = if remaining > 0 then resolvedQueue.poll() else null
    val any = promise != null
    while promise != null do {
      val pending = promise.pending
      promise.pending = null
      pendingPromises -= promise
      promise match {
        case blocking: KqueueBlocking if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
          assert(pending == null, "a blocker awaiting its cancel was resubmitted")
          val cancelled = blocking.cancelling.asInstanceOf[Pending[?]]
          blocking.cancelling = null
          cancellingBlockers -= blocking
          cancelled.cancelled()
        case _ =>
          if pending != null then {
            val p = pending.asInstanceOf[Pending[?]]
            val failure = promise.failure
            if failure != null then p.fail(failure.nn) else p.complete()
          }
      }
      remaining -= 1
      promise = if remaining > 0 then resolvedQueue.poll() else null
    }
    any
  }

  /** Cancels every pending promise, interrupting blockers, then closes the kqueue. The kqueue's own filters, timers,
    * and wake event go with it. A blocker still running is waited for before its `onCancel`.
    */
  def close(): Unit = {
    if !closed then {
      // Detach every pending op first, so nothing can be delivered while the reactor shuts down.
      val dropped = scala.collection.mutable.ArrayBuffer.empty[Pending[?]]
      for slot <- slots if slot != null do {
        if slot.nn.read != null then dropped += slot.nn.read.nn
        if slot.nn.write != null then dropped += slot.nn.write.nn
      }
      java.util.Arrays.fill(slots.asInstanceOf[Array[AnyRef]], null)
      dropped ++= timers.values
      timers.clear()
      while !ready.isEmpty do dropped += ready.poll().nn // finished, but their completions have not run
      val running = scala.collection.mutable.ArrayBuffer.empty[KqueueBlocking]
      for promise <- pendingPromises.toList do {
        val pending = detach(promise)
        if pending != null then {
          dropped += pending.nn
          promise match {
            case blocking: KqueueBlocking if blocking.interrupt() => running += blocking
            case _                                                => ()
          }
        }
      }
      for blocking <- cancellingBlockers do {
        dropped += blocking.cancelling.asInstanceOf[Pending[?]]
        blocking.cancelling = null
        running += blocking
      }
      cancellingBlockers.clear()
      resolvedQueue.clear()
      wakeLock.synchronized {
        closed = true
        KqueueLoop.close(kq) // Its filters, timers, and wake event go with it.
        DescriptorHooks.removeOnClose(forgetClosed)
      }
      running.foreach(_.awaitEnd()) // Until then they may still write to their ops' buffers and slots.
      // Only now tell the completions, so one that throws cannot leak the reactor's resources.
      dropped.foreach(_.cancelled())
    }
  }

  // Polled events, only meaningful while the reactor dispatches them.

  private def isWriteEvent(event: Event): Boolean = KqueueLoop.isWriteEvent(event)
  private def fileIdent(event: Event): Int = KqueueLoop.fileIdent(event)

  /** A command that finished when submitted counts as finished: its completion still runs, and this returns false. A
    * pending command is removed from its slot and its filter deleted; a pending timer is deleted; a promise is
    * detached, so completing it later does nothing, and a blocking task is skipped or interrupted. The completion's
    * `onCancel` then runs before this returns, except for a blocking task that was running: its `onCancel` runs from
    * the loop once `block` has returned.
    */
  def cancel(op: Op): Boolean = {
    val pending: Pending[?] | Null = op match {
      case command: Command =>
        val slot = if command.fd >= 0 && command.fd < slots.length then slots(command.fd) else null
        if slot == null then null
        else {
          val read = command.interest == Interest.Read
          val p = if read then slot.read else slot.write
          if p == null || !(p.nn.op.asInstanceOf[AnyRef] eq command) then null
          else {
            // A persistent read filter stays; a one-shot filter is deleted so that it cannot fire for nothing.
            if read then slot.read = null else slot.write = null
            if !(read && command.edgeTriggered) then {
              if read then slot.readRegistered = false
              deleteFilter(command.fd, read)
            }
            p
          }
        }
      case timer: KqueueTimer =>
        val p = if timer.id < 0 then None else timers.remove(timer.id)
        p.foreach { _ =>
          KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
            KqueueLoop.deleteTimer(events(0), timer.id.toUSize)
          }
        }
        p.orNull
      case promise: KqueuePromise => detach(promise)
    }
    if pending == null then false
    else {
      op match {
        case blocking: KqueueBlocking if blocking.interrupt() =>
          blocking.cancelling = pending
          cancellingBlockers += blocking
        case _ => pending.nn.cancelled()
      }
      true
    }
  }

  /** Deletes a pending command's one-shot filter so it cannot wake the loop for nothing. If the caller already closed
    * the descriptor, kqueue removed the filter with it and reports `ENOENT` or `EBADF`; the op is cancelled all the
    * same.
    */
  private def deleteFilter(fd: Int, read: Boolean): Unit = KqueueLoop.deleteFileIfOpen(kq, fd, read)

  /** Detaches a pending promise so its outcome is never delivered. The caller skips or interrupts a blocking task. */
  private def detach(promise: KqueuePromise): Pending[?] | Null = {
    val pending = promise.pending
    promise.pending = null
    pendingPromises -= promise
    pending.asInstanceOf[Pending[?] | Null]
  }

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
    if KqueueLoop.isUserEvent(event) then wakePending.set(false) // resolved promises are delivered by `runOnce` itself
    else if KqueueLoop.isTimerEvent(event) then timers.remove(ident).foreach(_.complete())
    else {
      val slot = if ident < slots.length then slots(ident) else null
      if slot != null then {
        val write = isWriteEvent(event)
        val pending = if write then slot.write else slot.read
        if !write && KqueueLoop.isEOF(event) then slot.readEof = true
        if pending == null then {
          if !write then slot.readReadiness = KqueueReactor.Ready // kept for the next read submitted
        } else {
          if write then slot.write = null else slot.read = null
          val p = pending.nn
          p.op match {
            case command: Command =>
              // A throwing command fails its own op; an exception thrown by the completion itself still escapes.
              var failure: Throwable | Null = null
              val done =
                try command.perform()
                catch {
                  case t: Throwable =>
                    failure = t
                    true
                }
              // Readiness is settled before the completion runs, since it may submit the next read at once.
              if !write then
                slot.settleReadiness(
                  if failure != null || !command.edgeTriggered then KqueueReactor.Unknown
                  else if !done then KqueueReactor.NotReady
                  else readinessAfter(command, KqueueLoop.data(event).toLong)
                )
              if failure != null then p.fail(failure.nn)
              else if done then p.complete()
              else if write then {
                // Not ready after all: wait again, without the completion ever seeing it.
                slot.write = p
                arm(ident, read = false)
              } else {
                slot.read = p
                waitToRead(command, slot)
              }
            case _ => () // Only commands live in descriptor slots.
          }
        }
      }
    }
  }

  /** Delivers commands that finished when submitted and resolved promises, polls once, dispatches what it polled, then
    * delivers promises resolved meanwhile. It blocks in the poll only if asked to and nothing was delivered first, and
    * skips the poll after delivering something, except every `KqueueReactor.PollEvery` turns. Error events throw.
    */
  def runOnce(block: Boolean): Unit =
    KqueueLoop.pollQueue(maxEvents) { events =>
      loopThread = Thread.currentThread()
      val delivered = deliverReady() | deliverResolved() // both, without short-circuiting
      val polled =
        if !delivered then {
          turnsWithoutPoll = 0
          if block then KqueueLoop.pollEventsForever(kq, events, maxEvents)
          else KqueueLoop.pollEventsNow(kq, events, maxEvents)
        } else if turnsWithoutPoll < KqueueReactor.PollEvery then {
          turnsWithoutPoll += 1
          0
        } else {
          turnsWithoutPoll = 0
          KqueueLoop.pollEventsNow(kq, events, maxEvents)
        }
      var i = 0
      while i < polled do {
        val event = events(i)
        if KqueueLoop.isError(event) then throw new IOException(s"Event error: ${KqueueLoop.errno(event)}")
        dispatch(event)
        i += 1
      }
      deliverResolved() // including any whose wake-up was merged into an event just dispatched
      ()
    }

  def run(): Unit = {
    running = true
    while running do runOnce(block = true)
  }
}

object KqueueReactor {

  // What a slot knows of its descriptor's read readiness. `Ready`: kqueue reported it readable, or the last read
  // probably left data, so a read is performed at once. `NotReady`: a read would block, so the next waits for an edge.
  // `Unknown`: a new or reused descriptor, which a read may try first.
  private inline val Unknown = 0
  private inline val Ready = 1
  private inline val NotReady = 2

  /** A loop that keeps delivering completions polls kqueue at least once in this many turns. */
  private inline val PollEvery = 16

  /** Opens a reactor on a new kqueue; each poll returns up to `maxEvents` events. Close it when done. */
  def open(maxEvents: Int = 255): KqueueReactor = new KqueueReactor(KqueueLoop.open(), maxEvents)

  /** Opens a reactor for `body` and closes it afterwards. */
  def scoped(maxEvents: Int = 255)(body: KqueueReactor => Unit): Unit = {
    val reactor = open(maxEvents)
    try body(reactor)
    finally reactor.close()
  }
}
