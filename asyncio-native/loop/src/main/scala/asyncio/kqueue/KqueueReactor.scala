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
import asyncio.unsafe.DescriptorTable
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event

/** The kqueue implementation of `Reactor`, for ops built by `KqueueOps`. A command is performed as soon as it is
  * submitted when its descriptor is known to be ready, or when readiness is not known yet and the command allows it;
  * otherwise, or when it would block, it waits for readiness and is performed on the loop thread. The reactor's own
  * reads (`Command.edgeTriggered`) wait on a persistent edge-triggered filter, added once per descriptor, so waiting
  * again costs no syscall; other reads, and writes, which rarely have to wait, use a one-shot filter. Descriptors must
  * be closed through `PosixSockets.close` (as `KqueueHandles.close` does) on the reactor's thread, which tells the
  * reactor to forget them before their numbers are reused; timers are kept in a heap on the loop thread, by deadline,
  * and bound how long a poll blocks, so setting or cancelling one costs no syscall; `wakeup`, and blocking tasks
  * finishing on other threads, wake the loop with a user event. Each submission is its own kevent call, and each poll
  * returns up to `maxEvents` events.
  */
final class KqueueReactor private (val kq: Int, maxEvents: Int, spinPolls: Int) extends Reactor {

  /** Every kqueue reactor runs the same kinds of op, so kqueue-specific code may build them directly, and an op may go to
    * any kqueue reactor in turn (see `KqueueBackend`).
    */
  type Op = KqueueOp

  def ops: Ops[Op] = KqueueOps

  /** Kqueue watches POSIX file descriptors, so a handle is the descriptor itself. */
  def handles: Handles = KqueueHandles

  /** Takes a pending op's completion off it, ending the submission: the completion may then submit the op again. */
  private def take(op: KqueueOp): Completion[KqueueOp] = {
    val completion = op.completion.nn
    op.completion = null
    completion
  }

  private def complete(op: KqueueOp): Unit = take(op).onComplete(op)
  private def fail(op: KqueueOp, failure: Throwable): Unit = take(op).onFailure(op, failure)

  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  // Commands that finished as soon as they were submitted, whose completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[KqueueOp]()

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
    var read: KqueueOp | Null = null
    var write: KqueueOp | Null = null

    /** Whether the descriptor's persistent read filter is registered. */
    var readRegistered = false

    /** What is known of the descriptor's read readiness: `Unknown`, `Ready` or `NotReady` (see `KqueueReactor`). */
    var readReadiness = KqueueReactor.Unknown

    /** Whether kqueue reported the end of the stream. It lasts, so every later read returns at once, but an
      * edge-triggered filter reports it only once; after it, the descriptor stays `Ready`.
      */
    var readEof = false

    /** What kqueue last reported waiting to be read, bytes or, on a listening socket, connections, less what reads have
      * taken since; -1 when not known.
      */
    var readAvailable = -1L

    /** The descriptor's generation in `DescriptorTable` when this slot last checked; a different one means its number
      * was closed and reused since.
      */
    var generation = 0

    def settleReadiness(readiness: Int): Unit = readReadiness = if readEof then KqueueReactor.Ready else readiness

    /** Forgets what is known of reading the descriptor: it was closed, or another reactor reads it now. */
    def forget(): Unit = {
      readRegistered = false
      readReadiness = KqueueReactor.Unknown
      readEof = false
      readAvailable = -1
    }
  }

  /** Distinguishes this reactor from the others sharing descriptors with it, in `DescriptorTable`. */
  private val id = KqueueReactor.nextId.incrementAndGet()

  // Forgets a descriptor closed on this thread: kqueue drops its filters, and its number may be reused. A close on
  // another thread shows as a new generation instead (see `currentSlot`).
  private val forgetClosed: Int => Unit = fd =>
    if fd >= 0 && fd < slots.length && slots(fd) != null then slots(fd).nn.forget()
  DescriptorHooks.onClose(forgetClosed)

  private var slots = new Array[Slot | Null](64)
  private val timers = new TimerHeap
  private var nextTimerOrder = 0L
  private var running = false

  // Promises submitted here whose completion has not run, so `close` can cancel them. Loop thread only.
  private val pendingPromises = scala.collection.mutable.HashSet.empty[KqueuePromise]

  // Cancelled blockers whose `block` was still running, so their `onCancel` waits for it to return. Loop thread only.
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[KqueueBlocking]

  // Guards triggering the wake event against `close`, after which the kqueue's descriptor number may be reused.
  private object wakeLock {}

  // The thread that runs the loop: the one that opens the reactor, which owns it, or the one that calls `run`. A promise
  // resolved on it needs no wake-up, because the loop delivers every resolved promise before it polls again. Not
  // refreshed by every `runOnce`: on macOS `Thread.currentThread` is a call into thread-local storage.
  @volatile private var loopThread: Thread = Thread.currentThread()
  @volatile private var closed = false

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = submitOrFinish(op, completion, finishNow = false): Unit

  def trySubmit[C <: Op](op: C, completion: Completion[C]): Boolean = submitOrFinish(op, completion, finishNow = true)

  /** Submits `op`. A command tried at once that finishes queues its completion for the next turn, or, with `finishNow`,
    * takes the completion back off it uncalled, throws a failure, and returns true.
    */
  private def submitOrFinish[C <: Op](op: C, completion: Completion[C], finishNow: Boolean): Boolean = {
    var finished = false
    op match {
      case _: Repeatable => ()
      case one: KqueueOp =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[KqueueOp]] // only ever called with `op`, which is a C
    op match {
      case command: Command =>
        val slot = currentSlot(command.fd)
        val read = command.interest == Interest.Read
        // Checked before trying, so that a new command cannot take data or buffer space ahead of a waiting one.
        if read then require(slot.read == null, s"A read command is already pending on descriptor ${command.fd}")
        else require(slot.write == null, s"A write command is already pending on descriptor ${command.fd}")
        command.completion = pending
        if read then {
          val attempt =
            slot.readReadiness == KqueueReactor.Ready ||
              (slot.readReadiness == KqueueReactor.Unknown && command.tryFirst)
          if attempt && performedAtOnce(command, slot, queue = !finishNow) then finished = finishNow
          else {
            slot.read = command
            waitToRead(command, slot)
          }
        } else if command.tryFirst && performedAtOnce(command, null, queue = !finishNow) then finished = finishNow
        else {
          slot.write = command
          if command.registersReads && !slot.readRegistered then armWithReads(command.fd, slot)
          else arm(command.fd, read = false)
        }
      case timer: KqueueTimer =>
        require(timer.heapIndex < 0, "This timer is already pending")
        timer.deadline = System.nanoTime() + timer.milliseconds * 1_000_000L
        timer.order = nextTimerOrder
        nextTimerOrder += 1
        timer.completion = pending
        timers.add(timer)
      case promise: KqueuePromise =>
        promise match {
          case blocking: KqueueBlocking if blocking.cancelling != null =>
            throw new IllegalStateException(s"$op is still running after being cancelled")
          case _ => ()
        }
        promise.reactor = this
        promise.completion = pending
        pendingPromises += promise
        promise match {
          case blocking: KqueueBlocking => Blockers.run(blocking)
        }
    }
    op.submitted = true
    if finished then {
      op.completion = null // finished here, so the completion is never called
      val failure = op.atOnceFailure
      if failure != null then {
        op.atOnceFailure = null
        throw failure
      }
    }
    finished
  }

  /** Performs `command` before waiting for readiness. If it finishes, or fails, its completion is queued for the next
    * turn rather than run here, and this returns true; if it would block, this returns false. For a read, `slot`
    * records what that taught about the descriptor's readiness.
    */
  private def performedAtOnce(command: Command, slot: Slot | Null, queue: Boolean): Boolean = {
    val done =
      try command.perform()
      catch {
        case t: Throwable =>
          command.atOnceFailure = t
          true
      }
    if slot != null then settleAfterRead(command, slot, done, command.atOnceFailure != null, slot.readAvailable)
    if done && queue then ready.add(command)
    done
  }

  /** Records what a read command's perform taught about its descriptor's read readiness. After one that finished, that
    * comes from what it took against what was `available`, when both are known, or else from the command's own guess.
    * Knowing a descriptor drained saves the next read a try that would block: once the count runs out it waits for an
    * edge, which anything arriving later triggers.
    */
  private def settleAfterRead(command: Command, slot: Slot, done: Boolean, failed: Boolean, available: Long): Unit =
    if failed || !command.edgeTriggered then {
      slot.readAvailable = -1
      slot.settleReadiness(KqueueReactor.Unknown)
    } else if !done then {
      slot.readAvailable = -1
      slot.settleReadiness(KqueueReactor.NotReady)
    } else {
      val taken = command.transferred
      val left = if available >= 0 && taken >= 0 then math.max(available - taken, 0L) else -1L
      slot.readAvailable = if left > 0 then left else -1
      slot.settleReadiness(
        if left > 0 || (left < 0 && command.mayStillBeReady) then KqueueReactor.Ready else KqueueReactor.NotReady
      )
    }

  /** Makes a pending read wait for readiness. An edge-triggered command uses the descriptor's persistent filter,
    * registered once; if data is already waiting, kqueue reports it on the next poll. Any other command arms a one-shot
    * filter, which replaces a persistent one, and is gone once it fires.
    */
  private def waitToRead(command: Command, slot: Slot): Unit =
    if !command.edgeTriggered then {
      slot.readRegistered = false
      DescriptorTable.claimRead(command.fd, id) // so that another reactor's persistent filter is dropped
      arm(command.fd, read = true)
    } else if !slot.readRegistered then {
      DescriptorTable.claimRead(command.fd, id)
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
      val op = ready.poll().nn
      count -= 1
      val failure = op.atOnceFailure
      if failure != null then {
        op.atOnceFailure = null
        fail(op, failure)
      } else complete(op)
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
      val completion = promise.completion
      promise.completion = null
      pendingPromises -= promise
      promise match {
        case blocking: KqueueBlocking if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
          assert(completion == null, "a blocker awaiting its cancel was resubmitted")
          val cancelled = blocking.cancelling.nn
          blocking.cancelling = null
          cancellingBlockers -= blocking
          cancelled.onCancel(blocking)
        case _ =>
          if completion != null then {
            val failure = promise.failure
            if failure != null then completion.onFailure(promise, failure.nn) else completion.onComplete(promise)
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
      val dropped = scala.collection.mutable.ArrayBuffer.empty[(KqueueOp, Completion[KqueueOp])]
      def drop(op: KqueueOp): Unit = dropped += ((op, take(op)))
      for slot <- slots if slot != null do {
        if slot.nn.read != null then drop(slot.nn.read.nn)
        if slot.nn.write != null then drop(slot.nn.write.nn)
      }
      java.util.Arrays.fill(slots.asInstanceOf[Array[AnyRef]], null)
      while !timers.isEmpty do drop(timers.poll())
      while !ready.isEmpty do drop(ready.poll().nn) // finished, but their completions have not run
      val running = scala.collection.mutable.ArrayBuffer.empty[KqueueBlocking]
      for promise <- pendingPromises.toList do {
        val completion = detach(promise)
        if completion != null then {
          dropped += ((promise, completion))
          promise match {
            case blocking: KqueueBlocking if blocking.interrupt() => running += blocking
            case _                                                => ()
          }
        }
      }
      for blocking <- cancellingBlockers do {
        dropped += ((blocking, blocking.cancelling.nn))
        blocking.cancelling = null
        running += blocking
      }
      cancellingBlockers.clear()
      resolvedQueue.clear()
      wakeLock.synchronized {
        closed = true
        KqueueLoop.close(kq) // Its filters, timers, and wake event go with it.
        KqueueLoop.freeQueue(events)
        DescriptorHooks.removeOnClose(forgetClosed)
      }
      running.foreach(_.awaitEnd()) // Until then they may still write to their ops' buffers and slots.
      // Only now tell the completions, so one that throws cannot leak the reactor's resources.
      dropped.foreach((op, completion) => completion.onCancel(op))
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
    val completion: Completion[KqueueOp] | Null = op match {
      case command: Command =>
        val slot = if command.fd >= 0 && command.fd < slots.length then slots(command.fd) else null
        if slot == null then null
        else {
          val read = command.interest == Interest.Read
          val p = if read then slot.read else slot.write
          if p == null || !(p eq command) then null
          else {
            // A persistent read filter stays; a one-shot filter is deleted so that it cannot fire for nothing.
            if read then slot.read = null else slot.write = null
            if !(read && command.edgeTriggered) then {
              if read then slot.readRegistered = false
              deleteFilter(command.fd, read)
            }
            take(command)
          }
        }
      case timer: KqueueTimer =>
        if timer.heapIndex < 0 then null
        else {
          timers.remove(timer)
          take(timer)
        }
      case promise: KqueuePromise => detach(promise)
    }
    if completion == null then false
    else {
      op match {
        case blocking: KqueueBlocking if blocking.interrupt() =>
          blocking.cancelling = completion
          cancellingBlockers += blocking
        case _ => completion.onCancel(op)
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
  private def detach(promise: KqueuePromise): Completion[KqueueOp] | Null = {
    val completion = promise.completion
    promise.completion = null
    pendingPromises -= promise
    completion
  }

  def stop(): Unit = running = false

  /** `fd`'s slot, brought up to date for a submission. What it knew no longer holds if the descriptor's number was closed
    * and reused since, on any thread, or if another reactor has registered a read filter for it since this one did; it
    * then starts over, as for a new descriptor. A filter of this reactor's that is still registered is harmless: a new
    * registration replaces it, and an event it reports first is dropped by `dispatch`.
    */
  private def currentSlot(fd: Int): Slot = {
    val slot = slotFor(fd)
    val entry = DescriptorTable.get(fd)
    val generation = DescriptorTable.generation(entry)
    if generation != slot.generation then {
      slot.forget()
      slot.generation = generation
    } else if slot.readRegistered && DescriptorTable.readOwner(entry) != id then slot.forget()
    slot
  }

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

  /** Arms a one-shot write filter, for a command such as a connect, and adds the descriptor's persistent read filter in
    * the same call. Nothing can then arrive unreported, so the descriptor starts out not ready to read, and its first
    * read waits for an edge instead of trying a read that would block; data already waiting is reported as soon as the
    * filter is added.
    */
  private def armWithReads(fd: Int, slot: Slot): Unit = {
    DescriptorTable.claimRead(fd, id)
    KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
      KqueueLoop.addFile(events(0), fd, read = false, clear = false, oneShot = true)
      KqueueLoop.addFile(events(1), fd, read = true, clear = true)
    }
    slot.readRegistered = true
    slot.readAvailable = -1
    slot.settleReadiness(KqueueReactor.NotReady)
  }

  /** Finds the command an event is for, performs it, and hands its completion the result. The slot is cleared first so
    * the completion may submit the next command.
    */
  private def dispatch(event: Event): Unit = {
    val ident = fileIdent(event)
    if KqueueLoop.isUserEvent(event) then wakePending.set(false) // resolved promises are delivered by `runOnce` itself
    else {
      val slot = if ident < slots.length then slots(ident) else null
      if slot != null then {
        val write = isWriteEvent(event)
        val pending = if write then slot.write else slot.read
        if pending == null then {
          if !write then {
            val entry = DescriptorTable.get(ident)
            if DescriptorTable.readOwner(entry) != id || DescriptorTable.generation(entry) != slot.generation then {
              // Another reactor reads the descriptor now, or its number was reused: this filter is left over. Drop it,
              // so this reactor no longer wakes for it; it is registered again if a read here needs it.
              KqueueLoop.deleteFileIfOpen(kq, ident, read = true)
              slot.forget()
            } else { // kept for the next read submitted
              if KqueueLoop.isEOF(event) then slot.readEof = true
              slot.readReadiness = KqueueReactor.Ready
              slot.readAvailable = KqueueLoop.data(event).toLong
            }
          }
        } else {
          if !write && KqueueLoop.isEOF(event) then slot.readEof = true
          if write then slot.write = null else slot.read = null
          pending.nn match {
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
              if !write then settleAfterRead(command, slot, done, failure != null, KqueueLoop.data(event).toLong)
              if failure != null then fail(command, failure.nn)
              else if done then complete(command)
              else if write then {
                // Not ready after all: wait again, without the completion ever seeing it.
                slot.write = command
                arm(ident, read = false)
              } else {
                slot.read = command
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
  // The buffer polls fill, allocated once and freed by `close`, rather than on the stack every turn.
  private val events = KqueueLoop.allocateQueue(maxEvents)

  def runOnce(block: Boolean): Unit = {
    val delivered = deliverReady() | deliverResolved() // both, without short-circuiting
    val polled =
      if !delivered then {
        turnsWithoutPoll = 0
        if block then pollBeforeBlocking(events)
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
    fireDueTimers()
    deliverResolved() // including any whose wake-up was merged into an event just dispatched
  }

  /** Polls without blocking up to `spinPolls` times, then blocks if nothing turned up. In request/response traffic the
    * awaited reply often arrives within microseconds, and catching it this way saves the thread a sleep and a wake-up.
    */
  private def pollBeforeBlocking(events: KqueueLoop.EventQueue): Int = {
    var polled = 0
    var spins = 0
    while polled == 0 && spins < spinPolls do {
      polled = KqueueLoop.pollEventsNow(kq, events, maxEvents)
      spins += 1
    }
    if polled != 0 then polled
    else if timers.isEmpty then KqueueLoop.pollEventsForever(kq, events, maxEvents)
    else {
      // Block no longer than until the earliest timer is due, or not at all if it already is.
      val wait = timers.peek.deadline - System.nanoTime()
      if wait <= 0 then KqueueLoop.pollEventsNow(kq, events, maxEvents)
      else
        KqueueLoop.pollEventsTimeout(
          kq,
          events,
          maxEvents,
          (wait / 1_000_000_000L).toInt,
          (wait % 1_000_000_000L).toInt
        )
    }
  }

  /** Completes the timers that are due, earliest first. Only timers submitted before it started fire, so a completion
    * that submits a zero-length timer leaves it for the next turn.
    */
  private def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.deadline <= now && timers.peek.order < submittedBefore do {
        complete(timers.poll())
      }
    }

  /** Pending timers in a binary min-heap ordered by deadline, then submission order. Each timer knows its index, so it
    * can be removed when cancelled.
    */
  private final class TimerHeap {
    private var heap = new Array[KqueueTimer | Null](16)
    private var size = 0

    def isEmpty: Boolean = size == 0
    def peek: KqueueTimer = heap(0).nn

    def add(timer: KqueueTimer): Unit = {
      if size == heap.length then heap = java.util.Arrays.copyOf(heap, size * 2)
      place(timer, size)
      size += 1
      siftUp(size - 1)
    }

    def poll(): KqueueTimer = {
      val timer = peek
      remove(timer)
      timer
    }

    def remove(timer: KqueueTimer): Unit = {
      val index = timer.heapIndex
      size -= 1
      val last = heap(size).nn
      heap(size) = null
      timer.heapIndex = -1
      if index != size then {
        place(last, index)
        siftDown(index)
        siftUp(index)
      }
    }

    private def before(a: KqueueTimer, b: KqueueTimer): Boolean =
      a.deadline < b.deadline || (a.deadline == b.deadline && a.order < b.order)

    private def place(timer: KqueueTimer, index: Int): Unit = {
      heap(index) = timer
      timer.heapIndex = index
    }

    private def siftUp(start: Int): Unit = {
      var index = start
      val timer = heap(index).nn
      var moving = true
      while moving && index > 0 do {
        val parent = (index - 1) / 2
        if before(timer, heap(parent).nn) then {
          place(heap(parent).nn, index)
          index = parent
        } else moving = false
      }
      place(timer, index)
    }

    private def siftDown(start: Int): Unit = {
      var index = start
      val timer = heap(index).nn
      var moving = true
      while moving do {
        val left = 2 * index + 1
        if left >= size then moving = false
        else {
          val right = left + 1
          val child = if right < size && before(heap(right).nn, heap(left).nn) then right else left
          if before(heap(child).nn, timer) then {
            place(heap(child).nn, index)
            index = child
          } else moving = false
        }
      }
      place(timer, index)
    }
  }

  def run(): Unit = {
    loopThread = Thread.currentThread()
    running = true
    while running do runOnce(block = true)
  }
}

object KqueueReactor {

  /** The last id given to a reactor (see `DescriptorTable`); ids start at 1, since 0 means no owner. */
  private val nextId = new java.util.concurrent.atomic.AtomicInteger(0)

  // What a slot knows of its descriptor's read readiness. `Ready`: kqueue reported it readable, or the last read
  // probably left data, so a read is performed at once. `NotReady`: a read would block, so the next waits for an edge.
  // `Unknown`: a new or reused descriptor, which a read may try first.
  private inline val Unknown = 0
  private inline val Ready = 1
  private inline val NotReady = 2

  /** A loop that keeps delivering completions polls kqueue at least once in this many turns. */
  private inline val PollEvery = 16

  /** Opens a reactor on a new kqueue; each poll returns up to `maxEvents` events. Before blocking for events it polls
    * without blocking up to `spinPolls` times, trading a little CPU for not sleeping when an event is about to arrive;
    * 0 blocks at once. Close it when done.
    */
  def open(maxEvents: Int = 255, spinPolls: Int = DefaultSpinPolls): KqueueReactor = {
    require(spinPolls >= 0, "spinPolls must not be negative")
    new KqueueReactor(KqueueLoop.open(), maxEvents, spinPolls)
  }

  /** Non-blocking polls before blocking, by default: the benchmark's request/response scenarios gained most of what
    * spinning offers by 4.
    */
  inline val DefaultSpinPolls = 4

  /** Opens a reactor for `body` and closes it afterwards. */
  def scoped(maxEvents: Int = 255, spinPolls: Int = DefaultSpinPolls)(body: KqueueReactor => Unit): Unit = {
    val reactor = open(maxEvents, spinPolls)
    try body(reactor)
    finally reactor.close()
  }
}
