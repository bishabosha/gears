package gears.async.io
package kqueue

import gears.async.io.Repeatable
import gears.async.io.internal.Completion
import gears.async.io.internal.DescriptorTable
import gears.async.io.internal.Interest
import gears.async.io.internal.Reactor
import gears.async.io.kqueue.KqueueLoop
import gears.async.io.kqueue.KqueueLoop.Event

import java.io.IOException
import java.util.concurrent.TimeoutException
import scala.scalanative.posix.string
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** The kqueue implementation of `Reactor`, for ops built by `KqueueOps`. A command is performed as soon as it is
  * submitted when its descriptor is known to be ready, or when readiness is not known yet and the command allows it;
  * otherwise, or when it would block, it waits for readiness and is performed on the loop thread. The reactor's own
  * reads (`Command.edgeTriggered`) wait on a persistent edge-triggered filter, added once per descriptor, so waiting
  * again costs no syscall; other reads, and writes, which rarely have to wait, use a one-shot filter. Handles must be
  * closed through the backend, by the `close` op or `KqueueHandles.close`, from any thread, which tells every reactor
  * (`closed`): the commands pending on the descriptor fail, and its slot is forgotten before the number is reused;
  * timers, and the ops with a time limit, are kept in a heap on the loop thread, by deadline, and bound how long a poll
  * blocks, so setting or cancelling one costs no syscall, and an op that times out is withdrawn as a cancelled one is,
  * with `onFailure` instead of `onCancel`; `wakeup`, and blocking tasks finishing on other threads, wake the loop with
  * a user event. Each submission is its own kevent call, and each poll returns up to `maxEvents` events.
  *
  * It also runs ops of another I/O facility, for a backend built on kqueue's reactors (`Notified`): attempted when
  * submitted and again when the facility notifies the reactor for them, with that facility's events taken in each turn
  * by its `ReactorExtension`, which wakes a blocked poll with a user event of its own.
  */
final class KqueueReactor private (
    val kq: Int,
    maxEvents: Int,
    spinPolls: Int,
    builtBy: Ops[KqueueOp, KqueueBackend.Handle]
) extends Reactor
    with gears.async.io.internal.TimerWaiter {

  /** Every kqueue reactor runs the same kinds of op, so kqueue-specific code may build them directly, and an op may go
    * to any kqueue reactor in turn (see `KqueueBackend`).
    */
  type Op = KqueueOp
  type Handle = KqueueBackend.Handle

  /** The ops of the backend that opened it: `KqueueOps`, or those of a backend built on kqueue's reactors. */
  def ops: Ops[KqueueOp, Handle] = builtBy

  /** Takes a pending op's completion off it, ending the submission, and its time limit with it: the completion may then
    * submit the op again.
    */
  /** Lets go of `op`'s claim on its handle (see `claim`), as its completion is taken or it finishes in `trySubmit`. */
  private def release(op: KqueueOp): Unit = {
    val claim = op.claim
    if claim != null then claim.release(op)
  }

  // Ops submitted here waiting for their handle's turn (see `Turns`), and those given it, from any thread, for the loop
  // to go on submitting (`deliverGiven`).
  private val awaiting = scala.collection.mutable.HashSet.empty[KqueueOp]
  private val givenQueue = new java.util.concurrent.ConcurrentLinkedQueue[KqueueOp]()

  /** Leaves `op`, pending, to wait for the turn `turns` holds for another op, its time limit running meanwhile. */
  private def awaitTurn(
      op: KqueueOp,
      turns: gears.async.io.internal.Turns,
      pending: Completion[KqueueOp],
      timeoutMillis: Int
  ): Unit = {
    op.completion = pending
    if timeoutMillis >= 0 then schedule(op, timeoutMillis)
    awaiting += op
    turns.await(
      op,
      () => {
        givenQueue.add(op)
        if !(Thread.currentThread() eq loopThread) then wakeup()
      }
    )
  }

  /** Goes on submitting the ops given their turn since the last turn of the loop, still waiting for it here. */
  private def deliverGiven(): Boolean = {
    var any = false
    var op = givenQueue.poll()
    while op != null do {
      val claim = op.claim
      // One withdrawn meanwhile, and perhaps submitted again since, holds no turn it was given before.
      if claim != null && claim.holds(op) && awaiting.remove(op) then {
        any = true
        val pending = op.completion.nn
        op.completion = null
        submitHolding(op, pending, null, finishNow = false, timeoutMillis = -1): Unit // its time limit is filed
      }
      op = givenQueue.poll()
    }
    any
  }

  private def take(op: KqueueOp): Completion[KqueueOp] = {
    val completion = op.completion.nn
    op.completion = null
    release(op)
    if op.heapIndex >= 0 then timers.remove(op)
    op match {
      case timer: KqueueTimer => timer.waiter = null // a move no longer concerns this reactor
      case _                  => ()
    }
    completion
  }

  // Timers whose deadline moved earlier, told from another thread, for the loop to file again (`moved`).
  private val movedQueue = new java.util.concurrent.ConcurrentLinkedQueue[KqueueTimer]()

  /** A pending timer's deadline moved earlier than it is filed under: filed again here, or by the loop, woken. */
  def moved(timer: gears.async.io.internal.TimerState): Unit = timer match {
    case t: KqueueTimer =>
      if Thread.currentThread() eq loopThread then refile(t)
      else {
        movedQueue.add(t)
        wakeup()
      }
    case _ => ()
  }

  /** Files `timer` again under its key, if it is still pending here. Loop thread only. */
  private def refile(timer: KqueueTimer): Unit =
    if timer.heapIndex >= 0 && timer.completion != null then {
      timers.remove(timer)
      file(timer)
    }

  /** Files the timers moved earlier from other threads since the last turn. */
  private def deliverMoved(): Unit = {
    var timer = movedQueue.poll()
    while timer != null do {
      refile(timer)
      timer = movedQueue.poll()
    }
  }

  /** Puts `timer` in the heap under its key (see `TimerState`). */
  private def file(timer: KqueueTimer): Unit = {
    val key = timer.key
    timer.when = key
    timer.filed = key
    timer.order = nextTimerOrder
    nextTimerOrder += 1
    timers.add(timer)
  }

  private def complete(op: KqueueOp): Unit = deliverComplete(take(op), op)

  // Close ops that closed their handle at once, whose completions run at the end of the turn after the one they were
  // queued in: by then that reactor has delivered the failures of the ops they ended there, so that a close completes
  // after them (`Ops.close`). Loop thread only.
  private val closing = new java.util.ArrayDeque[KqueueOp]()

  /** Runs the completions of the first `due` close ops queued, those queued before this turn began. */
  private def deliverClosing(due: Int): Unit = {
    var n = due
    while n > 0 do {
      val op = closing.poll().nn
      n -= 1
      val failure = op.atOnceFailure
      if failure != null then {
        op.atOnceFailure = null
        fail(op, failure)
      } else complete(op)
    }
  }
  private def fail(op: KqueueOp, failure: Throwable): Unit = deliverFailure(take(op), op, failure)

  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  // Commands that finished as soon as they were submitted, whose completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[KqueueOp]()

  // Another facility's ops waiting to be notified, by the token of their submission (`Notified`). Loop thread only.
  private val waiting = scala.collection.mutable.LongMap.empty[Notified]
  private var nextToken = 1L

  // Tokens of those ops their facility notified: from this thread, unboxed; and from any other, boxed, which is rare,
  // such as another reactor taking in events of a handle an op here waits on.
  private val notifiedHere = new KqueueReactor.Tokens
  private val notifiedElsewhere = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Long]()

  /** The other facility this reactor drives, if any (see `ReactorExtension`). */
  @volatile private[io] var extension: ReactorExtension | Null = null

  /** The reactor's extension, made by `make` if it has none, its user event registered. Loop thread only. */
  private[io] def extend(make: => ReactorExtension): ReactorExtension = {
    val existing = extension
    if existing != null then existing
    else {
      KqueueLoop.createAndRegisterEvents(kq, 1)(events => KqueueLoop.addUser(events(0), KqueueReactor.ExtensionEvent))
      val made = make
      extension = made
      made
    }
  }

  /** The other facility tells the reactor that the op of `token`, waiting here, is worth attempting again. Any thread.
    */
  private[io] def notified(token: Long): Unit =
    if Thread.currentThread() eq loopThread then notifiedHere.add(token)
    else {
      notifiedElsewhere.add(token)
      wakeup()
    }

  /** Takes in the extension's events, which notify the ops they finish, then attempts the ops notified: one that
    * finishes has its completion run; one that must still wait arranged again to be notified by its attempt. A token no
    * longer waiting, a cancelled op's, is dropped. Returns whether there were any.
    */
  private def deliverNotified(): Boolean = {
    val ext = extension
    var any = ext != null && ext.deliver()
    var token = notifiedElsewhere.poll()
    while token != null do {
      notifiedHere.add(token.longValue)
      token = notifiedElsewhere.poll()
    }
    while !notifiedHere.isEmpty do {
      any = true
      val t = notifiedHere.poll()
      val op = waiting.getOrNull(t) // no `Option` for each: a token no longer waiting is simply not found
      if op != null then {
        waiting -= t
        if attemptedNotified(op, first = false) then {
          val failure = op.atOnceFailure
          if failure != null then {
            op.atOnceFailure = null
            fail(op, failure)
          } else complete(op)
        } else waiting.put(t, op)
      }
    }
    any
  }

  /** Attempts `op`: true if it finished, or failed, with the failure kept for its completion. */
  private def attemptedNotified(op: Notified, first: Boolean): Boolean =
    try op.attempt(this, first)
    catch {
      case t: Throwable =>
        op.atOnceFailure = t
        true
    }

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

    /** Whether reads must always be tried (`DescriptorTable.alwaysTry`): a short read leaves readiness unknown, not
      * `NotReady`, since the read filter of a regular file or a FIFO may never report the end that follows it.
      */
    var alwaysTry = false

    def settleReadiness(readiness: Int): Unit = readReadiness = if readEof then KqueueReactor.Ready else readiness

    /** Forgets what is known of reading the descriptor: it was closed, or another reactor reads it now. */
    def forget(): Unit = {
      readRegistered = false
      readReadiness = KqueueReactor.Unknown
      readEof = false
      readAvailable = -1
    }
  }

  /** For measurement: descriptors this reactor took over from another, and read filters it dropped because another had
    * taken the descriptor over. On the reactor's thread only; read them once it has stopped.
    */
  var takenOver = 0L
  var filtersDropped = 0L

  /** The counters, for a runner printing its reactors' statistics. */
  override def toString: String = s"kqueue(taken over $takenOver, filters dropped $filtersDropped)"

  /** Distinguishes this reactor from the others sharing descriptors with it, in `DescriptorTable`. */
  private val id = KqueueReactor.nextId.incrementAndGet()

  // Descriptors close on any thread (`KqueueHandles.closeDescriptor`): each close queues here, as the number with the
  // generation that ended, and wakes a blocked poll, so that the commands pending on it fail (see `closed`).
  private val closedQueue = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Long]()

  // Descriptors `FifoWatcher` found readable, packed as `closedQueue`'s are, for the loop (`deliverSelected`).
  private val selectedQueue = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Long]()

  private var slots = new Array[Slot | Null](64)
  // Pending timers, and pending ops with a time limit, by deadline (see `fireDueTimers`).
  private val timers = new gears.async.io.internal.TimerHeap[KqueueOp]
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

  def submit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Unit =
    submitOrFinish(op, completion, finishNow = false, timeoutMillis): Unit

  def trySubmit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Boolean =
    submitOrFinish(op, completion, finishNow = true, timeoutMillis)

  /** Submits `op`. A command tried at once that finishes queues its completion for the next turn, or, with `finishNow`,
    * takes the completion back off it uncalled, throws a failure, and returns true. An op left waiting, with a time
    * limit, goes into the timer heap to time out when it is due.
    */
  private def submitOrFinish[C <: Op](
      op: C,
      completion: Completion[C],
      finishNow: Boolean,
      timeoutMillis: Int
  ): Boolean = {
    if closed then throw new IllegalStateException(s"The reactor is closed: $op")
    op.markSubmitted()
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[KqueueOp]] // only ever called with `op`, which is a C
    val slotRefusal = op.slotRefusal
    val claim = op.claim
    if slotRefusal == null && claim != null && !claim.take(op) && op.waitsItsTurn then {
      awaitTurn(op, claim, pending, timeoutMillis)
      false
    } else {
      val refusal =
        if slotRefusal != null then slotRefusal
        else if claim != null && !claim.holds(op) then gears.async.io.Ops.alreadyPending(op)
        else null
      submitHolding(op, pending, refusal, finishNow, timeoutMillis)
    }
  }

  /** Goes on submitting `op`, which holds its handle's turn if it takes one, failing it with `refusal` if not null. */
  private def submitHolding(
      op: KqueueOp,
      pending: Completion[KqueueOp],
      refusal: Throwable | Null,
      finishNow: Boolean,
      timeoutMillis: Int
  ): Boolean = {
    var finished = false
    var waits = false
    if refusal != null then {
      op.completion = pending
      op.atOnceFailure = refusal
      if finishNow then finished = true else ready.add(op)
    } else
      op match {
        case connect: ConnectTo if !opened(connect, pending, finishNow) => finished = finishNow
        case command: Command                                           =>
          // The command's first try and its registration are a use of its handle's descriptor (see `Command.handle`).
          val handle = command.handle
          if handle != null && !handle.acquire() then {
            command.completion = pending
            command.atOnceFailure = KqueueHandles.notOpen(handle)
            if finishNow then finished = true else ready.add(command)
          } else
            try {
              val read = command.interest == Interest.Read
              val slot = currentSlot(command.fd, read)
              // Checked before trying, so that a new command cannot take data or buffer space ahead of a waiting one.
              if read then require(slot.read == null, s"A read command is already pending on descriptor ${command.fd}")
              else require(slot.write == null, s"A write command is already pending on descriptor ${command.fd}")
              command.completion = pending
              val refusal = command.refusal
              if refusal != null then {
                command.atOnceFailure = refusal
                if finishNow then finished = true else ready.add(command)
              } else if read then {
                val attempt =
                  slot.readReadiness == KqueueReactor.Ready ||
                    (slot.readReadiness == KqueueReactor.Unknown && command.tryFirst)
                if attempt && performedAtOnce(command, slot, queue = !finishNow) then finished = finishNow
                else {
                  slot.read = command
                  val failed = registrationFailure(command, slot, read = true)(waitToRead(command, slot))
                  if failed == null then waits = true
                  else {
                    command.atOnceFailure = failed
                    if finishNow then finished = true else ready.add(command)
                  }
                }
              } else if command.tryFirst && performedAtOnce(command, null, queue = !finishNow) then finished = finishNow
              else {
                slot.write = command
                val failed = registrationFailure(command, slot, read = false)(
                  if command.registersReads && !slot.readRegistered then armWithReads(command.fd, slot)
                  else arm(command.fd, read = false)
                )
                if failed == null then waits = true
                else {
                  command.atOnceFailure = failed
                  if finishNow then finished = true else ready.add(command)
                }
              }
            } finally if handle != null then handle.release()
        case notified: Notified =>
          notified.token = nextToken
          nextToken += 1
          notified.completion = pending
          val done =
            try {
              notified.begin()
              attemptedNotified(notified, first = true)
            } catch {
              case t: Throwable => // an argument its facility refuses, too, fails the op, never a half-made submit
                notified.atOnceFailure = t
                true
            }
          if !done then {
            waiting.put(notified.token, notified)
            waits = true
          } else if notified.closes then closing.add(notified) // after the failures it causes, never in trySubmit
          else if finishNow then finished = true
          else ready.add(notified)
        case timer: KqueueTimer =>
          require(timer.heapIndex < 0, "This timer is already pending")
          // Due at its time, or failed at its time limit if that comes first (see `TimerState`).
          timer.start(System.nanoTime(), timeoutMillis)
          timer.completion = pending
          timer.waiter = this
          file(timer)
        case close: Close =>
          // Closed here, as an at-once op is, unless a use of the descriptor is in progress: then pending, as a promise,
          // until the last use ends and closes it.
          close.reset()
          close.reactor = this
          close.completion = pending
          if close.start() then closing.add(close) // completed after the failures it causes, never in trySubmit
          else {
            pendingPromises += close
            waits = true
          }
        case promise: KqueuePromise =>
          promise match {
            case blocking: KqueueBlocking if blocking.cancelling != null =>
              throw new IllegalStateException(s"$op is still running after being cancelled")
            case _ => ()
          }
          promise.reset() // a `Repeatable` promise's previous outcome
          promise.reactor = this
          promise.completion = pending
          pendingPromises += promise
          promise match {
            case blocking: KqueueBlocking =>
              blocking.restart()
              gears.async.io.internal.Blockers.run(blocking)
          }
          waits = true
        case atOnce: KqueueAtOnce =>
          atOnce.completion = pending
          try atOnce.perform()
          catch case t: Throwable => atOnce.atOnceFailure = t
          if finishNow then finished = true else ready.add(atOnce)
      }
    if waits && timeoutMillis >= 0 then schedule(op, timeoutMillis)
    if finished then {
      op.completion = null // finished here, so the completion is never called
      release(op)
      val failure = op.atOnceFailure
      if failure != null then {
        op.atOnceFailure = null
        throw failure
      }
    }
    finished
  }

  /** Opens the socket of a `ConnectTo` as it is submitted, and returns true; or, if that fails, delivers the failure as
    * an at-once op's is, queued for the next turn or, with `finishNow`, thrown by `submitOrFinish`, and returns false.
    */
  private def opened(connect: ConnectTo, pending: Completion[KqueueOp], finishNow: Boolean): Boolean =
    try {
      connect.open()
      true
    } catch {
      case t: Throwable =>
        connect.completion = pending
        connect.atOnceFailure = t
        if !finishNow then ready.add(connect)
        false
    }

  /** Puts `op` in the timer heap, due in `milliseconds`: a timer, to complete then, or an op with a time limit, to time
    * out then (see `fireDueTimers`).
    */
  private def schedule(op: KqueueOp, milliseconds: Int): Unit = {
    op.when = System.nanoTime() + milliseconds * 1_000_000L
    op.order = nextTimerOrder
    nextTimerOrder += 1
    timers.add(op)
  }

  /** Performs `command` before waiting for readiness. If it finishes, or fails, its completion is queued for the next
    * turn if `queue`, rather than run here, and this returns true; if it would block, this returns false. For a read,
    * `slot` records what that taught about the descriptor's readiness.
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

  /** Runs `register`, which registers the filter `command` waits on, from its `slot`, and returns its failure, or null.
    * A registration that fails, such as with `EBADF` for a descriptor closed behind the reactor's back, takes the
    * command out of its slot, so that the command fails alone, through its completion, and the loop goes on.
    */
  private inline def registrationFailure(command: Command, slot: Slot, read: Boolean)(
      inline register: Unit
  ): Throwable | Null =
    try {
      register
      null
    } catch {
      case t: Throwable =>
        if read then {
          slot.read = null
          slot.readRegistered = false
        } else slot.write = null
        t
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
        if left > 0 || (left < 0 && command.mayStillBeReady) then KqueueReactor.Ready
        else if slot.alwaysTry then KqueueReactor.Unknown
        else KqueueReactor.NotReady
      )
    }

  /** Makes a pending read wait for readiness. An edge-triggered command uses the descriptor's persistent filter,
    * registered once; if data is already waiting, kqueue reports it on the next poll. Any other command arms a one-shot
    * filter, which replaces a persistent one, and is gone once it fires.
    */
  private def waitToRead(command: Command, slot: Slot): Unit = {
    if slot.alwaysTry then watchSelected(command.fd, slot.generation)
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
  }

  /** Has `FifoWatcher` watch `fd`, an always-tried descriptor (a FIFO or a regular file) with a read waiting, whose
    * filter may never fire for its end: the watcher tells this reactor once `select` reports it readable.
    */
  private def watchSelected(fd: Int, generation: Int): Unit = {
    val packed = (generation.toLong << 32) | (fd.toLong & 0xffffffffL)
    gears.async.io.internal.FifoWatcher.watch(fd, this, () => selected(packed))
  }

  /** From `FifoWatcher`'s thread: the descriptor packed in `packed` is readable. */
  private def selected(packed: Long): Unit =
    if !closed then {
      selectedQueue.add(packed)
      wakeup()
    }

  /** Attempts the reads waiting on the descriptors `FifoWatcher` found readable since the last turn, as a read filter
    * firing would, where the slot still knows that descriptor and a read waits. Returns whether there were any.
    */
  private def deliverSelected(): Boolean = {
    var any = false
    var packed = selectedQueue.poll()
    while packed != null do {
      val fd = (packed.longValue & 0xffffffffL).toInt
      val generation = (packed.longValue >>> 32).toInt
      if fd >= 0 && fd < slots.length then {
        val slot = slots(fd)
        if slot != null && slot.nn.generation == generation && slot.nn.read != null then {
          any = true
          dispatchFile(fd, write = false, eof = false, data = -1L)
        }
      }
      packed = selectedQueue.poll()
    }
    any
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

  /** Called from any thread when descriptor `fd`, whose generation in `DescriptorTable` was `generation`, is closed:
    * the commands pending on it here fail, from the loop, as `Ops.close` says, and its slot forgets it. A slot that
    * knows a later generation knows a reused number, and is left alone.
    */
  private[kqueue] def closed(fd: Int, generation: Int): Unit =
    if !closed then {
      closedQueue.add((generation.toLong << 32) | (fd.toLong & 0xffffffffL))
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  /** Fails the commands pending on the descriptors closed since the last turn, where the slot still knows that
    * descriptor and not a reused number. Returns whether any were.
    */
  private def deliverClosed(): Boolean = {
    var any = false
    var packed = closedQueue.poll()
    while packed != null do {
      val fd = (packed.longValue & 0xffffffffL).toInt
      val generation = (packed.longValue >>> 32).toInt
      if fd >= 0 && fd < slots.length then {
        val slot = slots(fd)
        if slot != null && slot.nn.generation == generation then any |= failClosed(slot.nn, fd)
      }
      packed = closedQueue.poll()
    }
    any
  }

  /** Queues the failure of the commands pending in `slot`, whose descriptor was closed, as commands that failed when
    * submitted, so their completions run from the next turn and never inside a submit; and forgets the descriptor.
    * Returns whether there were any.
    */
  private def failClosed(slot: Slot, fd: Int): Boolean = {
    val read = slot.read
    val write = slot.write
    slot.read = null
    slot.write = null
    slot.forget()
    if read != null then {
      read.atOnceFailure = new IOException(s"Descriptor $fd was closed")
      ready.add(read)
    }
    if write != null then {
      write.atOnceFailure = new IOException(s"Descriptor $fd was closed")
      ready.add(write)
    }
    read != null || write != null
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
      if promise.heapIndex >= 0 then timers.remove(promise) // its time limit ends with it
      pendingPromises -= promise
      promise match {
        case blocking: KqueueBlocking if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
          assert(completion == null, "a blocker awaiting its cancel was resubmitted")
          val cancelled = blocking.cancelling.nn
          val failure = blocking.cancelledBy
          blocking.cancelling = null
          blocking.cancelledBy = null
          cancellingBlockers -= blocking
          if failure == null then deliverCancel(cancelled, blocking) else deliverFailure(cancelled, blocking, failure)
        case _ =>
          if completion != null then {
            val failure = promise.failure
            if failure != null then deliverFailure(completion, promise, failure.nn)
            else deliverComplete(completion, promise)
          }
      }
      remaining -= 1
      promise = if remaining > 0 then resolvedQueue.poll() else null
    }
    any
  }

  /** Cancels every pending op, interrupting blockers, then closes the kqueue. The kqueue's own filters and wake event
    * go with it. A blocker still running is waited for before its `onCancel`. An op that had already finished, whose
    * completion had not run yet, gets its outcome, not a cancel: what it read or opened is the caller's.
    */
  def close(): Unit = {
    if !closed then {
      // Detach every pending op first, so nothing can be delivered while the reactor shuts down.
      val dropped = scala.collection.mutable.ArrayBuffer.empty[(KqueueOp, Completion[KqueueOp])]
      val finished = scala.collection.mutable.ArrayBuffer.empty[(KqueueOp, Completion[KqueueOp], Throwable | Null)]
      def drop(op: KqueueOp): Unit = dropped += ((op, take(op)))
      for slot <- slots if slot != null do {
        if slot.nn.read != null then drop(slot.nn.read.nn)
        if slot.nn.write != null then drop(slot.nn.write.nn)
      }
      java.util.Arrays.fill(slots.asInstanceOf[Array[AnyRef]], null)
      for op <- awaiting do {
        val claim = op.claim
        if claim != null then claim.withdraw(op): Unit
        drop(op)
      }
      awaiting.clear()
      givenQueue.clear()
      for (_, op) <- waiting.toList do {
        op.withdraw()
        drop(op)
      }
      waiting.clear()
      notifiedHere.clear()
      notifiedElsewhere.clear()
      while !ready.isEmpty do { // finished, but their completions have not run
        val op = ready.poll().nn
        val failure = op.atOnceFailure
        op.atOnceFailure = null
        finished += ((op, take(op), failure))
      }
      while !closing.isEmpty do { // closed their handles, but their completions have not run
        val op = closing.poll().nn
        val failure = op.atOnceFailure
        op.atOnceFailure = null
        finished += ((op, take(op), failure))
      }
      var resolved = resolvedQueue.poll()
      while resolved != null do { // finished on another thread; a cancelled blocker among them is settled below
        resolved match {
          case blocking: KqueueBlocking if blocking.cancelling != null => ()
          case promise                                                 =>
            val completion = detach(promise)
            if completion != null then finished += ((promise, completion, promise.failure))
        }
        resolved = resolvedQueue.poll()
      }
      val running = scala.collection.mutable.ArrayBuffer.empty[KqueueBlocking]
      // Blockers that wrote their result: as good as finished, so they get their outcome once they return.
      val written = scala.collection.mutable.ArrayBuffer.empty[(KqueueBlocking, Completion[KqueueOp])]
      for promise <- pendingPromises.toList do {
        val completion = detach(promise)
        if completion != null then
          promise match {
            case blocking: KqueueBlocking if !blocking.cancelUnlessCommitted() =>
              written += ((blocking, completion))
              running += blocking
            case _ =>
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
        blocking.cancelledBy = null
        running += blocking
      }
      cancellingBlockers.clear()
      // What is left in the heap are timers: the other ops in it left it as they were taken or detached above.
      while !timers.isEmpty do drop(timers.poll())
      resolvedQueue.clear()
      closedQueue.clear()
      selectedQueue.clear()
      val ext = extension
      if ext != null then ext.close() // nothing triggers its event from here on
      wakeLock.synchronized {
        closed = true
        KqueueLoop.close(kq) // Its filters and wake event go with it.
        KqueueLoop.freeQueue(events)
        KqueueReactor.all.remove(this)
      }
      running.foreach(_.awaitEnd()) // Until then they may still write to their ops' buffers and slots.
      for (blocking, completion) <- written do finished += ((blocking, completion, blocking.thrownBy))
      // Only now tell the completions, so one that throws cannot leak the reactor's resources.
      finished.foreach { (op, completion, failure) =>
        if failure == null then deliverComplete(completion, op) else deliverFailure(completion, op, failure)
      }
      dropped.foreach { (op, completion) =>
        op match {
          case opens: Opens       => opens.abandon() // what it opened never reaches the caller
          case notified: Notified => notified.abandon()
          case _                  => ()
        }
        deliverCancel(completion, op)
      }
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
    val completion = withdraw(op)
    if completion == null then false
    else {
      settleWithdrawn(op, completion, null)
      true
    }
  }

  /** Fails `op`, whose time limit ran out, with `TimeoutException`: withdrawn as a cancelled op is, then `onFailure`
    * where that would be `onCancel`. Nothing if it is no longer pending.
    */
  private def timedOut(op: KqueueOp): Unit = {
    val completion = withdraw(op)
    if completion != null then settleWithdrawn(op, completion, new TimeoutException(s"$op timed out"))
  }

  /** Takes a pending op out of the reactor, as `cancel` describes, and returns its completion: null if it is not
    * pending here.
    */
  private def withdraw(op: KqueueOp): Completion[KqueueOp] | Null =
    if awaiting.remove(op) then {
      // Waiting for its turn, or given it and not yet submitted again: out of the queue, or the turn passed on.
      val claim = op.claim
      if claim != null then claim.withdraw(op): Unit
      take(op)
    } else
      op match {
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
        case notified: Notified =>
          if notified.completion == null || waiting.getOrNull(notified.token) == null then null // finished, or not here
          else {
            waiting -= notified.token
            notified.withdraw() // a notification already queued for it is dropped when delivered
            take(notified)
          }
        case timer: KqueueTimer => if timer.heapIndex < 0 then null else take(timer)
        case blocking: KqueueBlocking if !blocking.cancelUnlessCommitted() => null // it wrote its result: finishing
        case promise: KqueuePromise                                        => detach(promise)
        case _: KqueueAtOnce => null // performed when submitted, so finished
      }

  /** Runs the completion of an op `withdraw` took out: `onCancel`, or `onFailure` with `failure` for one that timed
    * out; for a blocking task that was running, from the loop once `block` has returned.
    */
  private def settleWithdrawn(op: KqueueOp, completion: Completion[KqueueOp], failure: Throwable | Null): Unit =
    op match {
      case opens: Opens =>
        opens.abandon() // what it opened never reaches the caller
        if failure == null then deliverCancel(completion, op) else deliverFailure(completion, op, failure)
      case notified: Notified =>
        notified.abandon()
        if failure == null then deliverCancel(completion, op) else deliverFailure(completion, op, failure)
      case blocking: KqueueBlocking if blocking.interrupt() =>
        blocking.cancelling = completion
        blocking.cancelledBy = failure
        cancellingBlockers += blocking
      case _ => if failure == null then deliverCancel(completion, op) else deliverFailure(completion, op, failure)
    }

  /** Deletes a pending command's one-shot filter so it cannot wake the loop for nothing. If the caller already closed
    * the descriptor, kqueue removed the filter with it and reports `ENOENT` or `EBADF`; the op is cancelled all the
    * same, as it is if the deletion fails otherwise: the filter left over fires once, finding no command, and
    * `dispatch` drops its event.
    */
  private def deleteFilter(fd: Int, read: Boolean): Unit = KqueueLoop.deleteFileIfOpen(kq, fd, read): Unit

  /** Detaches a pending promise so its outcome is never delivered. The caller skips or interrupts a blocking task. */
  private def detach(promise: KqueuePromise): Completion[KqueueOp] | Null = {
    val completion = promise.completion
    promise.completion = null
    if promise.heapIndex >= 0 then timers.remove(promise) // its time limit ends with it
    pendingPromises -= promise
    completion
  }

  def stop(): Unit = running = false

  /** `fd`'s slot, brought up to date for a submission, a read if `reading`. What it knew no longer holds if the
    * descriptor's number was closed and reused since, on any thread: it then starts over, as for a new descriptor. Nor
    * does what it knew of reading hold if another reactor reads the descriptor now, having registered a read filter
    * since this one did; a read then starts over not ready, so that it registers a filter here before trying anything,
    * since kqueue reports data already waiting as soon as a filter is added, and trying a read first would only find
    * out what the filter will. Writes leave what it knew of reading alone. A filter of this reactor's that is still
    * registered is harmless: a new registration replaces it, and an event it reports first is dropped by `dispatch`.
    */
  private def currentSlot(fd: Int, reading: Boolean): Slot = {
    val slot = slotFor(fd)
    val entry = DescriptorTable.get(fd)
    val generation = DescriptorTable.generation(entry)
    if generation != slot.generation then {
      // Its number was closed, and reused, since: whatever is still pending belonged to the closed descriptor.
      failClosed(slot, fd)
      slot.generation = generation
    }
    slot.alwaysTry = DescriptorTable.alwaysTry(entry)
    val owner = DescriptorTable.readOwner(entry)
    if reading && owner != 0 && owner != id then {
      slot.forget()
      slot.settleReadiness(KqueueReactor.NotReady)
      takenOver += 1
    }
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
  private def dispatch(event: Event): Unit =
    if KqueueLoop.isUserEvent(event) then {
      // Resolved promises are delivered by `runOnce` itself, as are the extension's events (`deliverNotified`).
      if KqueueLoop.ident(event) == WakeEvent then wakePending.set(false)
    } else dispatchFile(fileIdent(event), isWriteEvent(event), KqueueLoop.isEOF(event), KqueueLoop.data(event).toLong)

  /** What `dispatch` does for a descriptor's filter firing, a write's if `write`, at the end of the stream if `eof`,
    * with `data` bytes available, -1 if not known; and for `FifoWatcher` reporting a descriptor readable
    * (`deliverSelected`).
    */
  private def dispatchFile(ident: Int, write: Boolean, eof: Boolean, data: Long): Unit = {
    val slot = if ident < slots.length then slots(ident) else null
    if slot != null then {
      val pending = if write then slot.write else slot.read
      if pending == null then {
        if !write then {
          val entry = DescriptorTable.get(ident)
          if DescriptorTable.readOwner(entry) != id || DescriptorTable.generation(entry) != slot.generation then {
            // Another reactor reads the descriptor now, or its number was reused: this filter is left over. Drop it,
            // so this reactor no longer wakes for it; it is registered again if a read here needs it.
            KqueueLoop.deleteFileIfOpen(kq, ident, read = true): Unit // if it stays, it is dropped again next time
            slot.forget()
            filtersDropped += 1
          } else { // kept for the next read submitted
            if eof then slot.readEof = true
            slot.readReadiness = KqueueReactor.Ready
            slot.readAvailable = data
          }
        }
      } else {
        if !write && eof then slot.readEof = true
        if write then slot.write = null else slot.read = null
        pending.nn match {
          case command: Command =>
            // A throwing command fails its own op; an exception thrown by the completion itself is reported as a
            // `CompletionFailed` (see `Reactor`), not as the op's failure. The system call, and a registration again
            // if it was not ready, are a use of the command's handle (see `Command.handle`), ended before the
            // completion runs, which may close the handle.
            val handle = command.handle
            var failure: Throwable | Null = null
            var done = true
            // Readiness is settled before the completion runs, since it may submit the next read at once.
            if handle != null && !handle.acquire() then {
              failure = KqueueHandles.notOpen(handle)
              if !write then settleAfterRead(command, slot, done = true, failed = true, -1L)
            } else
              try {
                done =
                  try command.perform()
                  catch {
                    case t: Throwable =>
                      failure = t
                      true
                  }
                if !write then settleAfterRead(command, slot, done, failure != null, data)
                if !done then {
                  // Not ready after all: wait again, without the completion ever seeing it, unless waiting fails.
                  val failed =
                    if write then {
                      slot.write = command
                      registrationFailure(command, slot, read = false)(arm(ident, read = false))
                    } else {
                      slot.read = command
                      registrationFailure(command, slot, read = true)(waitToRead(command, slot))
                    }
                  if failed != null then failure = failed
                }
              } finally if handle != null then handle.release()
            if failure != null then fail(command, failure.nn)
            else if done then complete(command)
          case _ => () // Only commands live in descriptor slots.
        }
      }
    }
  }

  /** An event with `EV_ERROR`, whose data is the error number: the command pending on the descriptor's filter fails,
    * and no other; the slot forgets what it knew of reading, so a later read registers its filter again. On the wake
    * event, which the reactor registers itself, there is nothing to fail; a wake-up is still taken as one.
    */
  private def dispatchError(event: Event): Unit =
    if KqueueLoop.isUserEvent(event) then { if KqueueLoop.ident(event) == WakeEvent then wakePending.set(false) }
    else if KqueueLoop.isReadEvent(event) || isWriteEvent(event) then {
      val ident = fileIdent(event)
      val slot = if ident >= 0 && ident < slots.length then slots(ident) else null
      if slot != null then {
        val write = isWriteEvent(event)
        val pending = if write then slot.write else slot.read
        if write then slot.write = null
        else {
          slot.read = null
          slot.forget()
        }
        if pending != null then {
          val code = KqueueLoop.errno(event)
          fail(
            pending,
            new IOException(s"kqueue reported an error on descriptor $ident: ${fromCString(string.strerror(code))}")
          )
        }
      }
    }

  // The buffer polls fill, allocated once and freed by `close`, rather than on the stack every turn.
  private val events = KqueueLoop.allocateQueue(maxEvents)

  /** Delivers commands that finished when submitted and resolved promises, polls once, dispatches what it polled, then
    * delivers promises resolved meanwhile. It blocks in the poll only if asked to and nothing was delivered first, and
    * skips the poll after delivering something, except every `KqueueReactor.PollEvery` turns. An error event fails the
    * command it is for (`dispatchError`); a failing poll, of the kqueue itself, throws.
    */
  def runOnce(block: Boolean): Unit = {
    val closingDue = closing.size
    deliverMoved()
    val delivered = // no short-circuiting
      deliverGiven() | deliverClosed() | deliverSelected() | deliverReady() | deliverResolved() | deliverNotified() |
        closingDue > 0
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
      if KqueueLoop.isError(event) then dispatchError(event) else dispatch(event)
      i += 1
    }
    if polled > 0 then deliverNotified(): Unit // what the extension's event, if among them, woke the poll for
    fireDueTimers()
    deliverClosed() // their failures are delivered next turn, which a wake-up makes immediate
    deliverResolved() // including any whose wake-up was merged into an event just dispatched
    deliverClosing(closingDue)
  }

  /** Polls without blocking up to `spinPolls` times, then blocks if nothing turned up. In request/response traffic the
    * awaited reply often arrives within microseconds, and catching it this way saves the thread a sleep and a wake-up.
    * A timer already due needs no spinning, since the turn has it to complete anyway; and the polls cost most on a
    * thread with little else to do, as an otherwise idle worker waiting on a timer is.
    */
  private def pollBeforeBlocking(events: KqueueLoop.EventQueue): Int = {
    if !timers.isEmpty && timers.peek.when <= System.nanoTime() then
      return KqueueLoop.pollEventsNow(kq, events, maxEvents)
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
      val wait = timers.peek.when - System.nanoTime()
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

  /** Completes the timers that are due, earliest first, and times out the ops whose time limit is. Only entries
    * scheduled before it started fire, so a completion that submits a zero-length timer leaves it for the next turn.
    */
  private def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.when <= now && timers.peek.order < submittedBefore do
        timers.poll() match {
          case timer: KqueueTimer =>
            timer.due(now) match {
              case gears.async.io.internal.TimerState.Complete => complete(timer)
              case gears.async.io.internal.TimerState.Expire   => fail(timer, new TimeoutException(s"$timer timed out"))
              case _                                           => file(timer) // its deadline moved later
            }
          case op => timedOut(op)
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

  /** The open reactors of the process, told of every descriptor closed (`KqueueHandles.closeDescriptor`). */
  private val all = java.util.concurrent.ConcurrentHashMap.newKeySet[KqueueReactor]()

  /** Tells every open reactor that `fd`, whose generation was `generation`, has been closed (see `closed`). */
  private[kqueue] def descriptorClosed(fd: Int, generation: Int): Unit = all.forEach(_.closed(fd, generation))

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
  def open(maxEvents: Int = 255, spinPolls: Int = DefaultSpinPolls): KqueueReactor =
    openFor(KqueueOps, maxEvents, spinPolls)

  /** Opens a reactor for a backend built on kqueue's reactors, whose ops are `ops` (see `Notified`). */
  private[io] def openFor(
      ops: Ops[KqueueOp, KqueueBackend.Handle],
      maxEvents: Int = 255,
      spinPolls: Int = DefaultSpinPolls
  ): KqueueReactor = {
    require(spinPolls >= 0, "spinPolls must not be negative")
    val reactor = new KqueueReactor(KqueueLoop.open(), maxEvents, spinPolls, ops)
    all.add(reactor)
    reactor
  }

  /** The user event an extension triggers on the reactor's kqueue to wake it (see `ReactorExtension`). */
  private[io] val ExtensionEvent: USize = 1.toUSize

  /** A queue of tokens that allocates nothing per token: a ring of longs, grown when full. */
  private final class Tokens {
    private var ring = new Array[Long](64)
    private var head = 0
    private var size = 0

    def isEmpty: Boolean = size == 0

    def add(token: Long): Unit = {
      if size == ring.length then {
        val grown = new Array[Long](ring.length * 2)
        var i = 0
        while i < size do {
          grown(i) = ring((head + i) % ring.length)
          i += 1
        }
        ring = grown
        head = 0
      }
      ring((head + size) % ring.length) = token
      size += 1
    }

    def poll(): Long = {
      val token = ring(head)
      head = (head + 1) % ring.length
      size -= 1
      token
    }

    def clear(): Unit = {
      head = 0
      size = 0
    }
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
