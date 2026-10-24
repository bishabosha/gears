package gears.async.io
package nio

import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.concurrent.TimeoutException

import gears.async.io.Completion
import gears.async.io.Interest
import gears.async.io.Ops
import gears.async.io.Reactor
import gears.async.io.Repeatable
import gears.async.io.internal.TimerHeap

/** The NIO implementation of `Reactor`, for ops built by `NioOps`, over a `Selector`. A command is performed as soon as
  * it is submitted when it allows that; otherwise, or when it would block, it waits for readiness and is performed on
  * the loop thread. A selectable channel waits on its key in this reactor's selector, whose interest ops follow the
  * commands pending on it, brought up to date once per turn, before the select, so a read finishing and the next one
  * submitted from its completion change nothing. A regular file is always ready, so a command on it that is not done
  * is performed again next turn. Timers, and the ops with a time limit, are kept in a heap on the loop thread and
  * bound how long a select blocks; an op that times out is withdrawn as a cancelled one is, with `onFailure` instead
  * of `onCancel`. `wakeup`, and blocking tasks finishing, wake the selector.
  *
  * The selector's keys belong to the channels: closing a handle cancels its keys, so a command pending on it never
  * finishes, and is only cancelled, as with the kqueue backend.
  */
final class NioReactor private (selector: Selector) extends Reactor {

  type Op = NioOp
  type Handle = NioCapabilities.Handle

  def ops: Ops[Op, Handle] = NioOps


  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  /** What waits on one channel's key in this selector: a command for each direction, and the interest ops each wants. */
  private final class Waiting {
    var read: Command | Null = null
    var write: Command | Null = null
    var readOps = 0
    var writeOps = 0
    var dirty = false // queued in `changed`
  }

  /** Takes a pending op's completion off it, ending the submission, and its time limit with it: the completion may
    * then submit the op again.
    */
  private def take(op: NioOp): Completion[NioOp] = {
    val completion = op.completion.nn
    op.completion = null
    if op.heapIndex >= 0 then timers.remove(op)
    completion
  }

  private def complete(op: NioOp): Unit = take(op).onComplete(op)
  private def fail(op: NioOp, failure: Throwable): Unit = take(op).onFailure(op, failure)

  // Commands that finished as soon as they were submitted, whose completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[NioOp]()

  // Commands on regular files that were not done: a file is always ready, so they are performed again each turn.
  private val again = new java.util.ArrayDeque[Command]()

  // Keys whose interest ops must be brought up to date before the next select. Loop thread only.
  private val changed = new java.util.ArrayDeque[SelectionKey]()

  // The keys the last select found ready, collected so that commands run after it returns, not inside it.
  private var selected = new Array[SelectionKey](64)
  private var selectedCount = 0
  private val collect: java.util.function.Consumer[SelectionKey] = key => {
    if selectedCount == selected.length then selected = java.util.Arrays.copyOf(selected, selectedCount * 2)
    selected(selectedCount) = key
    selectedCount += 1
  }

  // Turns in a row that delivered completions without selecting (see `KqueueReactor`).
  private var turnsWithoutPoll = 0

  // Handles close on other threads; their entries queue here and wake the selector, so that the commands pending on
  // them fail (see `closed`). The waiting commands are found through the reactor's own map, not the selector's key:
  // the select the wake-up ends deregisters a closed channel's key before the loop gets here. Loop thread only.
  private val closedQueue = new java.util.concurrent.ConcurrentLinkedQueue[Selectable]()
  private val waitingByEntry = new java.util.IdentityHashMap[Selectable, Waiting]()

  // Promises resolve on other threads; they queue here and wake the selector.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[NioPromise]()

  // Set by the first `wakeup` since the loop last selected, so later ones skip waking the selector, which a select
  // returning clears; cleared after each select, before anything it was for is looked at.
  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  // Whether the loop is blocked in a select, or about to be, and whether a wake-up was asked for since the last select.
  // `wakeup` sets the second, then wakes the selector only if the first is set; the loop sets the first, then blocks
  // only if the second is not, so one of them always sees the other: a wake-up while the loop is awake, running
  // completions or a caller's tasks, costs no `Selector.wakeup`, which writes to the selector's pipe, and the read that
  // drains it.
  @volatile private var parked = false
  @volatile private var wakeRequested = false

  // Pending timers, and pending ops with a time limit, by deadline (see `fireDueTimers`).
  private val timers = new TimerHeap[NioOp]
  private var nextTimerOrder = 0L
  private var running = false

  // Promises submitted here whose completion has not run, and cancelled blockers whose `block` was still running.
  private val pendingPromises = scala.collection.mutable.HashSet.empty[NioPromise]
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[NioBlocking]

  // Guards waking the selector against `close`.
  private object wakeLock {}

  // The thread that runs the loop (see `KqueueReactor`).
  @volatile private var loopThread: Thread = Thread.currentThread()
  @volatile private var closed = false

  def submit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Unit =
    submitOrFinish(op, completion, finishNow = false, timeoutMillis): Unit

  def trySubmit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Boolean =
    submitOrFinish(op, completion, finishNow = true, timeoutMillis)

  /** Opens the socket of a `ConnectTo` as it is submitted, and returns true; or, if that fails, delivers the failure
    * as an at-once op's is, queued for the next turn or, with `finishNow`, thrown by `submitOrFinish`, and returns
    * false.
    */
  private def opened(connect: ConnectTo, pending: Completion[NioOp], finishNow: Boolean): Boolean =
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

  /** Submits `op`. A command tried at once that finishes queues its completion for the next turn, or, with `finishNow`,
    * takes the completion back off it uncalled, throws a failure, and returns true. An op left waiting, with a time
    * limit, goes into the timer heap to time out when it is due.
    */
  private def submitOrFinish[C <: Op](op: C, completion: Completion[C], finishNow: Boolean, timeoutMillis: Int): Boolean = {
    var finished = false
    var waits = false
    op match {
      case _: Repeatable => ()
      case one           =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[NioOp]] // only ever called with `op`, which is a C
    op match {
      case connect: ConnectTo if !opened(connect, pending, finishNow) => finished = finishNow
      case command: Command =>
        // A handle closed, or a channel this selector can no longer take, fails the op as a perform would.
        var failure: java.io.IOException | Null = null
        val entry =
          try NioHandles.entry(command.handle)
          catch {
            case e: java.io.IOException =>
              failure = e
              null
          }
        command.entry = entry
        val read = command.interest == Interest.Read
        val key = entry match {
          case s: Selectable =>
            try keyFor(command, s)
            catch {
              case e: java.io.IOException =>
                failure = e
                null
            }
          case _ => null
        }
        (entry, key) match {
          case _ if failure != null =>
            command.completion = pending
            command.atOnceFailure = failure
            if !finishNow then ready.add(command)
            finished = finishNow
          case (s: Selectable, key: SelectionKey) =>
            val waiting = key.attachment().asInstanceOf[Waiting]
            // Checked before trying, so that a new command cannot take data or buffer space ahead of a waiting one.
            if read then require(waiting.read == null, s"A read command is already pending on handle ${command.handle}")
            else require(waiting.write == null, s"A write command is already pending on handle ${command.handle}")
            command.completion = pending
            if performedAtOnce(command, queue = !finishNow) then finished = finishNow
            else {
              val ops = command.selectOps(s.channel)
              if read then {
                waiting.read = command
                waiting.readOps = ops
              } else {
                waiting.write = command
                waiting.writeOps = ops
              }
              markChanged(key, waiting)
              waits = true
            }
          case (_: RegularFile, _) =>
            command.completion = pending
            if performedAtOnce(command, queue = !finishNow) then finished = finishNow // always ready
            else {
              again.add(command)
              waits = true
            }
          case _ => throw new IllegalStateException(s"Unexpected handle entry $entry")
        }
      case timer: NioTimer =>
        require(timer.heapIndex < 0, "This timer is already pending")
        // A time limit shorter than the timer's own time is when it fires, failed instead of completed.
        timer.expires = timeoutMillis >= 0 && timeoutMillis < timer.milliseconds
        timer.completion = pending
        schedule(timer, if timer.expires then timeoutMillis else timer.milliseconds)
      case blocking: NioBlocking =>
        if blocking.cancelling != null then throw new IllegalStateException(s"$op is still running after being cancelled")
        blocking.reset() // a `Repeatable` promise's previous outcome
        blocking.reactor = this
        blocking.completion = pending
        pendingPromises += blocking
        blocking.restart()
        blocking.start()
        waits = true
      case atOnce: NioAtOnce =>
        atOnce.completion = pending
        try atOnce.perform()
        catch case t: Throwable => atOnce.atOnceFailure = t
        if finishNow then finished = true else ready.add(atOnce)
    }
    op.submitted = true
    if waits && timeoutMillis >= 0 then schedule(op, timeoutMillis)
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

  /** Puts `op` in the timer heap, due in `milliseconds`: a timer, to complete then, or an op with a time limit, to
    * time out then (see `fireDueTimers`).
    */
  private def schedule(op: NioOp, milliseconds: Int): Unit = {
    op.deadline = System.nanoTime() + milliseconds * 1_000_000L
    op.order = nextTimerOrder
    nextTimerOrder += 1
    timers.add(op)
  }

  /** `command`'s key in this selector for `s`'s channel, registering the channel with no interest ops the first time. */
  private def keyFor(command: Command, s: Selectable): SelectionKey = {
    val cached = command.key
    if cached != null && (cached.selector() eq selector) && (cached.channel() eq s.channel) && cached.isValid then
      cached
    else {
      var key = s.channel.keyFor(selector)
      if key == null then {
        val waiting = new Waiting
        key = s.channel.register(selector, 0, waiting)
        waitingByEntry.put(s, waiting)
        s.registered(this)
      }
      command.key = key
      key
    }
  }

  private def markChanged(key: SelectionKey, waiting: Waiting): Unit =
    if !waiting.dirty then {
      waiting.dirty = true
      changed.add(key)
    }

  /** Brings the interest ops of the keys whose commands changed up to date, before a select. */
  private def updateInterest(): Unit =
    while !changed.isEmpty do {
      val key = changed.poll()
      val waiting = key.attachment().asInstanceOf[Waiting]
      waiting.dirty = false
      val wanted = (if waiting.read != null then waiting.readOps else 0) |
        (if waiting.write != null then waiting.writeOps else 0)
      if key.isValid then
        try if key.interestOps() != wanted then key.interestOps(wanted): Unit
        catch case _: CancelledKeyException => () // its channel was closed meanwhile
    }

  /** Performs `command` before waiting for readiness. If it finishes, or fails, its completion is queued for the next
    * turn rather than run here, and this returns true; if it would block, this returns false.
    */
  private def performedAtOnce(command: Command, queue: Boolean): Boolean = {
    val done =
      try command.perform()
      catch {
        case t: Throwable =>
          command.atOnceFailure = t
          true
      }
    if done && queue then ready.add(command)
    done
  }

  /** Runs the completions of the commands that finished when submitted, as many as were queued when it started. */
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

  /** Performs a command whose handle is ready, and hands its completion the result; returns whether it is done. */
  private def performReady(command: Command): Boolean = {
    var failure: Throwable | Null = null
    val done =
      try command.perform()
      catch {
        case t: Throwable =>
          failure = t
          true
      }
    if failure != null then fail(command, failure)
    else if done then complete(command)
    done
  }

  /** Performs again the commands on regular files that were not done, as many as were waiting when it started. */
  private def deliverAgain(): Boolean = {
    var count = again.size
    val any = count > 0
    while count > 0 do {
      val command = again.poll()
      count -= 1
      if !performReady(command) then again.add(command)
    }
    any
  }

  /** Called from any thread when a promise has its result: queue it and wake the loop. */
  private[nio] def resolved(promise: NioPromise): Unit =
    if !closed then {
      resolvedQueue.add(promise)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  /** Called from any thread when a handle this reactor registered is closed: the commands pending on it fail, from
    * the loop, with `ClosedChannelException`, as `Ops.close` says.
    */
  private[nio] def closed(entry: Selectable): Unit =
    if !closed then {
      closedQueue.add(entry)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  /** Fails the commands pending on the handles closed since the last turn. Their keys, cancelled by the close, are
    * dropped by the next select.
    */
  private def deliverClosed(): Boolean = {
    var any = false
    var entry = closedQueue.poll()
    while entry != null do {
      val waiting = waitingByEntry.remove(entry)
      if waiting != null then {
        val read = waiting.read
        val write = waiting.write
        waiting.read = null
        waiting.write = null
        if read != null then {
          any = true
          fail(read, new java.nio.channels.ClosedChannelException())
        }
        if write != null then {
          any = true
          fail(write, new java.nio.channels.ClosedChannelException())
        }
      }
      entry = closedQueue.poll()
    }
    any
  }

  def wakeup(): Unit = {
    wakeRequested = true
    if parked && wakePending.compareAndSet(false, true) then
      wakeLock.synchronized {
        if !closed then selector.wakeup(): Unit
      }
  }

  /** Runs the completions of the promises resolved when it starts (see `KqueueReactor`). */
  private def deliverResolved(): Boolean = {
    var remaining = resolvedQueue.size()
    var promise = if remaining > 0 then resolvedQueue.poll() else null
    val any = promise != null
    while promise != null do {
      val completion = promise.completion
      promise.completion = null
      pendingPromises -= promise
      promise match {
        case blocking: NioBlocking if blocking.cancelling != null =>
          assert(completion == null, "a blocker awaiting its cancel was resubmitted")
          val cancelled = blocking.cancelling.nn
          val failure = blocking.cancelledBy
          blocking.cancelling = null
          blocking.cancelledBy = null
          cancellingBlockers -= blocking
          if failure == null then cancelled.onCancel(blocking) else cancelled.onFailure(blocking, failure)
        case _ =>
          if completion != null then {
            val failure = promise.failure
            if failure != null then completion.onFailure(promise, failure) else completion.onComplete(promise)
          }
      }
      remaining -= 1
      promise = if remaining > 0 then resolvedQueue.poll() else null
    }
    any
  }

  /** Cancels every pending op, interrupting blockers, then closes the selector. A blocker still running is waited for
    * before its `onCancel`.
    */
  def close(): Unit =
    if !closed then {
      val dropped = scala.collection.mutable.ArrayBuffer.empty[(NioOp, Completion[NioOp])]
      def drop(op: NioOp): Unit = dropped += ((op, take(op)))
      selector.keys().forEach { key =>
        val waiting = key.attachment().asInstanceOf[Waiting]
        if waiting.read != null then drop(waiting.read.nn)
        if waiting.write != null then drop(waiting.write.nn)
        waiting.read = null
        waiting.write = null
      }
      while !again.isEmpty do drop(again.poll())
      while !timers.isEmpty do drop(timers.poll())
      while !ready.isEmpty do drop(ready.poll().nn) // finished, but their completions have not run
      val running = scala.collection.mutable.ArrayBuffer.empty[NioBlocking]
      for promise <- pendingPromises.toList do {
        val completion = detach(promise)
        if completion != null then {
          dropped += ((promise, completion))
          promise match {
            case blocking: NioBlocking if blocking.interrupt() => running += blocking
            case _                                             => ()
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
      resolvedQueue.clear()
      closedQueue.clear()
      waitingByEntry.clear()
      wakeLock.synchronized {
        closed = true
        selector.close() // deregisters every channel; the channels stay open
      }
      running.foreach(_.awaitEnd())
      dropped.foreach { (op, completion) =>
        op match {
          case opens: Opens => opens.abandon() // what it opened never reaches the caller
          case _            => ()
        }
        completion.onCancel(op)
      }
    }

  /** As `KqueueReactor.cancel`: a command that finished when submitted counts as finished; anything else pending is
    * removed, and its `onCancel` runs before this returns, except for a blocking task that was running, whose
    * `onCancel` runs from the loop once `block` has returned.
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
  private def timedOut(op: NioOp): Unit = {
    val completion = withdraw(op)
    if completion != null then settleWithdrawn(op, completion, new TimeoutException(s"$op timed out"))
  }

  /** Takes a pending op out of the reactor, as `cancel` describes, and returns its completion: null if it is not
    * pending here.
    */
  private def withdraw(op: NioOp): Completion[NioOp] | Null =
    op match {
      case command: Command =>
        if command.completion == null then null
        else
          command.entry match {
            case _: Selectable =>
              val key = command.key
              if key == null || !(key.selector() eq selector) then null
              else {
                val waiting = key.attachment().asInstanceOf[Waiting]
                val pending = (waiting.read eq command) || (waiting.write eq command)
                if !pending then null // finished when submitted, or not submitted here
                else {
                  if waiting.read eq command then waiting.read = null else waiting.write = null
                  markChanged(key, waiting)
                  take(command)
                }
              }
            case _: RegularFile => if again.remove(command) then take(command) else null
            case _              => null // never submitted
          }
      case timer: NioTimer     => if timer.heapIndex < 0 then null else take(timer)
      case promise: NioPromise => detach(promise)
      case _: NioAtOnce        => null // performed when submitted, so finished
    }

  /** Runs the completion of an op `withdraw` took out: `onCancel`, or `onFailure` with `failure` for one that timed
    * out; for a blocking task that was running, from the loop once `block` has returned.
    */
  private def settleWithdrawn(op: NioOp, completion: Completion[NioOp], failure: Throwable | Null): Unit =
    op match {
      case opens: Opens =>
        opens.abandon() // what it opened never reaches the caller
        if failure == null then completion.onCancel(op) else completion.onFailure(op, failure)
      case blocking: NioBlocking if blocking.interrupt() =>
        blocking.cancelling = completion
        blocking.cancelledBy = failure
        cancellingBlockers += blocking
      case _ => if failure == null then completion.onCancel(op) else completion.onFailure(op, failure)
    }

  private def detach(promise: NioPromise): Completion[NioOp] | Null = {
    val completion = promise.completion
    promise.completion = null
    pendingPromises -= promise
    completion
  }

  def stop(): Unit = running = false

  /** Performs the commands a ready key is waiting for. Each is taken off the key first, so its completion may submit
    * the next; one not ready after all waits again, without its completion ever seeing it.
    */
  private def dispatch(key: SelectionKey): Unit = {
    val waiting = key.attachment().asInstanceOf[Waiting]
    val readyOps =
      try key.readyOps()
      catch case _: CancelledKeyException => 0
    val read = waiting.read
    if read != null && (readyOps & waiting.readOps) != 0 then {
      waiting.read = null
      if !performReady(read) then waiting.read = read
    }
    val write = waiting.write
    if write != null && (readyOps & waiting.writeOps) != 0 then {
      waiting.write = null
      if !performReady(write) then waiting.write = write
    }
    markChanged(key, waiting)
  }

  /** Delivers commands that finished when submitted, resolved promises and regular files, selects once,
    * performs what it found ready, fires due timers, then delivers promises resolved meanwhile. It blocks in the select
    * only if asked to and nothing was delivered first, and skips the select after delivering something, except every
    * `NioReactor.PollEvery` turns.
    */
  def runOnce(block: Boolean): Unit = {
    val delivered = deliverReady() | deliverResolved() | deliverAgain() | deliverClosed()
    updateInterest()
    val select =
      if !delivered then {
        turnsWithoutPoll = 0
        true
      } else if turnsWithoutPoll < NioReactor.PollEvery then {
        turnsWithoutPoll += 1
        false
      } else {
        turnsWithoutPoll = 0
        true
      }
    if select then {
      selectedCount = 0
      val waitFor = !delivered && block && again.isEmpty && ready.isEmpty
      if !waitFor then selector.selectNow(collect)
      else {
        parked = true
        try
          if wakeRequested || !resolvedQueue.isEmpty || !closedQueue.isEmpty then selector.selectNow(collect)
          else if timers.isEmpty then selector.select(collect)
          else {
            // Block no longer than until the earliest timer is due, rounding up, so it is due when the select returns.
            val wait = timers.peek.deadline - System.nanoTime()
            if wait <= 0 then selector.selectNow(collect)
            else selector.select(collect, (wait + 999_999L) / 1_000_000L)
          }
        finally parked = false
      }
      // Before anything a wake-up was for is looked at: the caller's tasks after this turn, and promises below.
      wakeRequested = false
      wakePending.set(false)
      var i = 0
      while i < selectedCount do {
        dispatch(selected(i))
        selected(i) = null
        i += 1
      }
    }
    fireDueTimers()
    deliverResolved()
  }

  /** Completes the timers that are due, earliest first, and times out the ops whose time limit is; only entries
    * scheduled before it started.
    */
  private def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.deadline <= now && timers.peek.order < submittedBefore do
        timers.poll() match {
          case timer: NioTimer => if timer.expires then fail(timer, new TimeoutException(s"$timer timed out")) else complete(timer)
          case op              => timedOut(op)
        }
    }

  def run(): Unit = {
    loopThread = Thread.currentThread()
    running = true
    while running do runOnce(block = true)
  }
}

object NioReactor {

  /** A loop that keeps delivering completions selects at least once in this many turns. */
  private inline val PollEvery = 16

  /** Opens a reactor on a new selector. Close it when done. */
  def open(): NioReactor = new NioReactor(Selector.open())
}
