package asyncio.nio

import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector

import asyncio.Completion
import asyncio.Handles
import asyncio.Interest
import asyncio.Ops
import asyncio.Reactor
import asyncio.Repeatable

/** The NIO implementation of `Reactor`, for ops built by `NioOps`, over a `Selector`. A command is performed as soon as
  * it is submitted when it allows that; otherwise, or when it would block, it waits for readiness and is performed on
  * the loop thread. A selectable channel waits on its key in this reactor's selector, whose interest ops follow the
  * commands pending on it, brought up to date once per turn, before the select, so a read finishing and the next one
  * submitted from its completion change nothing. A regular file is always ready, so a command on it that is not done
  * is performed again next turn. Timers are kept in a heap on the loop thread and bound how long a select blocks;
  * `wakeup`, and blocking tasks finishing, wake the selector.
  *
  * The selector's keys belong to the channels: closing a handle cancels its keys, so a command pending on it never
  * finishes, and is only cancelled, as with the kqueue backend.
  */
final class NioReactor private (selector: Selector) extends Reactor {

  type Op = NioOp

  def ops: Ops[Op] = NioOps

  def handles: Handles = NioHandles

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

  /** Takes a pending op's completion off it, ending the submission: the completion may then submit the op again. */
  private def take(op: NioOp): Completion[NioOp] = {
    val completion = op.completion.nn
    op.completion = null
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

  private val timers = new TimerHeap
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

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = submitOrFinish(op, completion, finishNow = false): Unit

  def trySubmit[C <: Op](op: C, completion: Completion[C]): Boolean = submitOrFinish(op, completion, finishNow = true)

  /** Submits `op`. A command tried at once that finishes queues its completion for the next turn, or, with `finishNow`,
    * takes the completion back off it uncalled, throws a failure, and returns true.
    */
  private def submitOrFinish[C <: Op](op: C, completion: Completion[C], finishNow: Boolean): Boolean = {
    var finished = false
    op match {
      case _: Repeatable => ()
      case one           =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[NioOp]] // only ever called with `op`, which is a C
    op match {
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
            val tryIt = command.tryFirst && !command.triedNow
            command.triedNow = false
            if tryIt && performedAtOnce(command, queue = !finishNow) then finished = finishNow
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
            }
          case (_: RegularFile, _) =>
            command.completion = pending
            if performedAtOnce(command, queue = !finishNow) then finished = finishNow // always ready
            else again.add(command)
          case _ => throw new IllegalStateException(s"Unexpected handle entry $entry")
        }
      case timer: NioTimer =>
        require(timer.heapIndex < 0, "This timer is already pending")
        timer.deadline = System.nanoTime() + timer.milliseconds * 1_000_000L
        timer.order = nextTimerOrder
        nextTimerOrder += 1
        timer.completion = pending
        timers.add(timer)
      case blocking: NioBlocking =>
        if blocking.cancelling != null then throw new IllegalStateException(s"$op is still running after being cancelled")
        blocking.reactor = this
        blocking.completion = pending
        pendingPromises += blocking
        blocking.start()
      case atOnce: NioAtOnce =>
        atOnce.completion = pending
        try atOnce.perform()
        catch case t: Throwable => atOnce.atOnceFailure = t
        if finishNow then finished = true else ready.add(atOnce)
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
    * the loop, with `ClosedChannelException`, as `Handles.close` says.
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
          blocking.cancelling = null
          cancellingBlockers -= blocking
          cancelled.onCancel(blocking)
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
      dropped.foreach((op, completion) => completion.onCancel(op))
    }

  /** As `KqueueReactor.cancel`: a command that finished when submitted counts as finished; anything else pending is
    * removed, and its `onCancel` runs before this returns, except for a blocking task that was running, whose
    * `onCancel` runs from the loop once `block` has returned.
    */
  def cancel(op: Op): Boolean = {
    val completion: Completion[NioOp] | Null = op match {
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
      case timer: NioTimer =>
        if timer.heapIndex < 0 then null
        else {
          timers.remove(timer)
          take(timer)
        }
      case promise: NioPromise => detach(promise)
      case _: NioAtOnce        => null // performed when submitted, so finished
    }
    if completion == null then false
    else {
      op match {
        case blocking: NioBlocking if blocking.interrupt() =>
          blocking.cancelling = completion
          cancellingBlockers += blocking
        case _ => completion.onCancel(op)
      }
      true
    }
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

  /** Completes the timers that are due, earliest first; only those submitted before it started. */
  private def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.deadline <= now && timers.peek.order < submittedBefore do
        complete(timers.poll())
    }

  /** Pending timers in a binary min-heap ordered by deadline, then submission order (as `KqueueReactor`'s). */
  private final class TimerHeap {
    private var heap = new Array[NioTimer | Null](16)
    private var size = 0

    def isEmpty: Boolean = size == 0
    def peek: NioTimer = heap(0).nn

    def add(timer: NioTimer): Unit = {
      if size == heap.length then heap = java.util.Arrays.copyOf(heap, size * 2)
      place(timer, size)
      size += 1
      siftUp(size - 1)
    }

    def poll(): NioTimer = {
      val timer = peek
      remove(timer)
      timer
    }

    def remove(timer: NioTimer): Unit = {
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

    private def before(a: NioTimer, b: NioTimer): Boolean =
      a.deadline < b.deadline || (a.deadline == b.deadline && a.order < b.order)

    private def place(timer: NioTimer, index: Int): Unit = {
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

object NioReactor {

  /** A loop that keeps delivering completions selects at least once in this many turns. */
  private inline val PollEvery = 16

  /** Opens a reactor on a new selector. Close it when done. */
  def open(): NioReactor = new NioReactor(Selector.open())
}
