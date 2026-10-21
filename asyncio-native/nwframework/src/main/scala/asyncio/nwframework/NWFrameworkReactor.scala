package asyncio.nwframework

import java.io.IOException
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Completion
import asyncio.Reactor
import asyncio.Repeatable
import asyncio.Blockers
import asyncio.unsafe.TimerHeap

/** A `Reactor` over Apple's Network.framework, for ops built by `NWFrameworkOps`. Network.framework is completion-based:
  * a receive or send is issued and its outcome arrives later, on a Dispatch thread, as a callback. The C side
  * (`nwframework_io.c`) queues what the callbacks deliver as events on the handle, and wakes the reactor watching the
  * handle; this reactor's thread drains the handles with events each turn, and parks between turns, so that every
  * completion runs here, as `Reactor` promises. The handle (`NWFrameworkHandle`) applies the events and tells the op
  * waiting for them, which is attempted again; an op is attempted when submitted, so a read of bytes already
  * received, an accept of a connection already accepted, or a connect of a connection already ready finishes at
  * once.
  *
  * A receive once issued cannot be withdrawn, so cancelling a read leaves its receive outstanding, and what it brings
  * waits on the handle for the next read; a write copies its bytes when submitted, so cancelling it leaves the
  * caller's buffer alone, the bytes already with the stack. Timers are kept in a heap here, and bound how long the
  * thread parks. Blocking ops run on the shared blocker pool, which wakes the thread. Closing a handle, from any
  * thread, tells the reactors waiting on it, so the ops pending on it fail there (`Handles.close`).
  */
final class NWFrameworkReactor private (spinChecks: Int, maxDrained: Int) extends Reactor {

  /** Every NWFramework reactor runs the same kinds of op, so an op may go to any of them in turn (see `NWFrameworkBackend`). */
  type Op = NWFrameworkOp

  def ops: NWFrameworkOps.type = NWFrameworkOps

  def handles: NWFrameworkHandles.type = NWFrameworkHandles

  /** The C side's list of handles with events for this reactor, and the condition its thread parks on. */
  private[nwframework] val ptr: NWFrameworkIo.Reactor = NWFrameworkIo.nwf_reactor_create()

  // The handles a turn drains, and the event a handle copies out, allocated once and freed by `close`.
  private val drained: Ptr[NWFrameworkIo.Handle] =
    stdlib.malloc((sizeOf[Ptr[Byte]] * maxDrained).toCSize).asInstanceOf[Ptr[NWFrameworkIo.Handle]]
  private[nwframework] val event: Ptr[NWFrameworkIo.Event] =
    stdlib.malloc(sizeOf[NWFrameworkIo.Event].toCSize).asInstanceOf[Ptr[NWFrameworkIo.Event]]

  private def take(op: NWFrameworkOp): Completion[NWFrameworkOp] = {
    val completion = op.completion.nn
    op.completion = null
    completion
  }

  private def complete(op: NWFrameworkOp): Unit = take(op).onComplete(op)
  private def fail(op: NWFrameworkOp, failure: Throwable): Unit = take(op).onFailure(op, failure)

  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  // Commands waiting on a handle, by the token of their submission, which a handle's notification carries. Loop
  // thread only.
  private val waiting = scala.collection.mutable.LongMap.empty[NWFrameworkCommand]
  private var nextToken = 1L

  // Tokens of commands a handle told are worth attempting again: from this thread, in a turn, unboxed; and from any
  // other thread, boxed, which is rare: another reactor draining a handle an op here waits on.
  private val notifiedHere = new NWFrameworkReactor.Tokens
  private val notifiedElsewhere = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Long]()

  // Ops that finished as soon as they were submitted, whose completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[NWFrameworkOp]()

  // Blocking ops settle on the blocker pool's threads; they queue here and wake the loop.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[NWFrameworkBlocking]()
  private val pendingBlockers = scala.collection.mutable.HashSet.empty[NWFrameworkBlocking]
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[NWFrameworkBlocking]

  // Set by the first `wakeup` since the loop last drained, so later ones skip the C call.
  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  private val timers = new TimerHeap[NWFrameworkTimer]
  private var nextTimerOrder = 0L
  private var running = false

  @volatile private var loopThread: Thread = Thread.currentThread()
  @volatile private var closed = false

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = submitOrFinish(op, completion, finishNow = false): Unit

  def trySubmit[C <: Op](op: C, completion: Completion[C]): Boolean = submitOrFinish(op, completion, finishNow = true)

  private def submitOrFinish[C <: Op](op: C, completion: Completion[C], finishNow: Boolean): Boolean = {
    var finished = false
    op match {
      case _: Repeatable => ()
      case one: NWFrameworkOp =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[NWFrameworkOp]] // only ever called with `op`, which is a C
    op match {
      case command: NWFrameworkCommand =>
        command.token = nextToken
        nextToken += 1
        command.completion = pending
        // A command on a handle that is not open fails, as one on a closed descriptor does, through its completion.
        val open =
          try {
            command.target = NWFrameworkHandles.get(command.handle)
            true
          } catch {
            case e: IOException =>
              command.atOnceFailure = e
              false
          }
        if !open || attempted(command) then {
          if finishNow then finished = true else ready.add(command)
        } else waiting.put(command.token, command)
      case timer: NWFrameworkTimer =>
        require(timer.heapIndex < 0, "This timer is already pending")
        timer.deadline = System.nanoTime() + timer.milliseconds * 1_000_000L
        timer.order = nextTimerOrder
        nextTimerOrder += 1
        timer.completion = pending
        timers.add(timer)
      case blocking: NWFrameworkBlocking =>
        if blocking.cancelling != null then throw new IllegalStateException(s"$op is still running after being cancelled")
        blocking.reactor = this
        blocking.completion = pending
        pendingBlockers += blocking
        Blockers.run(blocking)
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

  /** Attempts `command`: true if it finished, or failed, with the failure kept for its completion. On submission the
    * handle takes in its events first, so that an op finishes at once when it can; attempted again after a
    * notification, the events were taken in by the drain that led to it.
    */
  private def attempted(command: NWFrameworkCommand, drainFirst: Boolean = true): Boolean =
    try command.attempt(this, drainFirst)
    catch {
      case t: Throwable =>
        command.atOnceFailure = t
        true
    }

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

  /** A handle tells the reactor that the command of `token`, waiting there, is worth attempting again. Any thread. */
  private[nwframework] def notified(token: Long): Unit =
    if Thread.currentThread() eq loopThread then notifiedHere.add(token)
    else {
      notifiedElsewhere.add(token)
      wakeup()
    }

  /** Drains the handles with events for this reactor, which apply them and tell the waiting commands, then attempts
    * those commands: one that finishes has its completion run; one that must still wait is registered again by its
    * attempt. A token no longer waiting, such as a cancelled command's, is dropped. Returns whether there were any.
    */
  private def deliverNotified(): Boolean = {
    var any = false
    var n = NWFrameworkIo.nwf_reactor_drain(ptr, drained, maxDrained)
    while n > 0 do {
      any = true
      var i = 0
      while i < n do {
        val handle = NWFrameworkHandles.at(drained(i))
        if handle != null then handle.drain(event)
        i += 1
      }
      n = if n == maxDrained then NWFrameworkIo.nwf_reactor_drain(ptr, drained, maxDrained) else 0
    }
    var token = notifiedElsewhere.poll()
    while token != null do {
      notifiedHere.add(token.longValue)
      token = notifiedElsewhere.poll()
    }
    while !notifiedHere.isEmpty do {
      any = true
      val t = notifiedHere.poll()
      val command = waiting.getOrNull(t) // no `Option` for each: a token no longer waiting is simply not found
      if command != null then {
        waiting -= t
        if attempted(command, drainFirst = false) then {
          val failure = command.atOnceFailure
          if failure != null then {
            command.atOnceFailure = null
            fail(command, failure)
          } else complete(command)
        } else waiting.put(t, command)
      }
    }
    any
  }

  /** Called from a blocker thread when a blocking op has settled: queue it and wake the loop. */
  private[nwframework] def resolved(blocking: NWFrameworkBlocking): Unit =
    if !closed then {
      resolvedQueue.add(blocking)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  def wakeup(): Unit =
    if wakePending.compareAndSet(false, true) then if !closed then NWFrameworkIo.nwf_reactor_wake(ptr)

  private def deliverResolved(): Boolean = {
    var remaining = resolvedQueue.size()
    var blocking = if remaining > 0 then resolvedQueue.poll() else null
    val any = blocking != null
    while blocking != null do {
      val completion = blocking.completion
      blocking.completion = null
      pendingBlockers -= blocking
      if blocking.cancelling != null then {
        assert(completion == null, "a blocker awaiting its cancel was resubmitted")
        val cancelled = blocking.cancelling.nn
        blocking.cancelling = null
        cancellingBlockers -= blocking
        cancelled.onCancel(blocking)
      } else if completion != null then {
        val failure = blocking.failure
        if failure != null then completion.onFailure(blocking, failure.nn) else completion.onComplete(blocking)
      }
      remaining -= 1
      blocking = if remaining > 0 then resolvedQueue.poll() else null
    }
    any
  }

  private def withdraw(command: NWFrameworkCommand): Unit = {
    command.target.nn.unwait(command.kind, command.token)
    command match {
      case write: NWFrameworkWrite => write.reset()
      case send: NWFrameworkSend   => send.reset()
      case _                  => ()
    }
  }

  def cancel(op: Op): Boolean = {
    val completion: Completion[NWFrameworkOp] | Null = op match {
      case command: NWFrameworkCommand =>
        if command.completion == null || waiting.getOrNull(command.token) == null then null // finished, or never here
        else {
          waiting -= command.token
          withdraw(command) // a notification already queued for it is dropped when drained
          take(command)
        }
      case timer: NWFrameworkTimer =>
        if timer.heapIndex < 0 then null
        else {
          timers.remove(timer)
          take(timer)
        }
      case blocking: NWFrameworkBlocking => detach(blocking)
    }
    if completion == null then false
    else {
      op match {
        case blocking: NWFrameworkBlocking if blocking.interrupt() =>
          blocking.cancelling = completion
          cancellingBlockers += blocking
        case _ => completion.onCancel(op)
      }
      true
    }
  }

  private def detach(blocking: NWFrameworkBlocking): Completion[NWFrameworkOp] | Null = {
    val completion = blocking.completion
    blocking.completion = null
    pendingBlockers -= blocking
    completion
  }

  def runOnce(block: Boolean): Unit = {
    wakePending.set(false) // a wake-up from now on must reach the park below
    val delivered = deliverReady() | deliverResolved() | deliverNotified()
    if !delivered && block then {
      parkUntilNotified()
      wakePending.set(false)
      deliverNotified()
    }
    fireDueTimers()
    deliverResolved()
  }

  /** Parks the thread until a handle has events, a wake-up, or the earliest timer is due, after checking for events
    * `spinChecks` times without parking.
    */
  private def parkUntilNotified(): Unit = {
    var spins = 0
    while spins < spinChecks do {
      if NWFrameworkIo.nwf_reactor_ready(ptr) != 0 || !resolvedQueue.isEmpty || !notifiedElsewhere.isEmpty then return
      Thread.onSpinWait()
      spins += 1
    }
    if timers.isEmpty then NWFrameworkIo.nwf_reactor_wait(ptr, -1L)
    else {
      val wait = timers.peek.deadline - System.nanoTime()
      if wait > 0 then NWFrameworkIo.nwf_reactor_wait(ptr, wait)
    }
  }

  private def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.deadline <= now && timers.peek.order < submittedBefore do
        complete(timers.poll())
    }

  def run(): Unit = {
    loopThread = Thread.currentThread()
    running = true
    while running do runOnce(block = true)
  }

  def stop(): Unit = running = false

  /** Cancels every pending op, waits for running blockers, and frees the C side. Handles stay open. */
  def close(): Unit =
    if !closed then {
      val dropped = scala.collection.mutable.ArrayBuffer.empty[(NWFrameworkOp, Completion[NWFrameworkOp])]
      def drop(op: NWFrameworkOp): Unit = dropped += ((op, take(op)))
      for (_, command) <- waiting.toList do {
        withdraw(command)
        drop(command)
      }
      waiting.clear()
      notifiedHere.clear()
      while !timers.isEmpty do drop(timers.poll())
      while !ready.isEmpty do drop(ready.poll().nn)
      val running = scala.collection.mutable.ArrayBuffer.empty[NWFrameworkBlocking]
      for blocking <- pendingBlockers.toList do {
        val completion = detach(blocking)
        if completion != null then {
          dropped += ((blocking, completion))
          if blocking.interrupt() then running += blocking
        }
      }
      for blocking <- cancellingBlockers do {
        dropped += ((blocking, blocking.cancelling.nn))
        blocking.cancelling = null
        running += blocking
      }
      cancellingBlockers.clear()
      closed = true
      resolvedQueue.clear()
      notifiedElsewhere.clear()
      running.foreach(_.awaitEnd())
      NWFrameworkIo.nwf_reactor_destroy(ptr)
      stdlib.free(drained.asInstanceOf[Ptr[Byte]])
      stdlib.free(event.asInstanceOf[Ptr[Byte]])
      dropped.foreach((op, completion) => completion.onCancel(op))
    }
}

object NWFrameworkReactor {

  /** Opens a reactor. Before parking for events it checks for them `spinChecks` times; each turn drains up to
    * `maxDrained` handles with events at a time. Close it when done.
    */
  def open(spinChecks: Int = DefaultSpinChecks, maxDrained: Int = 256): NWFrameworkReactor = {
    require(spinChecks >= 0 && maxDrained > 0)
    new NWFrameworkReactor(spinChecks, maxDrained)
  }

  /** How many times a reactor checks for events before parking, by default: `ASYNCIO_NWFRAMEWORK_SPIN` in the
    * environment, for measuring, else none.
    */
  val DefaultSpinChecks: Int = sys.env.get("ASYNCIO_NWFRAMEWORK_SPIN").map(_.toInt).getOrElse(0)

  /** A queue of tokens that allocates nothing per token: a ring of longs, grown when full. */
  private[nwframework] final class Tokens {
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
}
