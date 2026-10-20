package asyncio.darwin

import java.io.IOException
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Completion
import asyncio.Reactor
import asyncio.Repeatable
import asyncio.unsafe.Blockers
import asyncio.unsafe.TimerHeap

/** A `Reactor` over Apple's Network.framework, for ops built by `DarwinOps`. Network.framework is completion-based:
  * a receive or send is issued and its outcome arrives later, on a Dispatch thread, as a callback. The C side
  * (`darwin_io.c`) keeps what the callbacks deliver on the handle, and posts a notification to the queue of the
  * reactor whose op waits for it; this reactor's thread drains the queue each turn, and parks on it between turns,
  * so that every completion runs here, as `Reactor` promises. An op is attempted when submitted, so a read of bytes
  * already received, an accept of a connection already accepted, or a connect of a connection already ready finishes
  * at once; otherwise it registers a waiter on the handle's C side and is attempted again when notified.
  *
  * A receive once issued cannot be withdrawn, so cancelling a read leaves its receive outstanding, and what it brings
  * waits on the handle for the next read; a write copies its bytes when submitted, so cancelling it leaves the
  * caller's buffer alone, the bytes already in the stack. Timers are kept in a heap here, and bound how long the
  * thread parks. Blocking ops run on the shared blocker pool, which wakes the thread. Closing a handle, from any
  * thread, notifies the reactors waiting on it, so the ops pending on it fail there (`Handles.close`).
  */
final class DarwinReactor private (spinChecks: Int, maxDrained: Int) extends Reactor {

  /** Every Darwin reactor runs the same kinds of op, so an op may go to any of them in turn (see `DarwinBackend`). */
  type Op = DarwinOp

  def ops: DarwinOps.type = DarwinOps

  def handles: DarwinHandles.type = DarwinHandles

  /** The C side's queue of notifications for this reactor, and the condition its thread parks on. */
  private[darwin] val ptr: DarwinIo.Reactor = DarwinIo.dn_reactor_create()

  // The notifications a turn drains, allocated once and freed by `close`.
  private val drained: Ptr[DarwinIo.Done] = stdlib.malloc((sizeOf[DarwinIo.Done] * maxDrained).toCSize).asInstanceOf[Ptr[DarwinIo.Done]]

  private def take(op: DarwinOp): Completion[DarwinOp] = {
    val completion = op.completion.nn
    op.completion = null
    completion
  }

  private def complete(op: DarwinOp): Unit = take(op).onComplete(op)
  private def fail(op: DarwinOp, failure: Throwable): Unit = take(op).onFailure(op, failure)

  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  // Commands waiting on a handle, by the token of their submission, which the notification carries. Loop thread only.
  private val waiting = scala.collection.mutable.LongMap.empty[DarwinCommand]
  private var nextToken = 1L

  // Ops that finished as soon as they were submitted, whose completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[DarwinOp]()

  // Blocking ops settle on the blocker pool's threads; they queue here and wake the loop.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[DarwinBlocking]()
  private val pendingBlockers = scala.collection.mutable.HashSet.empty[DarwinBlocking]
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[DarwinBlocking]

  // Set by the first `wakeup` since the loop last drained, so later ones skip the C call.
  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  private val timers = new TimerHeap[DarwinTimer]
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
      case one: DarwinOp =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[DarwinOp]] // only ever called with `op`, which is a C
    op match {
      case command: DarwinCommand =>
        command.token = nextToken
        nextToken += 1
        command.completion = pending
        // A command on a handle that is not open fails, as one on a closed descriptor does, through its completion.
        val open =
          try {
            command.target = DarwinHandles.get(command.handle)
            true
          } catch {
            case e: IOException =>
              command.atOnceFailure = e
              false
          }
        if !open || attempted(command) then {
          if finishNow then finished = true else ready.add(command)
        } else waiting.put(command.token, command)
      case timer: DarwinTimer =>
        require(timer.heapIndex < 0, "This timer is already pending")
        timer.deadline = System.nanoTime() + timer.milliseconds * 1_000_000L
        timer.order = nextTimerOrder
        nextTimerOrder += 1
        timer.completion = pending
        timers.add(timer)
      case blocking: DarwinBlocking =>
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

  /** Attempts `command`: true if it finished, or failed, with the failure kept for its completion. */
  private def attempted(command: DarwinCommand): Boolean =
    try command.attempt(this)
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

  /** Drains the C side's notifications and attempts the commands they are for: one that finishes has its completion
    * run; one that must still wait is registered again by its attempt. A notification for a token no longer waiting,
    * such as a cancelled command's, is dropped. Returns whether there were any.
    */
  private def deliverNotified(): Boolean = {
    var any = false
    var n = DarwinIo.dn_reactor_drain(ptr, drained, maxDrained)
    while n > 0 do {
      any = true
      var i = 0
      while i < n do {
        val token = (drained + i)._1
        val command = waiting.remove(token)
        command match {
          case Some(command) =>
            if attempted(command) then {
              val failure = command.atOnceFailure
              if failure != null then {
                command.atOnceFailure = null
                fail(command, failure)
              } else complete(command)
            } else waiting.put(token, command)
          case None => ()
        }
        i += 1
      }
      n = if n == maxDrained then DarwinIo.dn_reactor_drain(ptr, drained, maxDrained) else 0
    }
    any
  }

  /** Called from a blocker thread when a blocking op has settled: queue it and wake the loop. */
  private[darwin] def resolved(blocking: DarwinBlocking): Unit =
    if !closed then {
      resolvedQueue.add(blocking)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  def wakeup(): Unit =
    if wakePending.compareAndSet(false, true) then if !closed then DarwinIo.dn_reactor_wake(ptr)

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

  def cancel(op: Op): Boolean = {
    val completion: Completion[DarwinOp] | Null = op match {
      case command: DarwinCommand =>
        if command.completion == null || !waiting.remove(command.token).isDefined then null // finished, or never here
        else {
          // The waiter goes if it has not been notified yet; a notification already posted is dropped when drained.
          DarwinIo.dn_cancel_wait(command.target.nn.ptr, command.kind, command.token)
          command match {
            case write: DarwinWrite => write.reset()
            case send: DarwinSend   => send.reset()
            case _                  => ()
          }
          take(command)
        }
      case timer: DarwinTimer =>
        if timer.heapIndex < 0 then null
        else {
          timers.remove(timer)
          take(timer)
        }
      case blocking: DarwinBlocking => detach(blocking)
    }
    if completion == null then false
    else {
      op match {
        case blocking: DarwinBlocking if blocking.interrupt() =>
          blocking.cancelling = completion
          cancellingBlockers += blocking
        case _ => completion.onCancel(op)
      }
      true
    }
  }

  private def detach(blocking: DarwinBlocking): Completion[DarwinOp] | Null = {
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

  /** Parks the thread until a notification, a wake-up, or the earliest timer is due, after checking for a notification
    * `spinChecks` times without parking.
    */
  private def parkUntilNotified(): Unit = {
    var spins = 0
    while spins < spinChecks do {
      if DarwinIo.dn_reactor_ready(ptr) != 0 || !resolvedQueue.isEmpty then return
      Thread.onSpinWait()
      spins += 1
    }
    if timers.isEmpty then DarwinIo.dn_reactor_wait(ptr, -1L)
    else {
      val wait = timers.peek.deadline - System.nanoTime()
      if wait > 0 then DarwinIo.dn_reactor_wait(ptr, wait)
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
      val dropped = scala.collection.mutable.ArrayBuffer.empty[(DarwinOp, Completion[DarwinOp])]
      def drop(op: DarwinOp): Unit = dropped += ((op, take(op)))
      for (token, command) <- waiting.toList do {
        DarwinIo.dn_cancel_wait(command.target.nn.ptr, command.kind, token)
        command match {
          case write: DarwinWrite => write.reset()
          case send: DarwinSend   => send.reset()
          case _                  => ()
        }
        drop(command)
      }
      waiting.clear()
      while !timers.isEmpty do drop(timers.poll())
      while !ready.isEmpty do drop(ready.poll().nn)
      val running = scala.collection.mutable.ArrayBuffer.empty[DarwinBlocking]
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
      running.foreach(_.awaitEnd())
      DarwinIo.dn_reactor_destroy(ptr)
      stdlib.free(drained.asInstanceOf[Ptr[Byte]])
      dropped.foreach((op, completion) => completion.onCancel(op))
    }
}

object DarwinReactor {

  /** Opens a reactor. Before parking for a notification it checks for one `spinChecks` times; each turn drains up to
    * `maxDrained` notifications at a time. Close it when done.
    */
  def open(spinChecks: Int = DefaultSpinChecks, maxDrained: Int = 256): DarwinReactor = {
    require(spinChecks >= 0 && maxDrained > 0)
    new DarwinReactor(spinChecks, maxDrained)
  }

  /** How many times a reactor checks for a notification before parking, by default: `ASYNCIO_DARWIN_SPIN` in the
    * environment, for measuring, else none.
    */
  val DefaultSpinChecks: Int = sys.env.get("ASYNCIO_DARWIN_SPIN").map(_.toInt).getOrElse(0)
}
