package gears.async.io
package internal

import java.util.concurrent.TimeoutException

/** What a loop backend's reactor does the same whatever it waits on, kqueue's and NIO's alike, for ops of type `O`
  * (`LoopOp`): the timer heap, with the timers and the ops' time limits in it, and the thread that runs the loop. The
  * backend says how it waits for readiness and polls.
  */
abstract class ReactorCore[O <: LoopOp[O]] extends Reactor with TimerWaiter with LoopPromise.Owner[O] {

  /** Every reactor of the backend runs the same kinds of op, so backend-specific code may build them directly, and an
    * op may go to any of its reactors in turn.
    */
  type Op = O

  /** A promise of this reactor's: one of the backend's ops that is a `LoopPromise`. */
  protected type Promise = O & LoopPromise[O]

  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  // Pending timers, and pending ops with a time limit, by deadline (see `fireDueTimers`).
  protected final val timers = new TimerHeap[O]
  private var nextTimerOrder = 0L
  private var running = false

  // The thread that runs the loop: the one that opens the reactor, which owns it, or the one that calls `run`. A promise
  // resolved on it needs no wake-up, because the loop delivers every resolved promise before it polls again. Not
  // refreshed by every `runOnce`: on macOS `Thread.currentThread` is a call into thread-local storage.
  @volatile protected final var loopThread: Thread = Thread.currentThread()
  @volatile protected final var closed = false

  /** Lets go of `op`'s claim on its handle (see `claim`), as its completion is taken or it finishes in `trySubmit`. */
  protected final def release(op: O): Unit = {
    val claim = op.claim
    if claim != null then claim.release(op)
  }

  /** Takes a pending op's completion off it, ending the submission, and its time limit with it: the completion may then
    * submit the op again.
    */
  protected final def take(op: O): Completion[O] = {
    val completion = op.completion.nn
    op.completion = null
    release(op)
    if op.heapIndex >= 0 then timers.remove(op)
    op match {
      case timer: LoopTimer[?] => timer.waiter = null // a move no longer concerns this reactor
      case _                   => ()
    }
    completion
  }

  protected final def complete(op: O): Unit = deliverComplete(take(op), op)
  protected final def fail(op: O, failure: Throwable): Unit = deliverFailure(take(op), op, failure)

  /** Runs the completion of `op`, which finished: `onFailure` with the failure it kept (`atOnceFailure`), if any, else
    * `onComplete`.
    */
  protected final def finish(op: O): Unit = {
    val failure = op.atOnceFailure
    if failure != null then {
      op.atOnceFailure = null
      fail(op, failure)
    } else complete(op)
  }

  // Commands that finished as soon as they were submitted, and other ops that finished without waiting, whose
  // completions run on the next turn. Loop thread only.
  protected final val ready = new java.util.ArrayDeque[O]()

  /** Runs the completions of the ops that finished when submitted, as many as were queued when it started: a completion
    * that submits a command which finishes at once is left for the next turn, so one busy handle cannot keep the loop
    * from polling. Returns whether there were any.
    */
  protected final def deliverReady(): Boolean = {
    var count = ready.size
    val any = count > 0
    while count > 0 do {
      val op = ready.poll().nn
      count -= 1
      finish(op)
    }
    any
  }

  // Close ops that closed their handle at once, whose completions run at the end of the turn after the one they were
  // queued in: by then that reactor has delivered the failures of the ops they ended there, so that a close completes
  // after them (`Ops.close`). Loop thread only.
  protected final val closing = new java.util.ArrayDeque[O]()

  /** Runs the completions of the first `due` close ops queued, those queued before this turn began. */
  protected final def deliverClosing(due: Int): Unit = {
    var n = due
    while n > 0 do {
      val op = closing.poll().nn
      n -= 1
      finish(op)
    }
  }

  // Timers whose deadline moved earlier, told from another thread, for the loop to file again (`moved`).
  private val movedQueue = new java.util.concurrent.ConcurrentLinkedQueue[O & LoopTimer[O]]()

  /** A pending timer's deadline moved earlier than it is filed under: filed again here, or by the loop, woken. */
  def moved(timer: TimerState): Unit = timer match {
    case t: LoopTimer[?] =>
      val own = t.asInstanceOf[O & LoopTimer[O]] // only this reactor's timers name it their waiter
      if Thread.currentThread() eq loopThread then refile(own)
      else {
        movedQueue.add(own)
        wakeup()
      }
    case _ => ()
  }

  /** Files `timer` again under its key, if it is still pending here. Loop thread only. */
  private def refile(timer: O & LoopTimer[O]): Unit =
    if timer.heapIndex >= 0 && timer.completion != null then {
      timers.remove(timer)
      file(timer)
    }

  /** Files the timers moved earlier from other threads since the last turn. */
  protected final def deliverMoved(): Unit = {
    var timer = movedQueue.poll()
    while timer != null do {
      refile(timer)
      timer = movedQueue.poll()
    }
  }

  /** Puts `timer` in the heap under its key (see `TimerState`). */
  protected final def file(timer: O & LoopTimer[O]): Unit = {
    val key = timer.key
    timer.when = key
    timer.filed = key
    timer.order = nextTimerOrder
    nextTimerOrder += 1
    timers.add(timer)
  }

  /** Puts `op` in the timer heap, due in `milliseconds`: a timer, to complete then, or an op with a time limit, to time
    * out then (see `fireDueTimers`).
    */
  protected final def schedule(op: O, milliseconds: Int): Unit = {
    op.when = System.nanoTime() + milliseconds * 1_000_000L
    op.order = nextTimerOrder
    nextTimerOrder += 1
    timers.add(op)
  }

  // Promises resolve on other threads; they queue here and wake the loop.
  protected final val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[Promise]()

  // Promises submitted here whose completion has not run, so `close` can cancel them. Loop thread only.
  protected final val pendingPromises = scala.collection.mutable.HashSet.empty[Promise]

  // Cancelled blockers whose `block` was still running, so their `onCancel` waits for it to return. Loop thread only.
  protected final val cancellingBlockers = scala.collection.mutable.HashSet.empty[O & LoopBlocking[O]]

  /** Called from any thread when a promise has its result: queue it and wake the loop. On the loop thread itself the
    * loop is not polling, so queueing is enough.
    */
  private[io] final def resolved(promise: Promise): Unit =
    if !closed then { // otherwise its completion was dropped by `close`
      resolvedQueue.add(promise)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  /** Runs the completions of the promises resolved when it starts, on the loop thread. One resolved meanwhile, such as
    * by a completion, waits for the next turn, so completions that keep resolving promises cannot keep the loop from
    * polling. Returns whether there were any.
    */
  protected final def deliverResolved(): Boolean = {
    var remaining = resolvedQueue.size() // traverses the queue, which stays short
    var next = if remaining > 0 then resolvedQueue.poll() else null
    val any = next != null
    while next != null do {
      val promise = next.nn // `O` is abstract, so `next` is not narrowed
      val completion = promise.completion
      promise.completion = null
      if promise.heapIndex >= 0 then timers.remove(promise) // its time limit ends with it
      pendingPromises -= promise
      promise match {
        case blocking: LoopBlocking[O] @unchecked if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
          assert(completion == null, "a blocker queued its cancel was resubmitted")
          val cancelled = blocking.cancelling.nn
          val failure = blocking.cancelledBy
          blocking.cancelling = null
          blocking.cancelledBy = null
          cancellingBlockers -= blocking
          if failure == null then deliverCancel(cancelled, blocking) else deliverFailure(cancelled, blocking, failure)
        case _ =>
          if completion != null then {
            val failure = promise.failure
            if failure != null then deliverFailure(completion, promise, failure)
            else deliverComplete(completion, promise)
          }
      }
      remaining -= 1
      next = if remaining > 0 then resolvedQueue.poll() else null
    }
    any
  }

  /** Detaches a pending promise so its outcome is never delivered. The caller skips or interrupts a blocking task. */
  protected final def detach(promise: Promise): Completion[O] | Null = {
    val completion = promise.completion
    promise.completion = null
    if promise.heapIndex >= 0 then timers.remove(promise) // its time limit ends with it
    pendingPromises -= promise
    completion
  }

  /** Fails `op`, whose time limit ran out, with `TimeoutException`: withdrawn as a cancelled op is, then `onFailure`
    * where that would be `onCancel`. Nothing if it is no longer pending.
    */
  protected def timedOut(op: O): Unit

  /** Completes the timers that are due, earliest first, and times out the ops whose time limit is. Only entries
    * scheduled before it started fire, so a completion that submits a zero-length timer leaves it for the next turn.
    */
  protected final def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.when <= now && timers.peek.order < submittedBefore do
        timers.poll() match {
          case t: LoopTimer[?] =>
            val timer = t.asInstanceOf[O & LoopTimer[O]]
            timer.due(now) match {
              case TimerState.Complete => complete(timer)
              case TimerState.Expire   => fail(timer, new TimeoutException(s"$timer timed out"))
              case _                   => file(timer) // its deadline moved later
            }
          case op => timedOut(op)
        }
    }

  def stop(): Unit = running = false

  def run(): Unit = {
    loopThread = Thread.currentThread()
    running = true
    while running do runOnce(block = true)
  }
}
