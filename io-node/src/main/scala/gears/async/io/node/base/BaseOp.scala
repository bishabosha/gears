package gears.async.io.node.base

import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.io.Repeatable

import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import scala.scalajs.js
import scala.scalajs.js.wasm.JSPI.allowOrphanJSAwait

object BaseOp {

  /** Between performances. */
  inline val Unentered = 0

  /** Being performed, with no outcome yet, and nobody doing its work: a deadline, its handle becoming ready, its handle
    * closing or a cancel may claim or settle it.
    */
  inline val Waiting = 1

  /** Its work is being done, and only that settles it: by whoever claimed it, for the length of a call, or by a call of
    * the event loop's that cannot be taken back, until it calls back. A cancel or a time limit that comes meanwhile is
    * kept (`interruptedBy`).
    */
  inline val Claimed = 2

  /** It has its outcome, which its performer has yet to take. */
  inline val Settled = 3
}

/** An op as a state machine, which is the op object itself: a performance allocates nothing but the promise it waits
  * on, if it waits. The op is performed by one performer at a time (`requestEntry`, `requestExit`), is linked to the
  * performer's group while it is, so that a cancel of the group reaches it (`cancel`), and may be on a heap of timers,
  * as one entry for its time limit and for a deadline of its own, if it has one (`TimerOp`).
  *
  * It is settled exactly once in a performance, by whichever comes first: its work (`complete`), a deadline
  * (`timerWakeup`), or, while it is only waiting, a cancel. Everything happens on the event loop's one thread, so the
  * transitions need no guard; they are still made in two steps, claim and then settle, wherever work is done between
  * the two, so that nothing else settles the op meanwhile.
  */
abstract class BaseOp extends Cancellable with TimerOp {
  import BaseOp.*

  private var state = Unentered
  private var failure: Throwable | Null = null
  private var interruption: Throwable | Null = null
  private var resume: js.Function1[Unit, ?] | Null = null

  // ---- Entry: one performer at a time

  /** Lets the performer in, or refuses it: an op is not performed twice at once, nor a second time unless it is
    * `Repeatable`.
    */
  def requestEntry(): Unit

  /** Lets the performer out. Its outcome has been taken, and it is unlinked. */
  def requestExit(): Unit

  /** Checks made as the op is entered, before anything is done: `IllegalArgumentException`s. */
  protected def onEntry(): Unit = ()

  protected final def enter(): Unit = {
    failure = null
    interruption = null
    parked = false
    state = Waiting
    try onEntry()
    catch {
      case refused: Throwable =>
        state = Unentered
        throw refused
    }
  }

  protected final def leave(): Unit = {
    state = Unentered
    resume = null
  }

  // ---- Cancellation

  /** The group this performance is linked to, or `Unlinked` between performances. */
  private var linkedTo: CompletionGroup = CompletionGroup.Unlinked

  override def link(group: CompletionGroup): this.type = {
    linkedTo = group
    super.link(group)
  }

  /** A group cancels the members it had when its cancel began, so this can come after the performance it was meant for
    * has ended, even once the next has begun: it counts only if the group now linked to is cancelled.
    */
  final def cancel(): Unit =
    if linkedTo.isCancelled then
      state match {
        case Waiting => settle(new CancellationException())
        case Claimed => if interruption == null then interruption = new CancellationException()
        case _       => ()
      }

  // ---- Outcome

  /** Whether the op has its outcome, and so no longer waits, whether or not its performer has taken it yet. */
  final def settled: Boolean = state == Settled

  /** Whether the op is being performed and has no outcome yet. */
  protected final def pending: Boolean = state == Waiting || state == Claimed

  /** What came for the op while its work was being done and could not stop it, a cancel or its time limit, the first of
    * them; null if nothing did. Whoever does the work decides what it means for the outcome.
    */
  protected final def interruptedBy: Throwable | Null = interruption

  private def claim(): Boolean = {
    val free = state == Waiting
    if free then state = Claimed
    free
  }

  /** Claims the op for whoever found its handle ready, or closing. */
  final def claimAtReady(): Boolean = claim()

  /** Claims the op for work the event loop cannot take back, from its performer. */
  protected final def claimForCall(): Unit =
    if !claim() then throw new IllegalStateException(s"$this is not waiting")

  /** Gives back a claim made with `claimAtReady`, the handle not having been ready after all: the op waits on, and can
    * be claimed again, unless a cancel or its time limit came while it was claimed, which settles it here. Returns
    * whether the op still waits.
    */
  final def releaseReady(): Boolean = {
    if state != Claimed then throw new IllegalStateException(s"$this is not claimed")
    val came = interruption
    if came != null then {
      settle(came)
      false
    } else {
      state = Waiting
      true
    }
  }

  /** Settles the op with its work's outcome, a failure or null for none, if it is still being performed. */
  protected final def complete(failure: Throwable | Null): Unit = if pending then settle(failure)

  private def settle(failure: Throwable | Null): Unit = {
    state = Settled
    this.failure = failure
    val waiting = resume
    resume = null
    if waiting != null then waiting(())
  }

  private var parked = false

  /** Whether this performance suspended its performer, and so left the event loop a turn. */
  final def waited: Boolean = parked

  /** Suspends the performer until the op is settled, and returns or throws its outcome. */
  final def parkUntilSettled(): Unit = {
    if state != Settled then {
      parked = true
      js.await(new js.Promise[Unit]((resolve, _) => resume = resolve))
    }
    rethrow()
  }

  /** Returns or throws the outcome of an op that is settled. */
  final def rethrow(): Unit = {
    val thrown = failure
    if thrown != null then throw thrown
  }

  // ---- Deadlines: a time limit, and for some ops one of their own

  private var heldOn: TimerSource | Null = null
  private var limited = false
  private var limit = 0L

  /** Gives this performance a time limit of `timeoutMillis` from `now`; none if it is negative, or beyond the clock. */
  final def setTimeout(timeoutMillis: Long, now: Long): Unit = {
    limited = timeoutMillis >= 0 && timeoutMillis <= Long.MaxValue / 1_000_000L
    if limited then {
      limit = TimerHeap.deadlineAfter(now, timeoutMillis * 1_000_000L)
      limited = limit != TimerHeap.Never
    }
  }

  /** A deadline of the op's own, at which it completes, if `hasOwnDeadline`. */
  protected def ownDeadline: Long = TimerHeap.Never

  /** Whether `ownDeadline` means anything. */
  protected def hasOwnDeadline: Boolean = false

  final override def hasDeadline: Boolean = limited || hasOwnDeadline

  final def deadlineNow: Long =
    if !limited then ownDeadline else if hasOwnDeadline then math.min(limit, ownDeadline) else limit

  /** Puts the op on `timers` for as long as it waits, if it has a deadline. */
  final def holdTimerOn(timers: TimerSource): Unit =
    if hasDeadline then {
      heldOn = timers
      timers.addTimer(this)
    }

  /** Takes the op off its heap. */
  final def clearTimeout(): Unit = {
    val timers = heldOn
    heldOn = null
    if timers != null then timers.removeTimer(this): Unit
  }

  /** The op's own deadline is earlier than its heap knows of. */
  protected final def deadlineBroughtForward(): Unit = {
    val timers = heldOn
    if timers != null then timers.notifyDeadlineChange(this)
  }

  /** A waiting op is the heap's to fire. One whose work is under way is not, and is dropped from the heap: if it is its
    * time limit that has come, that is kept for whoever does the work, as a cancel is.
    */
  final def claimAtDeadline(): Boolean = {
    if state == Claimed && limited && interruption == null then interruption = timedOut()
    claim()
  }

  private def timedOut(): TimeoutException = new TimeoutException(s"$this timed out")

  /** What the op's own deadline settles it with, a failure or null for none. Only an op with one is asked. */
  protected def wakeup(now: Long): Throwable | Null = null

  /** The time limit decides only if it has passed and came first: at a tie the op has made its own deadline. */
  final def timerWakeup(now: Long): Unit =
    settle(
      if limited && now >= limit && (!hasOwnDeadline || limit < ownDeadline) then timedOut()
      else
        try wakeup(now)
        catch case thrown: Exception => thrown
    )

  override def toString: String = getClass.getSimpleName
}

/** An op performed once: performed again, or by a second performer at once, it throws `IllegalStateException`. */
abstract class SingleUseOp extends BaseOp {
  private var spent = false

  final def requestEntry(): Unit = {
    if spent then throw new IllegalStateException(s"$this is not Repeatable, and has been performed already")
    spent = true
    enter()
  }

  final def requestExit(): Unit = leave()
}

/** An op that may be performed again once its performance has ended, by one performer at a time. */
abstract class RepeatableOp extends BaseOp with Repeatable {
  private var entered = false

  final def requestEntry(): Unit = {
    if entered then throw new IllegalArgumentException(s"$this is already being performed")
    entered = true
    try enter()
    catch {
      case refused: Throwable =>
        entered = false
        throw refused
    }
  }

  final def requestExit(): Unit = {
    leave()
    entered = false
  }
}

/** An op that is done as it is performed, with nothing to wait for. */
abstract class ImmediateOp extends SingleUseOp {
  def perform(): Unit
}

/** A timer of a duration, counted from each performance. */
abstract class AbstractMakeTimerOp(nanos: Long) extends RepeatableOp {
  private var due = TimerHeap.Never

  override protected def onEntry(): Unit = due = TimerHeap.deadlineAfter(System.nanoTime(), nanos)

  override protected def ownDeadline: Long = due
  override protected def hasOwnDeadline: Boolean = true

  /** Whether there is nothing to wait for. */
  final def dueAtOnce: Boolean = nanos == 0
}

/** A timer of a deadline, which may be moved while it waits: later at no cost, since the heap finds that out when the
  * old deadline comes round, and earlier at the cost of its place in the heap.
  */
abstract class AbstractMakeDeadlineOp(initial: Long) extends RepeatableOp with gears.async.io.Deadline {
  private var time = initial

  def deadline: Long = time

  def moveTo(nanoTime: Long): Unit = {
    val earlier = nanoTime < time
    time = nanoTime
    if earlier then deadlineBroughtForward()
  }

  override protected def ownDeadline: Long = time
  override protected def hasOwnDeadline: Boolean = true

  final def dueAtOnce: Boolean = time - System.nanoTime() <= 0
}
