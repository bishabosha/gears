package gears.async.io.shareable

import gears.async.Async
import gears.async.Listener
import gears.async.io.CompletionQueue
import gears.async.io.SubmissionQueue
import gears.async.io.shareable.TimerHeapOp.Outcome

import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.util.Failure

/** What a backend keeps for a submission queue: the ops in flight, and those of them that have been settled since its
  * performer last looked, by whichever thread settled them.
  *
  * An op in flight has this as its listener, where an op performed by itself has its performer's. Whoever settles the
  * op, the worker that found its handle ready, the one whose heap its time limit was on, tells the listener, which
  * here puts the op among the settled and wakes the performer if it waits. Everything else is the performer's, on its
  * own thread: beginning the ops, and ending them once they are settled.
  *
  * The performer waits on it as a source that has an item whenever something is settled; how it is woken is its
  * backend's (`wake`), which may gather a turn's worth of completions into one wake-up.
  */
abstract class Pipeline[S <: EventSource] extends Async.Source[Unit], Listener[Outcome] {
  private type Op = BackendOp[S] & AbstractTimerHeapOp

  private var flying = new Array[BackendOp[S] | Null](16)
  private var flyingCount = 0
  private val settled = new ConcurrentLinkedQueue[BackendOp[S]]()
  private val waiting = new AtomicReference[Listener[Unit] | Null](null)

  // ---- the listener of every op in flight

  val lock: Listener.ListenerLock | Null = null

  final def complete(outcome: Outcome, op: Async.Source[Outcome]): Unit = {
    settled.add(op.asInstanceOf[BackendOp[S]]): Unit
    wake()
  }

  /** Something has been settled: the performer is woken, now or once more may have been. */
  protected def wake(): Unit = wakeNow()

  /** Wakes the performer, if it waits. */
  protected final def wakeNow(): Unit = {
    val listener = waiting.getAndSet(null)
    if listener != null then listener.completeNow((), this): Unit
  }

  // ---- what the performer waits on

  /** The queue this is of, once it has been performed: for its signal. */
  @volatile private var watching: SubmissionQueue[?] | Null = null
  private val waker: Runnable = () => wake()

  private def arrived: Boolean = {
    val queue = watching
    !settled.isEmpty() || (queue != null && queue.signalled)
  }

  final def poll(k: Listener[Unit]): Boolean = arrived && k.completeNow((), this)

  final def onComplete(k: Listener[Unit]): Unit = {
    waiting.set(k)
    // Settled between the performer's last look and this: nobody was there to wake.
    if arrived then wakeNow()
  }

  final def dropListener(k: Listener[Unit]): Unit = waiting.compareAndSet(k, null): Unit

  // ---- the ops in flight: the performer's

  private def fly(op: BackendOp[S]): Unit = {
    if flyingCount == flying.length then flying = java.util.Arrays.copyOf(flying, flyingCount * 2)
    flying(flyingCount) = op
    op.flightSlot = flyingCount
    flyingCount += 1
  }

  private def land(op: BackendOp[S]): Unit = {
    val slot = op.flightSlot
    val last = flying(flyingCount - 1).nn
    flying(slot) = last
    last.flightSlot = slot
    flyingCount -= 1
    flying(flyingCount) = null
    op.flightSlot = -1
  }

  private def failureOf(op: Op): Throwable | Null = op.poll() match {
    case Some(Failure(failure))                => failure
    case Some(TimerHeapOp.CancelledSentinel)   => new CancellationException()
    case _                                     => null
  }

  /** What an op's failure is, of what beginning it threw: the backend's own, where its platform throws another kind. */
  protected def failureFrom(thrown: Throwable): Throwable = thrown

  /** Ends an op that has finished or been settled, as its performer does once `performOn` has returned or thrown, and
    * hands it over.
    */
  private def finish[O](op: Op, thrown: Throwable | Null, completed: CompletionQueue[O])(using Async): Unit = {
    var failure = thrown
    try op.endOn()
    catch case ending: Throwable => if failure == null then failure = ending
    op.clearTimeout()
    if failure == null then failure = failureOf(op)
    try op.requestExit()
    catch case leaving: Throwable => if failure == null then failure = leaving
    val tag = op.flightTag
    op.flightTag = null
    completed.add(op.asInstanceOf[O], tag, failure)
  }

  private def takeSettled[O](ops: SubmissionQueue[O], completed: CompletionQueue[O])(using Async): Unit = {
    var op = settled.poll()
    while op != null do {
      // Not one given up meanwhile, whose completion was on its way.
      if op.flightSlot >= 0 then {
        land(op)
        ops.flying(-1)
        finish(op.asInstanceOf[Op], null, completed)
      }
      op = settled.poll()
    }
  }

  /** What `Io.performQueue` does, on `performer`, the event source the ops are begun on. `whole` performs an op whose
    * kind is not begun and ended in two parts, as `perform` does, waiting for it.
    */
  final def perform[O](ops: SubmissionQueue[O], completed: CompletionQueue[O], performer: OpPerformer[S] & S)(
      whole: (BackendOp[S], Long) => Unit
  )(using Async): Unit = {
    if watching == null then {
      watching = ops
      ops.waker = waker
    }
    takeSettled(ops, completed)
    val submitted = ops.submitted
    var i = 0
    while i < submitted do {
      val op = ops.op(i).asInstanceOf[Op]
      val limit = ops.limit(i)
      val tag = ops.tag(i)
      if !op.begins then
        try {
          whole(op, limit)
          completed.add(op.asInstanceOf[O], tag, null)
        } catch case failure: Throwable => completed.add(op.asInstanceOf[O], tag, failureFrom(failure))
      else {
        var entered = false
        try {
          op.requestEntry()
          entered = true
          op.flightTag = tag
          op.setTimeout(limit)
          if op.beginOn(performer) then finish(op, null, completed)
          else {
            fly(op)
            ops.flying(1)
            op.onComplete(this) // settled already, perhaps: then it is among the settled by now
          }
        } catch {
          case failure: Throwable =>
            if entered then finish(op, failureFrom(failure), completed)
            else completed.add(op.asInstanceOf[O], tag, failureFrom(failure))
        }
      }
      i += 1
    }
    ops.started()
    takeSettled(ops, completed)
    if completed.count == 0 && !ops.signalled && (flyingCount > 0 || ops.lingers) then {
      try Parking.parkOn(this)
      catch {
        case cancelled: CancellationException =>
          giveUp(ops, completed)
          throw cancelled
      }
      takeSettled(ops, completed)
    }
    if ops.signalled then ops.signalled = false
  }

  /** Starts one op of the queue's now, for a performer that would rather go on than be told later: whether it is done,
    * its failure thrown. If not, it is in flight with `tag`, and its completion comes with a later `perform`.
    */
  final def startNow[O](ops: SubmissionQueue[O], submitted: O, tag: AnyRef | Null, limit: Long, performer: OpPerformer[S] & S)(
      whole: (BackendOp[S], Long) => Unit
  )(using Async): Boolean = {
    val op = submitted.asInstanceOf[Op]
    if !op.begins then {
      whole(op, limit)
      true
    } else {
      op.requestEntry()
      var done = false
      var failure: Throwable | Null = null
      try {
        op.setTimeout(limit)
        done = op.beginOn(performer)
      } catch {
        case thrown: Throwable =>
          failure = failureFrom(thrown)
          done = true
      }
      if done then {
        try op.endOn()
        catch case ending: Throwable => if failure == null then failure = ending
        op.clearTimeout()
        if failure == null then failure = failureOf(op)
        try op.requestExit()
        catch case leaving: Throwable => if failure == null then failure = leaving
        if failure != null then throw failure
        true
      } else {
        op.flightTag = tag
        fly(op)
        ops.flying(1)
        op.onComplete(this)
        false
      }
    }
  }

  /** The performer has been cancelled: every op in flight is given up, as `perform` gives up its one op, which cancels
    * it unless whoever does its work has it already, and then waits for that.
    */
  private def giveUp[O](ops: SubmissionQueue[O], completed: CompletionQueue[O])(using Async): Unit = {
    while flyingCount > 0 do {
      val op = flying(flyingCount - 1).nn.asInstanceOf[Op]
      land(op)
      ops.flying(-1)
      op.dropListener(this)
      finish(op, null, completed)
    }
    settled.clear()
  }
}

object Pipeline {

  /** The pipeline `ops` has, made by `make` for its first `performQueue`. */
  inline def of[S <: EventSource, O](ops: SubmissionQueue[O])(inline make: => Pipeline[S]): Pipeline[S] = {
    val attached = ops.attached
    if attached != null then attached.asInstanceOf[Pipeline[S]]
    else {
      val made = make
      ops.attached = made
      made
    }
  }
}
