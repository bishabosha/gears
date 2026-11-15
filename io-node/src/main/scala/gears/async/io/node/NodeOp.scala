package gears.async.io
package node

import gears.async.Async
import gears.async.Cancellable
import gears.async.cancellationScope

import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import scala.scalajs.js
import scala.scalajs.js.wasm.JSPI.allowOrphanJSAwait

/** One performance of an op: how it ends, and what waits for it. Its op either finishes it as it starts, or leaves with
  * Node what will, a listener or a callback, which finishes it from the event loop (`succeed`, `fail`); the future
  * performing it is suspended meanwhile (`suspend`), and goes on once it is finished. A cancel, the time limit and a
  * close of the op's handle stop it (`stop`), at once, undoing what the op left with Node (`release`), unless the op
  * holds it (`hold`): what Node has begun and cannot take back, a file's read or a datagram's send, is waited for, and
  * the op decides how it ends from what stopped it (`stopped`).
  */
private[node] final class Run(op: NodeOp, val start: Long, limit: Long) extends Cancellable {
  private var finished = false
  private var failure: Throwable | Null = null
  private var wake: js.Function1[Unit, ?] | Null = null
  private var limitAlarm: Clock.Alarm | Null = null
  private var held = false
  private var stopping: Throwable | Null = null

  /** Undoes what the op left with Node, called once as the run finishes, however it does. */
  var release: (() => Unit) | Null = null

  /** Called once the run has finished, after `release`: what a close waiting for the run goes on with. */
  var finishing: (() => Unit) | Null = null

  def done: Boolean = finished

  /** What has stopped the run while it was held, a cancel, a time limit or a close, the first of them; null if none. */
  def stopped: Throwable | Null = stopping

  /** From here on the run is finished only by its op: a stop is kept for the op to find (`stopped`). */
  def hold(): Unit = held = true

  def succeed(): Unit = finish(null)

  def fail(failure: Throwable): Unit = finish(failure)

  /** Finishes a held run as what stopped it says, if anything did: true if it was stopped. */
  def failIfStopped(): Boolean = {
    val stop = stopping
    if stop != null then fail(stop)
    stop != null
  }

  private def finish(failure: Throwable | Null): Unit =
    if !finished then {
      finished = true
      this.failure = failure
      val alarm = limitAlarm
      if alarm != null then alarm.cancel()
      limitAlarm = null
      val undo = release
      release = null
      if undo != null then undo()
      val next = finishing
      finishing = null
      if next != null then next()
      val resume = wake
      wake = null
      if resume != null then resume(())
    }

  /** Stops the run with `failure`, unless it has finished, or is held, when the failure is kept for its op. */
  def stop(failure: Throwable): Unit =
    if !finished then
      if held then { if stopping == null then stopping = failure }
      else finish(failure)

  def cancel(): Unit = stop(new CancellationException())

  /** Suspends the caller until the run has finished, its time limit running, and a cancel of the caller's group
    * stopping it.
    */
  def suspend()(using Async): Unit =
    if !finished then {
      if limit != Clock.Never then
        limitAlarm = Clock.at(limit) { () =>
          limitAlarm = null
          if !finished then {
            op.settle(this) // what is due as the limit comes completes
            stop(new TimeoutException(s"$op timed out"))
          }
        }
      cancellationScope(this) {
        if !finished then js.await(new js.Promise[Unit]((resolve, _) => wake = resolve))
      }
    }

  /** Throws the failure the run finished with, if it did with one. */
  def rethrow(): Unit = {
    val thrown = failure
    if thrown != null then throw thrown
  }
}

/** Lets the event loop turn among ops that end without waiting: a future whose every op is ready at once, a read at the
  * end of its stream say, or refused at once, one on a closed handle, would otherwise never leave the loop a turn, and
  * no timer would fire nor any socket be heard. Every `Budget` such ops, whichever futures perform them, the one
  * performing waits for the loop's next turn.
  */
private[node] object Turn {
  private inline val Budget = 32
  private var spent = 0

  def spend(): Unit = {
    spent += 1
    if spent >= Budget then {
      spent = 0
      js.await(new js.Promise[Unit]((resolve, _) => Node.setImmediate(() => resolve(()): Unit): Unit))
    }
  }
}

/** An op of the Node.js backend, performed by the future performing it (`perform`), which makes the checks every op
  * makes, in this order: not cancelled already; a one-shot op not performed before (else `IllegalStateException`), nor
  * any op being performed now; its slots empty and what `check` checks (else `IllegalArgumentException`); its handle
  * open (else the closed handle's failure); then the op itself (`start`), with its handle taken up while it runs, so
  * that a close fails it.
  */
abstract class NodeOp {
  private var used = false
  private var busy = false

  /** The slots the op writes, which must be empty as it is performed. */
  protected def slots: List[Slot[?]] = Nil

  /** The handle the op is on, if any. */
  protected def handle: NodeHandle | Null = null

  /** Checks made as the op is performed, before anything is done: `IllegalArgumentException`s. */
  protected def check(): Unit = ()

  /** Begins the op: finishes `run` before returning if it can, else leaves with Node what will finish it, and with
    * `run` what undoes that (`Run.release`). A failure thrown is the op's.
    */
  protected def start(run: Run): Unit

  /** Called as `run`'s time limit comes, before it fails the op: completes the op if what it waits for has come. */
  private[node] def settle(run: Run): Unit = ()

  private[node] final def perform(timeoutMillis: Long)(using ac: Async): Unit = {
    if ac.group.isCancelled then throw new CancellationException()
    if !this.isInstanceOf[Repeatable] then {
      if used then throw new IllegalStateException(s"$this is not Repeatable, and has been performed already")
      used = true
    }
    if busy then throw new IllegalArgumentException(s"$this is already being performed")
    busy = true
    var waited = false
    try {
      if slots.exists(!_.isEmpty) then
        throw new IllegalArgumentException(
          "The op's slot still holds a value, not yet cleared, which the op would overwrite"
        )
      check()
      val began = System.nanoTime()
      // No limit if negative, or beyond what nanoseconds hold, some 292 years.
      val limit =
        if timeoutMillis < 0 || timeoutMillis > Long.MaxValue / 1_000_000L then Clock.Never
        else Clock.after(began, timeoutMillis * 1_000_000L)
      val run = new Run(this, began, limit)
      val on = handle
      if on != null then on.enter(run)
      try {
        // A failure thrown as the op starts finishes the run, as one reported later does, so that what the op had
        // already left with Node is undone (`Run.release`).
        try start(run)
        catch {
          case js.JavaScriptException(error) => run.fail(Errors.io(error))
          case failure: Exception            => run.fail(failure)
        }
        if !run.done then {
          waited = true
          run.suspend()
        }
        run.rethrow()
      } finally if on != null then on.leave(run)
    } finally {
      busy = false
      // An op that ended without waiting, completed or refused, has left the event loop no turn: it is counted.
      if !waited then Turn.spend()
    }
  }

  override def toString: String = getClass.getSimpleName.stripSuffix("Op")
}
