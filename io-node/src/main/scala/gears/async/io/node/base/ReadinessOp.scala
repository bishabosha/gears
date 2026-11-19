package gears.async.io.node.base

/** An op waiting for its handle to be ready, as the handle holds it and wakes it. */
trait ReadinessOp {

  /** Whether the op has its outcome, and so no longer waits, whether or not its performer has cleared it yet. */
  def settled: Boolean

  /** Claims the op for the handle that found itself ready, while the op is still its waiter. */
  def claimAtReady(): Boolean

  /** After `claimAtReady`: tries the op's work again. True if that settled the op; false if the handle was not ready
    * after all, which leaves the op claimed, still the handle's waiter, for the handle to `releaseReady`.
    */
  def readyWakeup(): Boolean

  /** Gives the claim back: whether the op still waits. */
  def releaseReady(): Boolean

  /** The handle is closing: the op fails as an op on a closed handle does. */
  def handleClosing(): Unit
}

object AbstractReadinessOp {

  /** What `attempt` returns when the handle was not ready. */
  inline val Blocked = -1
}

/** An op that does its work when its handle is ready for it: tried at once by its performer and, if the handle was not
  * ready, again by the handle each time it finds that it is. Nothing is left with the event loop for the op itself: it
  * is the handle's waiter, a reference, until it is done.
  */
abstract class AbstractReadinessOp[H <: BaseHandle](protected val handle: H) extends RepeatableOp with ReadinessOp {

  /** One attempt at the work, which must not block: how much it moved, which may be nothing, as at the end of a stream,
    * or `Blocked` if the handle was not ready, and nothing was done.
    */
  protected def attempt(): Int

  /** Whether the op waits for the handle to be readable, rather than writable. */
  protected def isRead: Boolean

  final def handleClosing(): Unit = complete(handle.failureIfClosed)

  final def readyWakeup(): Boolean =
    if interruptedBy != null then false // `releaseReady` settles it as what came says
    else {
      val moved =
        try attempt()
        catch {
          case failure: Exception =>
            complete(failure)
            0
        }
      if moved == AbstractReadinessOp.Blocked then false
      else {
        complete(null)
        true
      }
    }

  /** Does the work at once, which most often does it, or else waits for the handle to find itself ready and do it, for
    * the handle to close, or for the op's time limit or cancel. Returns whether it waited.
    */
  final def performWhenReady(timers: TimerSource): Boolean = {
    handle.checkOpen()
    if attempt() != AbstractReadinessOp.Blocked then false
    else {
      handle.awaitReadiness(isRead, this)
      try {
        holdTimerOn(timers)
        parkUntilSettled()
      } finally handle.clearWaiter(isRead, this): Unit
      true
    }
  }
}

/** An op whose work is a call of the event loop's that calls back: it waits for that, on a handle or to open one. Mixed
  * into an op that is performed once, or one that is repeatable.
  */
trait CallOp extends BaseOp {

  /** Makes the call. A failure thrown is the op's. */
  protected def call(): Unit

  /** Called once the performance is over, however it ended: undoes what a call that was abandoned left behind. */
  protected def finish(): Unit = ()

  /** Makes the call and waits for it, the op's time limit running. Returns whether it waited. */
  final def performCall(timers: TimerSource): Boolean =
    try {
      call()
      val waits = !settled
      if waits then holdTimerOn(timers)
      parkUntilSettled()
      waits
    } finally finish()
}

/** A call on a handle that the event loop cannot take back, a file's read or a datagram's send: the handle's work is
  * claimed until it calls back, so that a close waits for it, and so is the op, so that a cancel or a time limit that
  * comes meanwhile is kept for the callback to decide on.
  */
trait HandleCall[H <: BaseHandle] extends CallOp {
  protected def handle: H

  /** Claims the handle's work and the op, and makes the call with `body`; if that throws, nothing was begun. */
  protected final def calling(body: => Unit): Unit = {
    handle.claimWork()
    try {
      claimForCall()
      body
    } catch {
      case failure: Throwable =>
        handle.releaseWork()
        throw failure
    }
  }

  /** The call has called back: releases the handle's work, and gives what the op fails with if the call's outcome is to
    * be dropped, what came for the op meanwhile or its handle's close; null if the outcome stands.
    */
  protected final def returned(): Throwable | Null = {
    handle.releaseWork()
    val came = interruptedBy
    if came != null then came else handle.failureIfClosed
  }
}

/** A close of a handle, its one owner: done at once if no work is claimed on the handle, else once the last of it is
  * released. A close once begun finishes, whatever comes for its performer meanwhile.
  */
abstract class AbstractCloseOp(handle: BaseHandle) extends SingleUseOp with CloseListener {

  /** Whether the handle is closed already. */
  final def pollClose(): Boolean =
    if handle.requestClose() then true
    else {
      claimForCall()
      handle.onClosed(this)
      settled
    }

  final def handleClosed(): Unit = complete(null)
}
