package gears.async.io.shareable

/** An op waiting for its handle to be ready, as the handle holds it and as an event source wakes it. */
trait ReadinessOp:

  /** Whether the op has its outcome, and so no longer waits, whether or not its performer has cleared it yet. */
  def settled: Boolean

  /** Claims the op for the worker that found its handle ready. Only the handle calls it, from `reportReady`, while the
    * op is still its waiter and under the lock a performer clears it under: a performer can't leave, and the op be
    * performed again, between the two.
    */
  def claimAtReady(): Boolean

  /** After `claimAtReady`, on the thread of the event source reporting the handle ready, with the handle's lock for
    * this direction held: tries the op's work again. It takes no claim on the handle's work for it: the lock is what
    * a close waits on. Returns how many bytes the work moved if that settled the op; `ReadinessOp.Blocked` if the
    * handle was not ready after all, which leaves the op claimed, still the handle's waiter, to be released with
    * `releaseReady`; or `ReadinessOp.SettledOtherwise` if the op was settled without doing the work, as cancelled or
    * failed.
    */
  def readyWakeup(): Int

  /** Gives the claim back: whether the op still waits. */
  def releaseReady(): Boolean

  /** The handle is closing: the op fails as a closed channel's does. */
  def handleClosing(): Unit

object ReadinessOp:

  /** For `BaseHandle.reportReady`: the source can't say how much the handle is ready for, or what it reported was not
    * only that, the end of a stream or an error among it, which no later report follows.
    */
  inline val Unknown = Long.MaxValue

  /** What `readyWakeup` returns when the handle was not ready after all. */
  inline val Blocked = -1

  /** What `readyWakeup` returns when the op was settled without its work being done. */
  inline val SettledOtherwise = -2
