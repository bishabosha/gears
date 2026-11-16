package gears.async.io.node.base

import java.io.IOException

object HandleState {
  inline val OpenState = 1
  inline val ClosingState = 2
  inline val ClosedState = 3
}

/** Told that a handle's close is done. */
trait CloseListener {
  def handleClosed(): Unit
}

/** A handle's state and the work claimed on it. A handle is open from the moment it is made: construction is
  * acquisition. Work the event loop cannot take back, a file's read under way, is claimed on the handle (`claimWork`)
  * until it calls back (`releaseWork`); ops that only wait are the handle's waiters, in whatever way the kind of handle
  * keeps them (`awaitReadiness`).
  *
  * A close has one owner. It fails the waiters at once, and closes the handle's I/O object once no work is claimed: at
  * once if none is, else as the last of it is released, so that the object is never another's under a call. The owner
  * is told when that is done (`onClosed`).
  */
abstract class BaseHandle {
  import HandleState.*

  private var state = OpenState
  private var claimed = 0
  private var closeOwner: CloseListener | Null = null

  /** The failure of an op on this handle once it is closed. */
  protected def closedFailure(): IOException

  final def isOpen: Boolean = state == OpenState

  /** Throws the failure of an op on this handle if it has been closed. */
  final def checkOpen(): Unit = if state != OpenState then throw closedFailure()

  /** The failure of an op on this handle if it has been closed, or null. */
  final def failureIfClosed: IOException | Null = if state != OpenState then closedFailure() else null

  /** Claims work on the handle, which keeps its I/O object open until the work is released; fails if it is closed. */
  final def claimWork(): Unit = {
    checkOpen()
    claimed += 1
  }

  /** Work claimed is released even once the handle is closing: the last of it then closes the handle. */
  final def releaseWork(): Unit = {
    if claimed == 0 then throw new IllegalStateException(s"Releasing work on $this when none is claimed")
    claimed -= 1
    if claimed == 0 && state == ClosingState then closeHandle(): Unit
  }

  /** Begins the close: whether it is done already. If not, work claimed is still under way, and the last of it to be
    * released closes the handle, or its I/O object closes in its own time; `onClosed` tells when. Fails if the handle
    * was closed already: closing is done once.
    */
  final def requestClose(): Boolean = {
    checkOpen()
    state = ClosingState
    failWaiters()
    if claimed == 0 then closeHandle() else false
  }

  private def closeHandle(): Boolean = {
    val closedNow = cleanup()
    if closedNow then completeClose()
    closedNow
  }

  /** Closes the handle's I/O object and releases what goes with it. Returns true if that is done as it returns; if not,
    * `completeClose` is to be called once it is.
    */
  protected def cleanup(): Boolean

  protected final def completeClose(): Unit =
    if state != ClosedState then {
      state = ClosedState
      val owner = closeOwner
      closeOwner = null
      if owner != null then owner.handleClosed()
    }

  /** Has `owner` told once the handle is closed, at once if it already is. For whoever's `requestClose` began the close
    * and returned false: a close has one owner, so one listener at most.
    */
  final def onClosed(owner: CloseListener): Unit =
    if state == ClosedState then owner.handleClosed()
    else if closeOwner != null then throw new IllegalStateException("A close already has its owner waiting")
    else closeOwner = owner

  /** First in a close, before anything is released: whoever waits fails, as on a closed handle. */
  protected def failWaiters(): Unit

  /** Makes `op` a waiter of the handle, for reads or for writes, if the handle accepts it: what a handle accepts is its
    * own to decide, and it refuses with an `IllegalArgumentException`. From then on the handle wakes it when it finds
    * itself ready (`ReadinessOp.readyWakeup`).
    */
  def awaitReadiness(read: Boolean, op: ReadinessOp): Unit

  /** `op` no longer waits: whether it was a waiter. */
  def clearWaiter(read: Boolean, op: ReadinessOp): Boolean
}

/** A handle with one waiter in each direction, as a stream has: a second read, or write, pending is refused, since two
  * would take or interleave the stream's bytes. One that has settled, whose performer has yet to clear it, is not
  * waiting, and gives way.
  */
trait OneWaiterEachWay extends BaseHandle {
  private var readWaiter: ReadinessOp | Null = null
  private var writeWaiter: ReadinessOp | Null = null

  /** Called as an op begins to wait, and as it stops: for a handle that must do something to learn that it is ready.
    */
  protected def listening(read: Boolean, on: Boolean): Unit = ()

  final def awaitReadiness(read: Boolean, op: ReadinessOp): Unit = {
    val pending = if read then readWaiter else writeWaiter
    if pending != null && !pending.settled then
      throw new IllegalArgumentException(
        s"An op ${if read then "reading" else "writing"}, as $op does, is already pending on its handle, $this"
      )
    if read then readWaiter = op else writeWaiter = op
    listening(read, on = true)
  }

  final def clearWaiter(read: Boolean, op: ReadinessOp): Boolean = {
    val held = (if read then readWaiter else writeWaiter) eq op
    if held then {
      if read then readWaiter = null else writeWaiter = null
      listening(read, on = false)
    }
    held
  }

  /** The handle is ready, for reads or for writes: its waiter does its work, or, finding it not ready after all, waits
    * on for the next time. A waiter that cannot be claimed was settled some other way.
    */
  protected final def readyWaiter(read: Boolean): Unit = {
    val op = if read then readWaiter else writeWaiter
    if op != null && op.claimAtReady() && !op.readyWakeup() then op.releaseReady(): Unit
  }

  protected def failWaiters(): Unit = {
    val reader = readWaiter
    val writer = writeWaiter
    if reader != null then reader.handleClosing()
    if writer != null then writer.handleClosing()
  }
}

/** Ops waiting on a handle in turn, first come first served: a listener's accepts, a datagram socket's receives, each a
  * whole connection or a whole packet, so that several may wait where a stream takes one.
  */
final class WaitQueue {
  private val ops = scala.collection.mutable.ArrayDeque.empty[ReadinessOp]

  def add(op: ReadinessOp): Unit = ops.append(op)

  def remove(op: ReadinessOp): Boolean = {
    val at = ops.indexWhere(_ eq op)
    if at >= 0 then ops.remove(at): Unit
    at >= 0
  }

  /** The handle is ready: the ops waiting do their work in order, for as long as each finds it so. */
  def ready(): Unit = {
    var going = true
    while going && ops.nonEmpty do {
      val op = ops.head
      if op.settled then ops.removeHead(): Unit // its performer has yet to clear it
      else if !op.claimAtReady() then going = false
      else if op.readyWakeup() then ops.removeHead(): Unit
      else {
        op.releaseReady(): Unit
        going = false
      }
    }
  }

  def failAll(): Unit = {
    val waiting = ops.toList
    ops.clear()
    waiting.foreach(_.handleClosing())
  }
}
