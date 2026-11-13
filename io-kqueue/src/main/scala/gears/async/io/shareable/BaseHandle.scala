package gears.async.io.shareable

import gears.async.Async
import gears.async.Future
import gears.async.Listener
import gears.async.io.{shareable => ClosingState}
import gears.async.io.{shareable => UnenteredState}

import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.util.Try

object HandleState:
  inline val UnenteredState = 0
  inline val IdleState = 1
  inline val BusyState = 2
  inline val ClosingState = 3
  inline val ClaimingState = 4
  inline val ReleasingState = 5
  inline val ClosedState = -1
  type State = UnenteredState.type | IdleState.type | BusyState.type | ClosingState.type | ClaimingState.type |
    ReleasingState.type | ClosedState.type

  def showState(state: State): String =
    state match
      case UnenteredState => "Unentered"
      case IdleState      => "Idle"
      case BusyState      => "Busy"
      case ClosingState   => "Closing"
      case ClaimingState  => "Claiming"
      case ReleasingState => "Releasing"
      case ClosedState    => "Closed"

  sealed abstract class Holder:
    private val state = AtomicInteger(UnenteredState)
    final def getState: State = state.get().asInstanceOf[State]
    final def setState(newState: State): Unit = state.set(newState)
    final def compareAndSetState(expected: State, newState: State): Boolean =
      state.compareAndSet(expected, newState)
    final def compareAndExchangeState(expected: State, newState: State): State =
      state.compareAndExchange(expected, newState).asInstanceOf[State]

trait BaseHandle extends HandleState.Holder:
  def open(): Unit
  def requestClose(): Boolean

  /** Makes `op` the handle's waiter for reads, or for writes, if the handle accepts it: what a handle accepts is its
    * own to decide, and it refuses with an `IllegalArgumentException`.
    */
  def setWaiter(read: Boolean, op: ReadinessOp): Unit

  /** Whether `op` was the handle's waiter, and no longer is. */
  def clearWaiter(read: Boolean, op: ReadinessOp): Boolean

  def hasWaiter(read: Boolean): Boolean

  /** The handle's waiter, claimed with `claimAtReady` while still its waiter, or null if there is none to claim. */
  def claimWaiter(read: Boolean): ReadinessOp | Null
  def onComplete(k: Listener[Try[Unit]]): Unit
  def claimWork(): Unit
  def releaseWork(): Unit

trait BaseReadableHandle extends BaseHandle
trait BaseWritableHandle extends BaseHandle

abstract class AbstractBaseHandle extends BaseHandle:
  import HandleState.{BusyState, ClaimingState, ClosedState, ClosingState, IdleState, ReleasingState, UnenteredState}

  opaque type ClosePermit = Unit

  /** does final cleanup of the handle and releases any associated resources. Returns true if the handle was closed,
    * else a listener is required.
    */
  def cleanup()(using ClosePermit): Boolean

  final private def closeHandle()(using ClosePermit): Boolean =
    failWaiter(read = true)
    failWaiter(read = false)
    val closedSync = cleanup()
    if closedSync then
      assert(getState == ClosedState)
      completeClose()
    closedSync

  // TODO: promise uses locks, need a nicer lock-free mechanism for close notification
  private val closePromise: Future.Promise[Unit] = Future.Promise()

  protected def completeClose(): Unit =
    closePromise.complete(WorkpoolScheduler.SuccessUnit)

  final def onComplete(k: Listener[Try[Unit]]): Unit =
    closePromise.onComplete(k)

  /** The ops waiting for the handle to be readable, and to be writable. Guarded by this handle's monitor, which is
    * what makes a claim and a clear exclude each other (see `ReadinessOp.claimAtReady`).
    */
  private var readWaiter: ReadinessOp | Null = null
  private var writeWaiter: ReadinessOp | Null = null

  /** Whether the handle accepts `op` as its waiter while `pending` is. By default one op waits in each direction, and
    * a second is refused; one that has settled, whose performer has yet to clear it, is not waiting, and gives way.
    */
  protected def acceptsWaiter(read: Boolean, op: ReadinessOp, pending: ReadinessOp | Null): Boolean =
    pending == null || pending.settled

  final def setWaiter(read: Boolean, op: ReadinessOp): Unit = synchronized {
    if !acceptsWaiter(read, op, if read then readWaiter else writeWaiter) then
      throw new IllegalArgumentException(s"A ${if read then "read" else "write"} is already pending on this handle")
    if read then readWaiter = op else writeWaiter = op
  }

  final def clearWaiter(read: Boolean, op: ReadinessOp): Boolean = synchronized {
    val held = (if read then readWaiter else writeWaiter) eq op
    if held then if read then readWaiter = null else writeWaiter = null
    held
  }

  final def hasWaiter(read: Boolean): Boolean = synchronized {
    (if read then readWaiter else writeWaiter) != null
  }

  final def claimWaiter(read: Boolean): ReadinessOp | Null = synchronized {
    val op = if read then readWaiter else writeWaiter
    if op != null && op.claimAtReady() then op else null
  }

  /** First in a close, before anything is released: whoever waits fails, and takes itself back from its worker. */
  private def failWaiter(read: Boolean): Unit =
    val op = synchronized(if read then readWaiter else writeWaiter)
    if op != null then op.handleClosing()

  final def open(): Unit =
    if compareAndSetState(UnenteredState, IdleState) then ()
    else throw new IllegalStateException("Handle is already open or closed")

  private val active = AtomicLong(0L) // TODO: long wrap around? (unlikely in practice)
  final def claimWork(): Unit =
    var done = false
    while !done do
      getState match
        case IdleState =>
          if compareAndSetState(IdleState, ClaimingState) then
            active.incrementAndGet()
            if !compareAndSetState(ClaimingState, BusyState) then
              throw new IllegalStateException("Failed to transition from ClaimingState to BusyState")
            done = true
        case BusyState =>
          if compareAndSetState(BusyState, ClaimingState) then
            active.incrementAndGet()
            if !compareAndSetState(ClaimingState, BusyState) then
              throw new IllegalStateException("Failed to transition from ClaimingState to BusyState")
            done = true
        case ClaimingState | ReleasingState =>
          // spin until the state changes
          done = false
        case ClosedState | ClosingState =>
          throw new ClosedChannelException()
        case UnenteredState =>
          throw new IOException("Handle is not open")

  final def releaseWork(): Unit =
    // ALLOW TO RELEASE WORK EVEN IF HANDLE IS CLOSING
    var done = false
    while !done do
      getState match
        case BusyState =>
          if compareAndSetState(BusyState, ReleasingState) then
            val remaining = active.decrementAndGet()
            val transition: HandleState.State = if remaining == 0 then IdleState else BusyState
            if !compareAndSetState(ReleasingState, transition) then
              throw new IllegalStateException(
                "Failed to transition from ReleasingState to " + HandleState.showState(transition)
              )
            done = true
        case ClosingState =>
          val remaining = active.decrementAndGet()
          if remaining == 0 then
            given ClosePermit = ()
            val _ = closeHandle()
          done = true
        case ReleasingState | ClaimingState =>
          // spin until the state changes
          done = false
        case ClosedState =>
          throw new IOException("Handle is closed")
        case UnenteredState =>
          throw new IOException("Handle is not open")
        case IdleState =>
          throw new IllegalStateException("Releasing work when handle is idle")

  final def requestClose(): Boolean =
    var done = false
    var closed = false
    while !done do
      getState match
        case IdleState =>
          if compareAndSetState(IdleState, ClosingState) then
            given ClosePermit = ()
            closed = closeHandle()
            done = true
        case BusyState =>
          if compareAndSetState(BusyState, ClosingState) then
            // will cleanup when work is released
            done = true
        case ClaimingState | ReleasingState =>
          // spin until the state changes
          done = false
        case ClosedState =>
          done = true
          closed = true
        case ClosingState =>
          done = true
        case UnenteredState =>
          throw new IllegalStateException("Handle is not open")
    closed
