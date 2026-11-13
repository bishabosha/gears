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

  /** Counts the times the handle was reported readable, or writable: read before an attempt, for `awaitReadiness`. */
  def readinessTick(read: Boolean): Int

  /** Makes `op` the handle's waiter for reads, or for writes, if the handle accepts it: what a handle accepts is its
    * own to decide, and it refuses with an `IllegalArgumentException`. Returns the event source that listens for the
    * handle, which is `self` if none did, for `self` then to start listening; or null, with `op` not waiting, if the
    * handle was reported ready since `sinceTick`, for the performer to try again.
    */
  def awaitReadiness(read: Boolean, op: ReadinessOp, sinceTick: Int, self: EventSource): EventSource | Null

  /** `self`, made the handle's listener by `awaitReadiness`, could not listen after all. */
  def listenFailed(read: Boolean, self: EventSource): Unit

  /** Whether `op` was the handle's waiter, and no longer is. */
  def clearWaiter(read: Boolean, op: ReadinessOp): Boolean

  /** `source` reports the handle ready: its waiter, claimed with `claimAtReady` while still its waiter, or null if
    * there is none to claim. A handle reported ready time after time with no op waiting gives up its listener, which
    * it tells to `stopListening`: the next op that waits makes its own worker the listener.
    */
  def readyWaiter(read: Boolean, source: EventSource): ReadinessOp | Null

  /** Whether `source` is the handle's listener for reads, or for writes. */
  def listenedBy(read: Boolean, source: EventSource): Boolean
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
    forgetListener(read = true)
    forgetListener(read = false)
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

  /** The event source listening for the handle to be readable, and to be writable: the first an op waited on, and
    * the same one for as long as ops go on waiting (see `idleReportsLimit`), so that only it is woken for the handle.
    * Guarded by this handle's monitor.
    */
  private var readListener: EventSource | Null = null
  private var writeListener: EventSource | Null = null

  /** Written under this handle's monitor. */
  @volatile private var readTick = 0
  @volatile private var writeTick = 0

  final def readinessTick(read: Boolean): Int = if read then readTick else writeTick

  /** Whether the handle accepts `op` as its waiter while `pending` is. By default one op waits in each direction, and
    * a second is refused; one that has settled, whose performer has yet to clear it, is not waiting, and gives way.
    */
  protected def acceptsWaiter(read: Boolean, op: ReadinessOp, pending: ReadinessOp | Null): Boolean =
    pending == null || pending.settled

  final def awaitReadiness(read: Boolean, op: ReadinessOp, sinceTick: Int, self: EventSource): EventSource | Null =
    synchronized {
      if !acceptsWaiter(read, op, if read then readWaiter else writeWaiter) then
        throw new IllegalArgumentException(s"A ${if read then "read" else "write"} is already pending on this handle")
      // The listener may be another thread's, and have reported the handle ready, to no waiter, after the attempt.
      if readinessTick(read) != sinceTick then null
      else
        if read then readWaiter = op else writeWaiter = op
        var listener = if read then readListener else writeListener
        if listener == null then
          listener = self
          if read then readListener = self else writeListener = self
        listener
    }

  final def listenFailed(read: Boolean, self: EventSource): Unit = synchronized {
    if read then { if readListener eq self then readListener = null }
    else if writeListener eq self then writeListener = null
  }

  final def clearWaiter(read: Boolean, op: ReadinessOp): Boolean = synchronized {
    val held = (if read then readWaiter else writeWaiter) eq op
    if held then if read then readWaiter = null else writeWaiter = null
    held
  }

  /** How many times in a row the handle is reported ready with no op waiting before it gives up its listener. Until
    * then an op that waits costs no system call; past it, the handle is taken to be one whose ops find it ready, for
    * which the reports only wake the listener for nothing, and tie the handle's next wait to a worker that may by
    * then be kept busy.
    */
  protected def idleReportsLimit: Int = 16

  /** Reports with no op waiting since one last was. Guarded by this handle's monitor. */
  private var readIdleReports = 0
  private var writeIdleReports = 0

  final def readyWaiter(read: Boolean, source: EventSource): ReadinessOp | Null = {
    var stop = false
    val claimed = synchronized {
      if read then readTick += 1 else writeTick += 1
      // A report from a source that has stopped listening, already on its way: counted above, for an op about to
      // wait, but the waiter is its new listener's to claim.
      if (if read then readListener else writeListener) ne source then null
      else
        val op = if read then readWaiter else writeWaiter
        if op == null then
          val idle = (if read then readIdleReports else writeIdleReports) + 1
          stop = idle >= idleReportsLimit
          if read then readIdleReports = if stop then 0 else idle
          else writeIdleReports = if stop then 0 else idle
          if stop then if read then readListener = null else writeListener = null
          null
        else
          if read then readIdleReports = 0 else writeIdleReports = 0
          if op.claimAtReady() then op else null
    }
    if stop then source.stopListening(this, read)
    claimed
  }

  final def listenedBy(read: Boolean, source: EventSource): Boolean = synchronized {
    (if read then readListener else writeListener) eq source
  }

  /** First in a close, before anything is released: whoever waits fails. */
  private def failWaiter(read: Boolean): Unit =
    val op = synchronized(if read then readWaiter else writeWaiter)
    if op != null then op.handleClosing()

  /** Then the listeners drop the handle, while its descriptor is still its own. What they registered with the kernel
    * goes as the descriptor is closed.
    */
  private def forgetListener(read: Boolean): Unit =
    val listener = synchronized(if read then readListener else writeListener)
    if listener != null then listener.forgetHandle(this)

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
