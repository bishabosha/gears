package gears.async.io.shareable


import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/** A handle's state and the work claimed on it, in one word, so that each changes with regard to the other in one
  * step: the state in the top byte, and in the 56 bits below it the count of the work now claimed.
  */
object HandleState:
  inline val UnenteredState = 0
  inline val OpenState = 1
  inline val ClosingState = 2
  inline val ClosedState = 3
  type State = UnenteredState.type | OpenState.type | ClosingState.type | ClosedState.type

  def showState(state: State): String =
    state match
      case UnenteredState => "Unentered"
      case OpenState      => "Open"
      case ClosingState   => "Closing"
      case ClosedState    => "Closed"

  private inline val StateShift = 56

  /** The most work that can be claimed at once, and the mask of its count in the word. */
  inline val MaxClaimed = 0x00ffffffffffffffL

  private inline def stateOf(word: Long): Int = (word >>> StateShift).toInt
  private inline def claimedOf(word: Long): Long = word & MaxClaimed
  private inline def wordOf(state: Int, claimed: Long): Long = (state.toLong << StateShift) | claimed

  sealed abstract class Holder:
    private val word = AtomicLong(wordOf(UnenteredState, 0L))

    final def getState: State = stateOf(word.get()).asInstanceOf[State]

    /** How much work is claimed on the handle now. */
    final def claimedWork: Long = claimedOf(word.get())

    /** Moves from `expected` to `newState`, whatever work is claimed, which stays as it is. */
    final def compareAndSetState(expected: State, newState: State): Boolean =
      var result = false
      var done = false
      while !done do
        val current = word.get()
        if stateOf(current) != expected then done = true
        else if word.compareAndSet(current, wordOf(newState, claimedOf(current))) then
          result = true
          done = true
      result

    /** Claims work if the handle is open: the state it was found in, which is `OpenState` if the work was claimed. */
    protected final def claimIfOpen(): State =
      var found: Int = UnenteredState
      var done = false
      while !done do
        val current = word.get()
        found = stateOf(current)
        if found != OpenState then done = true
        else if claimedOf(current) == MaxClaimed then throw new IllegalStateException("Too much work claimed")
        else done = word.compareAndSet(current, current + 1)
      found.asInstanceOf[State]

    /** Gives back work claimed with `claimIfOpen`, which a handle that has begun to close still lets its work do:
      * whether that was the last of a closing handle's, for the caller then to close it.
      */
    protected final def releaseClaimed(): Boolean =
      var last = false
      var done = false
      while !done do
        val current = word.get()
        if claimedOf(current) == 0 then
          throw new IllegalStateException(s"Releasing work when none is claimed (${showState(getState)})")
        else if word.compareAndSet(current, current - 1) then
          last = stateOf(current) == ClosingState && claimedOf(current) == 1
          done = true
      last

    /** Begins the close of an open handle: the state it was found in, and, if that was `OpenState`, whether no work
      * was claimed, for the caller then to close it; otherwise the last work to be released does.
      */
    protected final def beginClose(): (found: State, idle: Boolean) =
      var found: Int = UnenteredState
      var idle = false
      var done = false
      while !done do
        val current = word.get()
        found = stateOf(current)
        if found != OpenState then done = true
        else if word.compareAndSet(current, wordOf(ClosingState, claimedOf(current))) then
          idle = claimedOf(current) == 0
          done = true
      (found.asInstanceOf[State], idle)

trait BaseHandle extends HandleState.Holder:
  def open(): Unit
  def requestClose(): Boolean

  /** Counts the times the handle's listener reported it readable, or writable: read before an attempt, for
    * `awaitReadiness`.
    */
  def readinessTick(read: Boolean): Int

  /** Makes `op` the handle's waiter for reads, or for writes, if the handle accepts it: what a handle accepts is its
    * own to decide, and it refuses with an `IllegalArgumentException`. Returns the event source that listens for the
    * handle, which is `self` if none did, for `self` then to start listening; or null, with `op` not waiting, if the
    * handle was reported ready since `sinceTick`, for the performer to try again.
    */
  def awaitReadiness(read: Boolean, op: ReadinessOp, sinceTick: Int, self: EventSource): EventSource | Null

  /** Whether the handle is known to have nothing to read, or no room to write, as of `tick`, read with
    * `readinessTick` just before: an op then waits without trying first, since the try could only find that out
    * again. It is known from the last try that found it so, or from an op that took all a report was for, and only
    * while a listener would report anything new, which moves the tick on.
    */
  def drainedAt(read: Boolean, tick: Int): Boolean

  /** Whether `op` was the handle's waiter, and no longer is. Costs nothing for an op that is not. */
  def clearWaiter(read: Boolean, op: ReadinessOp): Boolean

  /** `source` reports the handle ready, for `available` bytes to read or room to write, or `ReadinessOp.Unknown`. The
    * handle's waiter, if it can be claimed, does its work here and now, on `source`'s thread, and is the waiter no
    * longer once that settles it; one that finds the handle not ready after all waits on. An op that takes all that
    * was reported leaves the handle known to be drained until its next report. A handle reported ready time after
    * time with no op waiting gives up its listener, which it tells to `stopListening`: the next op that waits makes
    * its own worker the listener. A report from a source that is not the listener is ignored.
    */
  def reportReady(read: Boolean, source: EventSource, available: Long): Unit

  /** Whether `source` is the handle's listener for reads, or for writes. */
  def listenedBy(read: Boolean, source: EventSource): Boolean

  /** Has `owner` told once the handle is closed, at once if it already is. For whoever's `requestClose` began the
    * close and returned false: a close has one owner, so one listener at most.
    */
  def onClosed(owner: CloseListener): Unit
  def claimWork(): Unit
  def releaseWork(): Unit

trait BaseReadableHandle extends BaseHandle
trait BaseWritableHandle extends BaseHandle

/** Told that a handle's close is done, by whichever thread finished it. */
trait CloseListener:
  def handleClosed(): Unit

object AbstractBaseHandle:
  private[shareable] case object Closed

abstract class AbstractBaseHandle extends BaseHandle:
  import HandleState.{ClosedState, ClosingState, OpenState, UnenteredState}

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

  /** Whoever waits for the close to be done: null until there is one, its owner while it waits, and `Closed` from
    * when the close is done, whichever comes first. The owner is told exactly once, by whichever of the two comes
    * second.
    */
  private val closeListener = AtomicReference[CloseListener | AbstractBaseHandle.Closed.type | Null](null)

  protected def completeClose(): Unit =
    closeListener.getAndSet(AbstractBaseHandle.Closed) match
      case owner: CloseListener => owner.handleClosed()
      case _                    => ()

  final def onClosed(owner: CloseListener): Unit =
    if !closeListener.compareAndSet(null, owner) then
      closeListener.get() match
        case AbstractBaseHandle.Closed => owner.handleClosed()
        case _ => throw new IllegalStateException("A close already has its owner waiting")

  /** What the handle keeps for the ops that wait for it to be readable, and the same again for writable. Its own
    * monitor guards it, one for each direction, so that a reader and a writer of one handle never wait on each other.
    */
  private final class Waiting {

    /** The op that waits. Read without the lock by `clearWaiter`, to find that it has nothing to do. */
    @volatile var waiter: ReadinessOp | Null = null

    /** The event source listening for the handle: the first an op waited on, and the same one for as long as ops go
      * on waiting (see `idleReportsLimit`), so that only it is woken for the handle.
      */
    var listener: EventSource | Null = null

    /** Counts the listener's reports. */
    @volatile var tick = 0

    /** The tick at which the handle was last known drained, which is so now if it is still the tick: any other
      * value, as it is to begin with and when a listener goes, means it is not known.
      */
    @volatile var drainedTick = -1

    /** Reports with no op waiting since one last was. */
    var idleReports = 0

    /** No listener, so nothing would report the handle ready: whether it is drained is not known. */
    def forgetDrained(): Unit = drainedTick = tick - 1
  }

  private val reads = new Waiting
  private val writes = new Waiting

  private inline def side(read: Boolean): Waiting = if read then reads else writes

  final def readinessTick(read: Boolean): Int = side(read).tick

  /** Whether the handle accepts `op` as its waiter while `pending` is. By default one op waits in each direction, and
    * a second is refused; one that has settled, whose performer has yet to clear it, is not waiting, and gives way.
    */
  protected def acceptsWaiter(read: Boolean, op: ReadinessOp, pending: ReadinessOp | Null): Boolean =
    pending == null || pending.settled

  final def drainedAt(read: Boolean, tick: Int): Boolean = side(read).drainedTick == tick

  final def awaitReadiness(read: Boolean, op: ReadinessOp, sinceTick: Int, self: EventSource): EventSource | Null = {
    val waiting = side(read)
    var start = false
    val listener = waiting.synchronized {
      if !acceptsWaiter(read, op, waiting.waiter) then
        throw new IllegalArgumentException(s"A ${if read then "read" else "write"} is already pending on this handle")
      // The listener may be another thread's, and have reported the handle ready, to no waiter, after the attempt.
      if waiting.tick != sinceTick then null
      else
        waiting.waiter = op
        var listener = waiting.listener
        if listener == null then
          listener = self
          start = true
          waiting.listener = self
        // The op waits because the handle was not ready at this tick: so the next need not try before the next.
        waiting.drainedTick = sinceTick
        listener
    }
    // Only as `self` becomes the listener: it then reports the handle until it stops being one. No report can come
    // before this, and a second op to wait meanwhile is refused, as this one is the waiter.
    if start then
      try self.startListening(this, read)
      catch
        case failure: Throwable =>
          waiting.synchronized {
            if waiting.listener eq self then waiting.listener = null
            waiting.forgetDrained()
            if waiting.waiter eq op then waiting.waiter = null
          }
          throw failure
    listener
  }

  final def clearWaiter(read: Boolean, op: ReadinessOp): Boolean = {
    val waiting = side(read)
    // Looked at first without the lock: an op done by its handle's listener was taken out by `reportReady`, and only
    // its own performer, here, puts it in.
    if waiting.waiter ne op then false
    else
      // Under the lock, which `reportReady` claims under: see `ReadinessOp.claimAtReady`.
      waiting.synchronized {
        val held = waiting.waiter eq op
        if held then waiting.waiter = null
        held
      }
  }

  /** How many times in a row the handle is reported ready with no op waiting before it gives up its listener. Until
    * then an op that waits costs no system call; past it, the handle is taken to be one whose ops find it ready, for
    * which the reports only wake the listener for nothing, and tie the handle's next wait to a worker that may by
    * then be kept busy.
    */
  protected def idleReportsLimit: Int = 16

  /** The whole of a report under the one lock: the op is claimed while it is the waiter, does its work, and is the
    * waiter no longer, with nothing else to take or to swap. The work is done with the lock held and no claim on the
    * handle's work: a close, which begins by failing the waiters under this lock, waits for it, and a report that
    * comes once the handle is closing does nothing.
    */
  final def reportReady(read: Boolean, source: EventSource, available: Long): Unit = {
    val waiting = side(read)
    var stop = false
    waiting.synchronized {
      // A report from a source that has stopped listening, already on its way, is not this listener's: ignored.
      if waiting.listener eq source then
        waiting.tick += 1
        val op = waiting.waiter
        if op == null then
          val idle = waiting.idleReports + 1
          stop = idle >= idleReportsLimit
          waiting.idleReports = if stop then 0 else idle
          if stop then
            waiting.listener = null
            waiting.forgetDrained()
        else
          waiting.idleReports = 0
          if getState == OpenState && op.claimAtReady() then
            val moved = op.readyWakeup()
            if moved == ReadinessOp.Blocked then
              // If a cancel settles the op as its claim is given back, its performer clears it.
              op.releaseReady(): Unit
            else
              waiting.waiter = null
              // All that was there as the source took its report: whatever comes after it is reported anew, so until
              // then there is nothing, and the next op need not look. Not at the end of a stream, where nothing was
              // moved, since no report follows it.
              if moved > 0 && moved >= available then waiting.drainedTick = waiting.tick
    }
    if stop then source.stopListening(this, read)
  }

  final def listenedBy(read: Boolean, source: EventSource): Boolean = {
    val waiting = side(read)
    waiting.synchronized(waiting.listener eq source)
  }

  /** First in a close, before anything is released: whoever waits fails. Under the lock, which waits for a report's
    * work that is under way, and after which no report does any.
    */
  private def failWaiter(read: Boolean): Unit =
    val waiting = side(read)
    val op = waiting.synchronized(waiting.waiter)
    if op != null then op.handleClosing()

  /** Then the listeners drop the handle, while its descriptor is still its own. What they registered with the kernel
    * goes as the descriptor is closed.
    */
  private def forgetListener(read: Boolean): Unit =
    val waiting = side(read)
    val listener = waiting.synchronized(waiting.listener)
    if listener != null then listener.forgetHandle(this)

  final def open(): Unit =
    if compareAndSetState(UnenteredState, OpenState) then ()
    else throw new IllegalStateException("Handle is already open or closed")

  final def claimWork(): Unit =
    claimIfOpen() match
      case OpenState                  => ()
      case ClosedState | ClosingState => throw new ClosedChannelException()
      case UnenteredState             => throw new IOException("Handle is not open")

  /** Work claimed is released even once the handle is closing: the last of it then closes the handle. */
  final def releaseWork(): Unit =
    if releaseClaimed() then
      given ClosePermit = ()
      val _ = closeHandle()

  final def requestClose(): Boolean =
    val begun = beginClose()
    begun.found match
      case OpenState =>
        if begun.idle then
          given ClosePermit = ()
          closeHandle()
        else false // the last work released closes it
      // Closing is done once: a second close fails as any op on a closed handle does.
      case ClosingState | ClosedState => throw new ClosedChannelException()
      case UnenteredState             => throw new IllegalStateException("Handle is not open")
