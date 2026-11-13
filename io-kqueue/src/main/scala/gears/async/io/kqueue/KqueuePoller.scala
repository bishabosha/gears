package gears.async.io.kqueue

import gears.async.io.kqueue.internal.KqueueLoop
import gears.async.io.shareable.EventSource
import gears.async.io.shareable.Poller
import gears.async.io.shareable.ReadinessOp
import gears.async.io.shareable.ReadinessWaiters

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

trait KqueuePoller(maxEvents: Int) extends Poller, EventSource {
  private val kq = KqueueLoop.open()

  private val zone = Zone.open()

  val events = KqueueLoop.allocateQueue(maxEvents)(using zone)

  type ThisWorkerSource = KqueuePoller
  def asThisWorkerSource: this.type & ThisWorkerSource = this

  private val WakeEvent = 0.toUSize
  KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
    KqueueLoop.addUser(events(0), WakeEvent) // edge-triggered, registered once
  }

  /** Guards the waiters together with their filters: a filter is on the kqueue only while its descriptor has an op
    * waiting, and both change as one step, as a waiter can be cleared from another thread than this worker's (by a
    * close, or by a performer that a cancel resumed elsewhere).
    */
  private val waitersLock = new Object
  private val readWaiters = new ReadinessWaiters
  private val writeWaiters = new ReadinessWaiters

  private inline def waiters(read: Boolean): ReadinessWaiters = if read then readWaiters else writeWaiters

  /** One-shot and level-triggered: reported once if `fd` is ready now or when it next is, then gone, so that a
    * descriptor nobody is waiting on any more costs nothing. `dispatch` arms it again for ops still waiting.
    */
  private def arm(fd: Int, read: Boolean): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { changes =>
      KqueueLoop.addFile(changes(0), fd, read, clear = false, oneShot = true)
    }

  /** Holds `op` as waiting for `fd` to be readable, or writable, and has the kqueue report it. */
  final def registerWaiter(fd: Int, read: Boolean, op: ReadinessOp): Unit = waitersLock.synchronized {
    val ws = waiters(read)
    val wasEmpty = ws.isEmpty(fd)
    ws.append(fd, op)
    if wasEmpty then
      try arm(fd, read)
      catch
        case failure: Throwable =>
          ws.remove(fd, op): Unit
          throw failure
  }

  /** Takes `op` back, and the filter off the kqueue with the last op waiting on `fd`. An op that is no longer held,
    * because it was dispatched or already cleared, changes nothing: so nothing here touches a descriptor number that
    * has since become another handle's.
    */
  final def clearWaiter(fd: Int, read: Boolean, op: ReadinessOp): Unit = waitersLock.synchronized {
    val ws = waiters(read)
    if ws.remove(fd, op) && ws.isEmpty(fd) && !closed then KqueueLoop.deleteFileIfOpen(kq, fd, read): Unit
  }

  /** `fd` is ready: its waiting ops do their work in turn, until one finds it not ready any more, which goes back to
    * the front and has the kqueue report `fd` again. An op that can't be claimed was settled some other way, and is
    * dropped.
    */
  private def dispatch(fd: Int, read: Boolean): Unit = {
    val ws = waiters(read)
    var more = true
    while more do
      var op: ReadinessOp | Null = null
      var claimed = false
      waitersLock.synchronized {
        op = ws.takeFirst(fd)
        // Claimed while still held, under the lock `clearWaiter` takes: see `ReadinessOp.claimAtReady`.
        if op != null then claimed = op.claimAtReady()
      }
      if op == null then more = false
      else if claimed && !op.readyWakeup() then
        more = false
        waitersLock.synchronized {
          ws.prepend(fd, op)
          // The claim kept the performer from leaving until the op was held again. If a cancel settles it here, the
          // performer clears it; until then the filter is armed for it at worst once for nothing.
          op.releaseReady(): Unit
          arm(fd, read)
        }
  }

  /** Set before the kqueue is closed, which the worker does as it leaves its loop. */
  @volatile private var closed = false

  /** A worker that has left has nothing to wake, and a `wake` racing its `close` may find the kqueue gone: neither is a
    * failure. Should the descriptor have been reused by then, the worst this does is wake another kqueue once.
    */
  def wake(): Unit =
    if !closed then
      try KqueueLoop.createAndRegisterEvents(kq, 1)(events => KqueueLoop.triggerUser(events(0), WakeEvent))
      catch case _: java.io.IOException if closed => ()

  protected def waitForDeadline(deadlineTask: Long): Unit = {
    val now = System.nanoTime()
    // A wake-up is reported once, and the worker's budgets may have left work behind after the one that came with it:
    // then, as when a deadline is already due, this only looks at what the kqueue has, and does not wait.
    val timeout =
      if !isEmpty() then 0L
      else if deadlineTask >= 0 then Math.max(deadlineTask - now, 0)
      else -1L
    val polled =
      if timeout == 0 then KqueueLoop.pollEventsNow(kq, events, maxEvents)
      else if timeout > 0 then
        KqueueLoop.pollEventsTimeout(
          kq,
          events,
          maxEvents,
          (timeout / 1_000_000_000L).toInt,
          (timeout % 1_000_000_000L).toInt
        )
      else KqueueLoop.pollEventsForever(kq, events, maxEvents)
    var i = 0
    while i < polled do
      val event = events(i)
      // A wake-up has nothing to hand over: the worker looks at its inbox and timers as `waitForDeadline` returns.
      if KqueueLoop.isReadEvent(event) then dispatch(KqueueLoop.fileIdent(event), read = true)
      else if KqueueLoop.isWriteEvent(event) then dispatch(KqueueLoop.fileIdent(event), read = false)
      i += 1
  }

  override def close(): Unit = {
    closed = true
    KqueueLoop.close(kq)
    zone.close()
  }
}
