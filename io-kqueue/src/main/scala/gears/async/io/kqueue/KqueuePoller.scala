package gears.async.io.kqueue

import gears.async.io.kqueue.internal.KqueueLoop
import gears.async.io.shareable.EventSource
import gears.async.io.shareable.Poller
import gears.async.io.shareable.ReadinessOp

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

  /** Guards the filters together with whose they are: a filter is on the kqueue only while a handle has an op
    * waiting on this worker, and both change as one step, as a waiter can be cleared from another thread than this
    * worker's (by a close, or by a performer that a cancel resumed elsewhere).
    */
  private val filtersLock = new Object

  /** The handle with a filter on this kqueue, by descriptor number, which is all an event names. The waiters are the
    * handle's own. A handle may stay here after its last filter went, until its number is another's: an event that
    * finds it finds no waiter.
    */
  private var handles = new Array[KqueueHandle | Null](64)

  /** One-shot and level-triggered: reported once if `fd` is ready now or when it next is, then gone, so that a
    * descriptor nobody is waiting on any more costs nothing. `dispatch` arms it again for an op still waiting.
    */
  private def arm(fd: Int, read: Boolean): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { changes =>
      KqueueLoop.addFile(changes(0), fd, read, clear = false, oneShot = true)
    }

  /** Makes `op` the waiter of `handle`, which may refuse it, and has the kqueue report when it is readable, or
    * writable.
    */
  final def registerWaiter(handle: KqueueHandle, read: Boolean, op: ReadinessOp): Unit = filtersLock.synchronized {
    val fd = handle.fd
    handle.setWaiter(read, op)
    try
      if fd >= handles.length then handles = java.util.Arrays.copyOf(handles, Math.max(fd + 1, 2 * handles.length))
      handles(fd) = handle
      arm(fd, read)
    catch
      case failure: Throwable =>
        handle.clearWaiter(read, op): Unit
        throw failure
  }

  /** Takes `op` back from `handle`, and its filter off the kqueue. An op that is no longer the handle's waiter,
    * because it gave way, was refused or was already cleared, changes nothing: the filter is then another op's.
    */
  final def clearWaiter(handle: KqueueHandle, read: Boolean, op: ReadinessOp): Unit = filtersLock.synchronized {
    if handle.clearWaiter(read, op) then
      val fd = handle.fd
      if !closed then KqueueLoop.deleteFileIfOpen(kq, fd, read): Unit
      if (handles(fd) eq handle) && !handle.hasWaiter(!read) then handles(fd) = null
  }

  /** `fd` is ready: its handle's waiter does its work, or, finding it not ready after all, waits on and has the
    * kqueue report `fd` again. A waiter that can't be claimed was settled some other way.
    */
  private def dispatch(fd: Int, read: Boolean): Unit = {
    val handle = filtersLock.synchronized(if fd < handles.length then handles(fd) else null)
    val op = if handle == null then null else handle.claimWaiter(read)
    if op != null && !op.readyWakeup() then
      filtersLock.synchronized {
        // The claim kept the op its handle's waiter. If a cancel settles it here, its performer clears it.
        if op.releaseReady() then arm(fd, read)
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
