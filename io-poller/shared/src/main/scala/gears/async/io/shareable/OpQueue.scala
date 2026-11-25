package gears.async.io.shareable

import gears.async.Async
import gears.async.Listener
import gears.async.io.Interface
import gears.async.io.shareable.TimerHeapOp.Outcome
import gears.async.io.shareable.WorkpoolScheduler.AsyncOp
import gears.async.io.shareable.WorkpoolScheduler.ReentrantOp

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

/** One future's queue of ops on `pool` (see `Batch`): those added wait here until the future's next `perform`, which
  * begins each on its worker. One done as it is begun is done there and then; one that has to wait stays, with the
  * queue as its waiter, and whoever settles it, a poller, a timer, a close, leaves it in `arrived` for the future's
  * next `perform` to find. So the future is suspended once a `perform` at most, and only if nothing it had queued is
  * done: by an arrival, of whichever op.
  *
  * All but `arrived` and `waiter` is the future's alone.
  */
private[io] final class OpQueue[I <: Interface { type Op <: AsyncOp & ReentrantOp }](pool: WorkpoolScheduler[I])
    extends Listener[Outcome] {
  val lock: Listener.ListenerLock | Null = null

  // Added and not begun.
  private var fresh = new Array[AnyRef | Null](16)
  private var freshNotes = new Array[AnyRef | Null](16)
  private var freshLimits = new Array[Long](16)
  private var freshSize = 0

  /** Begun and waiting, each with its note (or itself, for none). */
  private val waiting = new java.util.IdentityHashMap[AnyRef, AnyRef]()

  // Done in the last `perform`.
  private var done = new Array[AnyRef | Null](16)
  private var doneNotes = new Array[AnyRef | Null](16)
  private var doneFailures = new Array[Throwable | Null](16)
  private var doneSize = 0

  /** The ops settled while they waited, by whoever settled them. */
  private val arrived = new ConcurrentLinkedQueue[AnyRef]()

  /** The future's listener while it is suspended for an arrival. */
  private val waiter = new AtomicReference[Listener[Unit] | Null](null)

  private object arrival extends Async.Source[Unit] {
    def poll(k: Listener[Unit]): Boolean = !arrived.isEmpty() && k.completeNow((), this)

    def onComplete(k: Listener[Unit]): Unit = {
      waiter.set(k)
      // One that arrived before the listener was there told nobody.
      if !arrived.isEmpty() && waiter.compareAndSet(k, null) then k.completeNow((), this): Unit
    }

    def dropListener(k: Listener[Unit]): Unit = waiter.compareAndSet(k, null): Unit
  }

  /** An op that waited is settled: on whichever thread, which may hold its handle's lock. */
  def complete(data: Outcome, source: Async.Source[Outcome]): Unit = {
    arrived.add(source): Unit
    val waiting = waiter.getAndSet(null)
    if waiting != null then waiting.completeNow((), arrival): Unit
  }

  def size: Int = freshSize + waiting.size()

  def add(op: AnyRef, note: AnyRef | Null, limitMillis: Long): Unit = {
    if freshSize == fresh.length then {
      fresh = java.util.Arrays.copyOf(fresh, freshSize * 2)
      freshNotes = java.util.Arrays.copyOf(freshNotes, freshSize * 2)
      freshLimits = java.util.Arrays.copyOf(freshLimits, freshSize * 2)
    }
    fresh(freshSize) = op
    freshNotes(freshSize) = note
    freshLimits(freshSize) = limitMillis
    freshSize += 1
  }

  def doneCount: Int = doneSize
  def doneOp(i: Int): AnyRef = done(i).nn
  def doneNote(i: Int): AnyRef | Null = doneNotes(i)
  def doneFailure(i: Int): Throwable | Null = doneFailures(i)

  private def record(op: AnyRef, note: AnyRef | Null, failure: Throwable | Null): Unit = {
    if doneSize == done.length then {
      done = java.util.Arrays.copyOf(done, doneSize * 2)
      doneNotes = java.util.Arrays.copyOf(doneNotes, doneSize * 2)
      doneFailures = java.util.Arrays.copyOf(doneFailures, doneSize * 2)
    }
    done(doneSize) = op
    doneNotes(doneSize) = note
    doneFailures(doneSize) = failure
    doneSize += 1
  }

  private def finish(worker: pool.PoolWorker, op: pool.Op)(using Async): Unit =
    try worker.endOp(op)
    finally
      try op.clearTimeout()
      finally op.requestExit()

  /** Begins the ops added, whether the worker is due its other work. */
  private def beginFresh(worker: pool.PoolWorker)(using Async): Boolean = {
    var spent = false
    var i = 0
    val count = freshSize
    while i < count do {
      val op = fresh(i).asInstanceOf[pool.Op]
      val note = freshNotes(i)
      fresh(i) = null
      freshNotes(i) = null
      if worker.opBudgetSpent() then spent = true
      var isDone = false
      var begun = false
      var failure: Throwable | Null = null
      try {
        op.requestEntry()
        try {
          op.setTimeout(freshLimits(i))
          isDone = worker.beginOp(op)
          begun = true
        } finally if !begun || isDone then finish(worker, op)
      } catch case thrown: Throwable => failure = thrown
      if failure != null || isDone then record(op, note, failure)
      else {
        waiting.put(op, if note == null then op else note): Unit
        op.waitWith(this) // which may tell the queue at once: it is found in `arrived`
      }
      i += 1
    }
    freshSize = 0
    spent
  }

  private def takeArrived(worker: pool.PoolWorker)(using Async): Unit = {
    var next = arrived.poll()
    while next != null do {
      val op = next.asInstanceOf[pool.Op]
      val note = waiting.remove(op)
      var failure: Throwable | Null = null
      try {
        finish(worker, op)
        op.takeOutcome()
      } catch case thrown: Throwable => failure = thrown
      record(op, if note eq op then null else note, failure)
      next = arrived.poll()
    }
  }

  private def worker(): pool.PoolWorker = {
    val on = pool.currentWorker
    if on == null then
      throw new UnsupportedOperationException("A batch is performed by a future on one of the pool's workers")
    on
  }

  /** Begins what was added, and collects what is done: suspended, once, only if nothing is and something waits. */
  def perform()(using Async): Int = {
    var i = 0
    while i < doneSize do {
      done(i) = null
      doneNotes(i) = null
      doneFailures(i) = null
      i += 1
    }
    doneSize = 0
    // An always-ready queue never gives its worker up otherwise.
    if beginFresh(worker()) then pool.`yield`()
    takeArrived(worker())
    if doneSize == 0 && !waiting.isEmpty() then {
      // What the ops wait for may be there already, known to the system and not yet to the worker: it is asked here,
      // which the worker would do as soon as the future was suspended, and which then need not suspend it.
      // Only with nothing else for the worker to do: with other futures to run, suspending lets them, and the worker
      // then asks once for all of them.
      val on = worker()
      if !on.hasOtherWork then
        on.lookForEvents()
        takeArrived(on)
    }
    while doneSize == 0 && !waiting.isEmpty() do {
      Parking.parkOn(arrival)
      takeArrived(worker())
    }
    doneSize
  }

  /** Ends every op of the queue: those not begun are dropped, and those that wait are cancelled and waited for. */
  def close()(using Async): Unit = {
    freshSize = 0
    val ops = waiting.keySet().toArray()
    var i = 0
    while i < ops.length do {
      ops(i).asInstanceOf[pool.Op].cancelByPerformer(): Unit
      i += 1
    }
    while !waiting.isEmpty() do {
      if arrived.isEmpty() then Parking.parkUncancelled(arrival)
      takeArrived(worker())
    }
    doneSize = 0
  }
}
