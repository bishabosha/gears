package gears.async.io.shareable

import gears.async.Async
import gears.async.Future
import gears.async.GearsCancellationException
import gears.async.Listener
import gears.async.io.Interface
import gears.async.io.flow.Flow
import gears.async.io.shareable.TimerHeapOp.Outcome
import gears.async.io.shareable.WorkpoolScheduler.AsyncOp
import gears.async.io.shareable.WorkpoolScheduler.ReentrantOp

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** One future's flows as they run on `pool` (see `Flow`): each a `Strand`, a place in its flow's steps and the op it
  * waits for, if any. A strand runs its steps on a worker until it comes to an op that has to wait, of which it makes
  * itself the waiter; whoever settles the op has the strand queued here, and the run is given to that worker to go on
  * with, as a future it resumed would be. No stack is kept for a strand, so nothing is suspended and nothing resumed.
  *
  * The strands of one run take turns: `turns` counts those queued, and whoever raises it from nothing runs them all, so
  * only one thread is in the run at a time, and the flows may share the plain variables of the future they are run
  * from.
  */
private[io] final class FlowRun[I <: Interface { type Op <: AsyncOp & ReentrantOp }](
    pool: WorkpoolScheduler[I],
    flows: Array[Flow[?]]
)(using async: Async)
    extends Runnable {

  /** Null, then the listener of the future that waits for the run, if it does, then the run's outcome: what the run is
    * as a source, with the one listener.
    */
  private val outcome = new java.util.concurrent.atomic.AtomicReference[AnyRef | Null](null)

  private object ended extends Async.Source[Try[Unit]] {
    def poll(k: Listener[Try[Unit]]): Boolean = outcome.get() match
      case result: Try[?] => k.completeNow(result.asInstanceOf[Try[Unit]], this)
      case _              => false

    def onComplete(k: Listener[Try[Unit]]): Unit = {
      var placed = false
      while !placed do
        outcome.get() match
          case result: Try[?] =>
            k.completeNow(result.asInstanceOf[Try[Unit]], this): Unit
            placed = true
          case other => placed = outcome.compareAndSet(other, k)
    }

    def dropListener(k: Listener[Try[Unit]]): Unit = outcome.compareAndSet(k, null): Unit
  }

  private def end(result: Try[Unit]): Unit = outcome.getAndSet(result) match
    case waiting: Listener[?] => waiting.asInstanceOf[Listener[Try[Unit]]].completeNow(result, ended): Unit
    case _                    => ()
  private val queue = new java.util.concurrent.ConcurrentLinkedQueue[Strand]()
  private val turns = new AtomicInteger(0)
  private val strands = flows.map(new Strand(_))

  // Read and written only by whoever is in the run.
  private var live = strands.length
  private var failure: Throwable | Null = null
  private var yielding = false

  /** Set by a failure of one strand, or by the cancel of the future the run is for: each strand ends at its next op. */
  @volatile private var cancelled = false

  private final class Strand(flow: Flow[?]) extends Listener[Outcome] {
    val lock: Listener.ListenerLock | Null = null

    private var pc = 0
    private var waiting = false

    /** The op being performed, for a cancel to end its wait. */
    @volatile var current: pool.Op | Null = null

    /** The op it waited for is settled: by its worker's poller, a timer, a close or a cancel, on whichever thread. */
    def complete(data: Outcome, source: Async.Source[Outcome]): Unit = enqueue(this)

    private def finish(worker: pool.PoolWorker, op: pool.Op): Unit = {
      current = null
      try worker.endOp(op)
      finally
        try op.clearTimeout()
        finally op.requestExit()
    }

    def step(worker: pool.PoolWorker): Unit =
      try {
        if waiting then {
          waiting = false
          val op = current.nn
          finish(worker, op)
          op.takeOutcome()
        }
        if interpret(worker) then strandEnded()
      } catch {
        case thrown: Throwable =>
          if failure == null then failure = thrown
          cancel()
          strandEnded()
      }

    /** Runs steps until one waits or the worker is due its other work: whether the flow has ended. */
    private def interpret(worker: pool.PoolWorker): Boolean = {
      val kinds = flow.kinds
      val fns = flow.fns
      val targets = flow.targets
      val end = kinds.length
      while pc < end do
        (kinds(pc): @scala.annotation.switch) match
          case Flow.Exec =>
            fns(pc).asInstanceOf[() => Unit]()
            pc += 1
          case Flow.Unless =>
            if fns(pc).asInstanceOf[() => Boolean]() then pc += 1 else pc = targets(pc)
          case Flow.Jump => pc = targets(pc)
          case _         =>
            if cancelled then throw new GearsCancellationException()
            if worker.opBudgetSpent() then {
              // As a future yields: the run goes behind the worker's other tasks, this strand first when it is back.
              queue.add(this): Unit
              turns.incrementAndGet(): Unit
              yielding = true
              return false
            }
            val op = fns(pc).asInstanceOf[() => pool.Op]()
            val timeout = flow.timeouts(pc)
            pc += 1
            op.requestEntry()
            // Before the op can wait: a cancel either sees it here, or is seen below.
            current = op
            var done = false
            var begun = false
            try {
              if cancelled then throw new GearsCancellationException()
              op.setTimeout(timeout)
              done = worker.beginOp(op)
              begun = true
            } finally if !begun || done then finish(worker, op)
            if !done then {
              waiting = true
              op.waitWith(this) // which may queue the strand at once: it is run once this has returned
              return false
            }
      true
    }
  }

  private def strandEnded(): Unit = {
    live -= 1
    if live == 0 then
      val thrown = failure
      end(if thrown == null then WorkpoolScheduler.SuccessUnit else Failure(thrown))
  }

  /** From any thread: the strands end at their next op, and those that wait for one stop waiting. */
  private def cancel(): Unit = {
    cancelled = true
    var i = 0
    while i < strands.length do
      val op = strands(i).current
      if op != null then op.cancelByPerformer(): Unit
      i += 1
  }

  private def enqueue(strand: Strand): Unit = {
    queue.add(strand): Unit
    if turns.getAndIncrement() == 0 then
      val worker = pool.currentWorker
      // Not here and now: whoever settled the op may hold its handle's lock.
      if worker != null then worker.runNext(this) else pool.execute(this)
  }

  /** The turns of the strands queued, on a worker, by whoever raised `turns` from nothing. */
  def run(): Unit = {
    val worker = pool.currentWorker.nn
    var more = true
    while more do
      queue.poll().nn.step(worker)
      if yielding then
        yielding = false
        turns.decrementAndGet(): Unit // not the last: the strand that yields is queued
        worker.spawnLocal(this)
        more = false
      else more = turns.decrementAndGet() != 0
  }

  /** Runs the flows, from the future they are for, on its worker, and waits for them if they wait. */
  def perform(): Unit = {
    if pool.currentWorker == null then
      throw new UnsupportedOperationException("Flows are run from a future on one of the pool's workers")
    if strands.length > 0 then
      var i = 0
      while i < strands.length do
        queue.add(strands(i)): Unit
        i += 1
      turns.set(strands.length)
      run()
      // Ended already, none of its ops having had to wait: nobody is suspended.
      val result = outcome.get() match
        case done: Try[?] => done
        case _            =>
          try Parking.parkOn(ended)
          catch
            case _: CancellationException =>
              cancel()
              Parking.parkUncancelled(ended)
      result.get: Unit
  }
}
