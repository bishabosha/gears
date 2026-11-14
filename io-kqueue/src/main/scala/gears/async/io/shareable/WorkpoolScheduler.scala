package gears.async.io.shareable

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Listener
import gears.async.Scheduler
import gears.async.SuspendSupport
import gears.async.Suspension
import gears.async.cancellationScope
import gears.async.io.Backend
import gears.async.io.Interface
import gears.async.io.Io
import gears.async.io.shareable.TimerHeap
import gears.async.io.shareable.TimerHeap.{TimerOp, TimerSource}
import gears.async.io.shareable.WorkpoolScheduler.DoAfter
import gears.async.native.NativeSuspend

import java.io.IOException
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import WorkpoolScheduler.{AsyncOp, ReentrantOp, Worker}

abstract class WorkpoolScheduler[Op1 <: AsyncOp & ReentrantOp, I <: Interface { type Op = Op1 }](
    val backend: Backend { type Interface <: I },
    val ops: I,
    workers: Int
) extends Scheduler
    with AsyncSupport
    with AsyncOperations
    with Io[I] {

  override private[async] def resumeAsync[T, R](suspension: Suspension[T, R])(arg: T)(using s: Scheduler): Unit =
    // Not here and now: the caller holds the listener's lock, and may hold a source's, partway through changing it.
    val worker = workerOf(Thread.currentThread())
    if worker != null then worker.resumeNext(suspension, arg)
    // A pool that has shut down refuses it: like a timer still pending, that waiter is never resumed.
    else nextWorker().submit(() => suspension.resume(arg)): Unit

  /** Schedule a computation with the suspension boundary already created. */
  // override private[async] def scheduleBoundary(body: Label[Unit] ?=> Unit)(using s: Scheduler): Unit =
  //   s.execute(() => boundary(body))

  def makeWorker(id: Int): Worker[Op1]

  private val workerTicker = new java.util.concurrent.atomic.AtomicInteger(0)
  private val workerGroup: Array[Worker[Op1]] = Array.tabulate(workers)(makeWorker)

  /** The worker of this pool that `thread` runs, or null: a worker's thread carries its worker, so this is no lookup
    * by thread.
    */
  private def workerOf(thread: Thread): Worker[Op1] | Null = thread match
    case thread: WorkpoolScheduler.WorkerThread =>
      val worker = thread.worker
      if worker.id < workerGroup.length && (workerGroup(worker.id) eq worker) then worker.asInstanceOf[Worker[Op1]]
      else null // another pool's
    case _ => null

  /** The group the program runs under (see `Backend.run`), which a fatal error in any worker cancels: the workers go
    * on, as they must for the program to unwind, but nothing should be built on what a fatal error left behind.
    */
  val rootGroup: CompletionGroup = CompletionGroup()

  private val firstFatal = new java.util.concurrent.atomic.AtomicReference[Throwable | Null](null)

  /** The first fatal error a worker met, or null. Whoever runs the program rethrows it once the program has ended. */
  final def fatalFailure: Throwable | Null = firstFatal.get()

  private def failFatally(failure: Throwable): Unit = {
    firstFatal.compareAndSet(null, failure): Unit
    rootGroup.cancel()
  }

  workerGroup.foreach { worker =>
    worker.onFatal = failFatally
    worker.steal = () => stealFor(worker)
    worker.callThief = () => wakeThief(worker)
  }
  // Only now: a worker's thread started as the worker was made would run it before its subclass had its fields.
  workerGroup.foreach(_.thread.start())
  private def nextWorker(): Worker[Op1] = {
    val index = Math.floorMod(workerTicker.getAndIncrement(), workerGroup.length)
    workerGroup(index)
  }
  protected final def workerOfCurrentThread(): Worker[Op1] =
    val worker = workerOf(Thread.currentThread())
    if worker == null then throw new IllegalStateException("Current thread is not a worker")
    worker

  /** The worker for an op, whose heap holds its deadline, and which fires it. An op that `tryPerform` completes must
    * not be firing when it returns, which holds for the performer's own worker: only its thread fires its timers.
    */
  protected def selectWorker(): Worker[Op1]

  type Scheduler = this.type

  /** An op that `tryPerform` completes is complete, whatever its state says: that state is neither read nor waited for.
    * Any other is parked on, and its own outcome is the performance's, not what the wait made of it: a performer
    * cancelled while a worker was already doing the op's work waits for that work and returns its outcome, as the work
    * was done. Its cancel still holds, at its next `await`.
    */
  override def performWithin(op: Op, timeoutMillis: Long)(using Async): Unit = {
    op.performWith {
      var worker = selectWorker()
      // An op that is done as it is tried never gives its worker up: a future whose ops are always ready, a read at
      // the end of a stream for one, would keep it from every other task, its timers and its handles' events.
      if worker.opBudgetSpent() then
        `yield`()
        worker = selectWorker() // the future may go on elsewhere
      op.setTimeout(timeoutMillis, worker)
      try {
        worker.performOp(op)
      } finally {
        op.clearTimeout()
      }
    }
  }

  /** Guards `execute` and `schedule`: work given to a pool that has shut down would never run. */
  @volatile private var isShutDown = false

  private def checkRunning(): Unit =
    if isShutDown then throw new IllegalStateException("Scheduler has shut down")

  /** Stops the workers and waits for them to finish what they are running, which may be the end of the program, as a
    * worker resumes its waiters itself. Timers still pending never fire. Call once the program has finished, from
    * outside the pool: a worker can't wait for itself.
    */
  final def shutdown(): Unit = {
    if workerOf(Thread.currentThread()) != null then
      throw new IllegalStateException("Can't shut down a scheduler from one of its workers")
    isShutDown = true
    workerGroup.foreach(_.stop())
    workerGroup.foreach(_.thread.join())
  }

  /** A task from one of this pool's workers stays with that worker, behind what it already has: what a future spawns
    * is most often to do with what it is doing, its handles among it, and a handle is cheapest used from one thread.
    * It is not tied there: a worker with nothing to do takes it (`stealFor`). A task from any other thread goes to the
    * workers in turn.
    */
  override def execute(body: Runnable): Unit =
    checkRunning()
    val current = workerOf(Thread.currentThread())
    if current != null then current.spawnLocal(body)
    // The check alone can pass just before the workers leave.
    else if !nextWorker().submit(body) then throw new IllegalStateException("Scheduler has shut down")

  /** A task for `thief`, which has nothing to do, from another worker's inbox, the next worker's first: or null. */
  private def stealFor(thief: Worker[Op1]): Runnable | Null = {
    val count = workerGroup.length
    var task: Runnable | Null = null
    var i = 1
    while task == null && i < count do
      task = workerGroup((thief.id + i) % count).takeForThief()
      i += 1
    task
  }

  /** Wakes one worker that is waiting with nothing to do, if there is one, for it to take from `busy`. */
  private def wakeThief(busy: Worker[Op1]): Unit = {
    val count = workerGroup.length
    var i = 1
    while i < count do
      val worker = workerGroup((busy.id + i) % count)
      if worker.wakeIfParked() then i = count
      else i += 1
  }

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    checkRunning()
    val worker = nextWorker()
    val timerOp = new WorkpoolScheduler.DoAfter(worker, TimerHeap.deadlineAfter(delay.toNanos), body)
    worker.addTimer(timerOp)
    timerOp
  }

}

object WorkpoolScheduler {

  trait AsyncOp extends TimerOp {
    def setTimeout(timeoutMillis: Long, source: TimerSource[?]): Unit

    /** Suspends until the op is settled, and returns or throws its outcome. Only the op ends the wait: cancelled
      * through the group it is linked to, it settles at once as cancelled if no worker has claimed it, and otherwise
      * keeps the performer waiting for the worker that claimed it, which should test for cancellation as soon as
      * possible or resolve with a complete outcome.
      */
    def parkUntilSettled()(using Async): Unit

    def clearTimeout(): Unit
  }

  val SuccessUnit: Try[Unit] = Success(())

  /** How many ops one worker performs between turns before the future performing the next yields. */
  inline val OpBudget = 64

  /** A worker's thread, which carries its worker for whoever asks which worker the current thread is. */
  final class WorkerThread(val worker: Worker[?], body: Runnable) extends Thread(body)

  trait ReentrantOp extends Cancellable {

    /** The op is not linked to the performer's group here: it links itself for as long as it waits, in
      * `parkUntilSettled`, and an op that is done as it is tried never does.
      */
    inline final def performWith(inline fn: Async ?=> Unit)(using Async): Unit =
      requestEntry()
      try fn
      finally requestExit()

    /** Aquires an entry permit. */
    def requestEntry()(using Async): Unit

    /** Potentially resets the entry permit. The op is unlinked by then. */
    def requestExit()(using Async): Unit
  }

  final class DoAfter(val owner: Worker[?], val deadlineNow: Long, body: Runnable) extends TimerOp {
    @volatile private var cancelled = false
    def timerWakeup(actAfter: Long): Unit = {
      if !cancelled then body.run()
    }
    def cancel(): Unit = {
      cancelled = true
      owner.removeTimer(this): Unit
    }

    /** called under the heap's lock when retrieved. If true then the heap now owns the claim for this timer. */
    def claimAtDeadline(): Boolean = !cancelled
  }

  abstract class Worker[Op1 <: TimerOp](val id: Int)
      extends TimerHeapImpl[Op1 | DoAfter],
        EventSource {

    type ThisWorkerSource <: EventSource
    def asThisWorkerSource: this.type & ThisWorkerSource

    // private[kqueue] val inbox: java.util.concurrent.ArrayBlockingQueue[Runnable] =
    //   new java.util.concurrent.ArrayBlockingQueue[Runnable](1024)
    private val inbox: java.util.concurrent.ConcurrentLinkedQueue[Runnable] =
      new java.util.concurrent.ConcurrentLinkedQueue[Runnable]

    /** Whether the worker has taken `task`, which it refuses once it has left `loop`. */
    def submit(task: Runnable): Boolean = {
      println(
        s"submit called on worker $id on thread ${Thread.currentThread().getName()}"
      )
      queued.incrementAndGet()
      inbox.add(task)
      wake()
      !exited || !takeBack(task)
    }

    /** A task from this worker's own thread: no wake-up, as the worker is running, and comes to its inbox in turn. */
    private[WorkpoolScheduler] def spawnLocal(task: Runnable): Unit = {
      // One task, or a pair, which most often work with each other, the worker comes to as soon as what it is running
      // lets go. A third behind them is worth another worker's while, and is what gets a thief called if what is
      // running keeps the worker.
      val behindOthers = queued.getAndIncrement() > 1
      inbox.add(task)
      if behindOthers then askForThief()
    }

    /** How many tasks are in the inbox, or about to be: what tells one task waiting its turn from a backlog. */
    private val queued = new java.util.concurrent.atomic.AtomicInteger(0)

    /** The next task in the inbox, for this worker or a thief, or null. */
    private def takeNext(): Runnable | Null = {
      val task = inbox.poll()
      if task != null then queued.decrementAndGet()
      task
    }

    private def takeBack(task: Runnable): Boolean = {
      val removed = inbox.remove(task)
      if removed then queued.decrementAndGet()
      removed
    }

    /** For a worker with nothing to do: a task of this one's, which it has not come to yet, if it has more than it
      * will come to soon (`surplus`). A worker that is merely awake does not take what another is about to run.
      */
    private[WorkpoolScheduler] def takeForThief(): Runnable | Null = if surplus then takeNext() else null

    /** Whether this worker has tasks to spare: set as it calls for a thief, and until a turn leaves its inbox empty. */
    @volatile private var surplus = false

    /** Set by the scheduler before the worker's thread starts: a task from another worker's inbox, or null. */
    private[WorkpoolScheduler] var steal: () => Runnable | Null = () => null

    /** Set by the scheduler before the worker's thread starts: wakes a worker that waits with nothing to do. */
    private[WorkpoolScheduler] var callThief: () => Unit = () => ()

    /** Whether this worker has called for a thief this turn. Worker thread only. One is called for at most once a
      * turn, when the worker is given a task it will not come to at once or ends a turn with tasks left over: so that
      * what one worker can keep up with stays on it, and what it can't, or what waits behind a task that keeps it, is
      * shared out a worker at a time.
      */
    private var thiefAsked = false

    private def askForThief(): Unit =
      surplus = true
      if !thiefAsked then
        thiefAsked = true
        callThief()

    /** Whether the worker is waiting, or about to, with nothing to do. */
    @volatile private var parked = false

    private[WorkpoolScheduler] def wakeIfParked(): Boolean =
      if parked then
        parked = false // so that the next call wakes another
        wake()
        true
      else false

    /** How many ops have been performed on this worker since it last began a turn or had a future yield: an op that
      * waits ends the run of whoever performs it, so only ops done as they are tried add up. Worker thread only.
      */
    private var opsThisRun = 0

    /** Whether the future performing an op now should yield first: every `OpBudget` ops without a turn between. */
    private[WorkpoolScheduler] def opBudgetSpent(): Boolean =
      opsThisRun += 1
      if opsThisRun > WorkpoolScheduler.OpBudget then
        opsThisRun = 0
        true
      else false

    /** Worker thread only: makes `isEmpty` false for a poll that must not wait. */
    private var pollOnly = false

    /** Once set, the worker finishes its inbox and leaves `loop`. */
    @volatile private var stopped = false

    /** Set as the worker leaves `loop`, for `submit` to take back a task that came too late for it. */
    @volatile private var exited = false

    private[WorkpoolScheduler] def stop(): Unit = {
      stopped = true
      wake()
    }

    private class Continuation[T, R](val suspension: Suspension[T, R], val value: T):
      def run(): Unit = {
        val _ = suspension.resume(value)
      }

    /** Worker thread only: continuations to run as soon as the listener resuming them has returned, ahead of the rest
      * of the timers and the inbox. Declared before `thread`, which reads it from the moment it starts.
      */
    private val ready = new java.util.ArrayDeque[Continuation[?, ?]]

    private[WorkpoolScheduler] def resumeNext[T, R](suspension: Suspension[T, R], value: T): Unit =
      println(
        s"resumeNext called on worker $id on thread ${Thread.currentThread().getName()}"
      )
      ready.addLast(Continuation(suspension, value))

    private[WorkpoolScheduler] val thread = {
      val t = new WorkerThread(this, () => loop())
      t.setName(s"WorkpoolSchedulerWorker-$id")
      t.setDaemon(true)
      t
    }

    /** Does the core work of performing an operation, when it returns the operation is guaranteed to have been
      * processed.
      */
    def performOp(op: Op1)(using Async): Unit

    def performClose(op: Op1 & AbstractCloseOp)(using Async): Unit =
      var shouldWait = false
      if op.hasDeadline then
        addTimer(op)
        shouldWait = true
      if !op.pollClose() then shouldWait = true
      if shouldWait then op.parkUntilSettled()

    /** Does a read or a write at once, or has it wait for its handle to be ready (see `performWhenReady`). */
    def performWhenReady(op: Op1 & AbstractReadinessOp[?, ThisWorkerSource])(using Async): Unit =
      op.performWhenReady(asThisWorkerSource)

    def isEmpty(): Boolean = inbox.isEmpty() && ready.isEmpty() && !pollOnly

    /** A worker outlives what it runs, as event loops do: it can't be replaced, and its timers and inbox would be lost
      * with it. What a task throws, of any kind, is reported to the thread's uncaught exception handler, as it would be
      * had it ended the thread, so that a program can install its own; by default that prints it. A fatal error also
      * cancels the program (see `rootGroup`).
      */
    private inline def guarded(inline body: Unit): Unit =
      try body
      catch
        case failure: Throwable =>
          reportFailure(failure)
          if !NonFatal(failure) then onFatal(failure)

    /** Set by the scheduler before the worker's thread starts: what a fatal error does beyond being reported. */
    private[WorkpoolScheduler] var onFatal: Throwable => Unit = _ => ()

    private def reportFailure(failure: Throwable): Unit = {
      val current = Thread.currentThread()
      // A handler that throws must not take the worker with it either.
      try current.getUncaughtExceptionHandler().uncaughtException(current, failure)
      catch case _: Throwable => failure.printStackTrace()
    }

    /** Has a budget so that tasks resuming each other can't keep the worker from its timers and inbox. */
    private def runReady(): Unit = {
      var readyBudget = 32
      while readyBudget > 0 do
        val continuation = ready.pollFirst()
        if continuation == null then readyBudget = 0
        else
          guarded(continuation.run())
          readyBudget -= 1
    }

    /** Each timer's waiter runs before the next timer leaves the heap, so that it can still move or cancel that one. */
    private def fireTimers(now: Long): Unit = {
      var timerBudget = 32
      while timerBudget > 0 do
        val timer = nextReadyTimer(now)
        if timer == null then timerBudget = 0
        else
          guarded(timer.timerWakeup(now))
          runReady()
          timerBudget -= 1
    }

    def drainInbox(): Unit = {
      var inboxBudget = 32
      while inboxBudget > 0 do
        val task = takeNext()
        if task == null then inboxBudget = 0
        else
          guarded(task.run())
          runReady()
          inboxBudget -= 1
    }

    def loop(): Unit = {
      while !stopped || !isEmpty() do
        val now = System.nanoTime()
        fireTimers(now)
        runReady()
        drainInbox()
        // `stop`'s wake-up may have been spent while the inbox was still draining.
        if !stopped then
          // More than a turn's worth, and more than the task or two that a turn's budget may leave for the next.
          if queued.get() > 2 then askForThief()
          else if surplus then surplus = false
          val stolen = if isEmpty() then steal() else null
          if stolen != null then
            guarded(stolen.run())
            runReady()
            // Its own handles' events still get their turn, without waiting: there may be more to take.
            pollOnly = true
            try guarded(waitForDeadline(peekNextDeadline()))
            finally pollOnly = false
          else
            parked = isEmpty()
            try guarded(waitForDeadline(peekNextDeadline()))
            finally parked = false
        thiefAsked = false
        opsThisRun = 0
      exited = true
      // A task added before `exited` showed is either run here or taken back by `submit`, never lost.
      while !isEmpty() do
        drainInbox()
        runReady()
      close()
    }
  }
}
