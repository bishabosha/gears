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

import WorkpoolScheduler.{AsyncOp, ReentrantOp}

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
    val thread = Thread.currentThread()
    val worker = workerOf(thread)
    if worker != null then
      // Where whoever is resuming it says a pinned future belongs (`resumingAt`). Any other future is resumed here,
      // and may be given to a worker with nothing to do before it is (`Worker.shareUnpinned`).
      val home = thread.asInstanceOf[WorkpoolScheduler.WorkerThread].resumeHome
      if home == null then worker.resumeNext(suspension, arg, pinned = false)
      else if home eq worker then worker.resumeNext(suspension, arg, pinned = true)
      else home.asInstanceOf[Worker[Op1]].submit(() => suspension.resume(arg)): Unit
    // A pool that has shut down refuses it: like a timer still pending, that waiter is never resumed.
    else nextWorker().submit(() => suspension.resume(arg)): Unit

  /** Schedule a computation with the suspension boundary already created. */
  // override private[async] def scheduleBoundary(body: Label[Unit] ?=> Unit)(using s: Scheduler): Unit =
  //   s.execute(() => boundary(body))

  def makeWorker(id: Int): Worker[Op1]

  private val workerTicker = new java.util.concurrent.atomic.AtomicInteger(0)
  private val workerGroup: Array[Worker[Op1]] = Array.tabulate(workers)(makeWorker)

  /** The worker of this pool that `thread` runs, or null: a worker's thread carries its worker, so this is no lookup by
    * thread.
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
    worker.idleExists = () => anyParked()
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
      op.pinnedTo(summon[Async] match
        case pinned: PinnedAsync => pinned.home
        case _                   => null)
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

  /** A task from one of this pool's workers stays with that worker, behind what it already has: what a future spawns is
    * most often to do with what it is doing, its handles among it, and a handle is cheapest used from one thread. It is
    * not tied there: a worker with nothing to do takes it (`stealFor`). A task from any other thread goes to the
    * workers in turn.
    */
  override def execute(body: Runnable): Unit =
    checkRunning()
    val current = workerOf(Thread.currentThread())
    if current != null then current.spawnLocal(body)
    // The check alone can pass just before the workers leave.
    else if !nextWorker().submit(body) then throw new IllegalStateException("Scheduler has shut down")

  /** A task for `thief`, which has nothing to do, to run now, with half of what the worker it came from had to spare
    * moved to the thief's inbox: from the next worker with tasks to spare, or null.
    */
  private def stealFor(thief: Worker[Op1]): Runnable | Null = {
    val count = workerGroup.length
    var task: Runnable | Null = null
    var i = 1
    while task == null && i < count do
      task = workerGroup((thief.id + i) % count).takeHalfFor(thief)
      i += 1
    task
  }

  private def anyParked(): Boolean = {
    var i = 0
    var found = false
    while !found && i < workerGroup.length do
      found = workerGroup(i).isParked
      i += 1
    found
  }

  /** `Io.pinned` on a pool: the future stays on the worker it is on while `body` runs. Whatever resumes it, it goes on
    * there and nowhere else: an op of this pool's resumes it there itself, and anything else it waits for is followed
    * by a move back. Outside any such scope a future is resumed where its op was done, and from there may be taken by a
    * worker with nothing to do (`Worker.shareUnpinned`); as `body` ends the future offers itself to one, if there is
    * one. A scope inside another is already pinned, and does neither.
    */
  override def pinned[T](body: Async ?=> T)(using async: Async): T =
    async match
      case _: PinnedAsync => body
      case _              =>
        try body(using PinnedAsync(async, workerOfCurrentThread()))
        finally offer(async)

  /** Moves the future to `home`, if it is not there. Under no group: a cancel must not leave it somewhere else. */
  private def goTo(base: Async, home: Worker[Op1]): Unit =
    if workerOf(Thread.currentThread()) ne home then base.withGroup(CompletionGroup.Unlinked).await(Hop(home))

  /** Has a worker with nothing to do take the future, if there is one; otherwise it goes on where it is. */
  private def offer(base: Async): Unit = {
    val worker = workerOf(Thread.currentThread())
    if worker != null && anyParked() then base.withGroup(CompletionGroup.Unlinked).await(Offer(worker))
  }

  /** The context of a future inside `pinned`: whatever it waits for, it goes on at `home`. */
  private final class PinnedAsync(val outer: Async, val home: Worker[Op1])
      extends Async(using outer.support, outer.scheduler) {
    def group: CompletionGroup = outer.group
    def withGroup(group: CompletionGroup): Async = PinnedAsync(outer.withGroup(group), home)
    def await[T](src: Async.Source[T]): T =
      // An op of this pool's resumes it at home itself (`resumingAt`). Anything else may resume it elsewhere.
      try outer.await(src)
      finally goTo(outer, home)
  }

  /** Completes its one listener on `home`'s thread, which resumes the future awaiting it there. */
  private final class Hop(home: Worker[Op1]) extends Async.Source[Unit] {
    def poll(k: Listener[Unit]): Boolean = false
    def onComplete(k: Listener[Unit]): Unit =
      home.submit(() => WorkpoolScheduler.resumingAt(home)(k.completeNow((), this): Unit)): Unit
    def dropListener(k: Listener[Unit]): Unit = ()
  }

  /** Completes its one listener from a task of `worker`'s that any worker with nothing to do may take, and that
    * `worker` runs itself if none does: the future awaiting it goes on wherever that is.
    */
  private final class Offer(worker: Worker[Op1]) extends Async.Source[Unit] {
    def poll(k: Listener[Unit]): Boolean = false
    def onComplete(k: Listener[Unit]): Unit = worker.offer(() => k.completeNow((), this): Unit)
    def dropListener(k: Listener[Unit]): Unit = ()
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

    /** The worker the future performing the op is pinned to, or null: set for each performance. */
    def pinnedTo(home: AnyRef | Null): Unit
  }

  val SuccessUnit: Try[Unit] = Success(())

  /** How many ops one worker performs between turns before the future performing the next yields. */
  inline val OpBudget = 64

  /** A worker's thread, which carries its worker for whoever asks which worker the current thread is. */
  final class WorkerThread(val worker: Worker[?], body: Runnable) extends Thread(body) {

    /** While this thread completes the listener of a pinned future: the worker the future is pinned to, which is where
      * the scheduler then resumes it. Null otherwise. This thread's alone.
      */
    var resumeHome: AnyRef | Null = null
  }

  /** Runs `complete`, which completes a listener of a future pinned to `home`: a worker's thread notes `home` for the
    * scheduler's `resumeAsync`, which comes within. On any other thread the future is resumed on some worker, and finds
    * its own way home (`PinnedAsync.await`).
    */
  inline def resumingAt(home: AnyRef)(inline complete: Unit): Unit =
    Thread.currentThread() match
      case thread: WorkerThread =>
        thread.resumeHome = home
        try complete
        finally thread.resumeHome = null
      case _ => complete

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
}
