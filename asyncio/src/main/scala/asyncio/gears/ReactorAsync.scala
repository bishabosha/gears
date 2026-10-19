package asyncio.gears

import gears.async.Async
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Listener

import java.util.concurrent.CancellationException
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import asyncio.Backend
import asyncio.Completion
import asyncio.Reactor

/** What gears futures do with a program's reactors: perform ops, submit them, and yield, the same on every platform.
  * Each platform's `ReactorFutures.run` starts the program and its reactors: on a `ReactorPool`, whose workers run both
  * the futures and the reactors, so an op goes to the reactor of the worker running the caller; or, on the JVM by
  * default, beside gears' own scheduler, so that an op is handed to a reactor's thread (see `OpRouter`).
  */
object ReactorAsync {

  /** Submits `op`, which must have been built by `b.ops`, to a reactor of the caller's program, and suspends the
    * caller until the reactor finishes with it: it returns when the op completes, throws its failure, or throws
    * `CancellationException` when it is cancelled. It must be called from a program that `ReactorFutures.run` started
    * with `b`. `b` only says what the op is; which reactor runs it depends on where the caller runs: on a pool's worker,
    * that worker's, and otherwise one picked by the caller's thread. A repeatable op may go to a different reactor each
    * time. To run an op alongside other work, call this inside a `Future`.
    *
    * Cancelling the caller asks the reactor to cancel the op, but this keeps waiting until the reactor confirms, so
    * once it returns or throws, the op's buffers and slots belong to the caller again.
    */
  def perform(b: Backend, op: b.Op)(using ac: Async): Unit =
    ac.scheduler match {
      // A pool's own scheduler, as on Scala Native: matched on the class, the common case, ahead of the general one.
      case pool: ReactorPool if pool.backend eq b =>
        val worker = pool.currentWorker
        if worker != null then performHere(b, op, worker)
        else performElsewhere(b, op, pool.driverFor(Thread.currentThread()))
      case _ =>
        val router = routerFor(b)
        val worker = router.currentWorker
        if worker != null then performHere(b, op, worker)
        else performElsewhere(b, op, router.driverFor(Thread.currentThread()))
    }

  /** `perform` on the worker running the caller: an op the reactor finishes at once returns without suspending. */
  private def performHere(b: Backend, op: b.Op, worker: ReactorWorker)(using ac: Async): Unit = {
    val r = worker.reactor.asInstanceOf[Reactor.Of[b.Op]] // its reactor is one of `b`'s
    if ac.group.isCancelled then throw new CancellationException()
    // An op performed again reuses its waiter, kept as its attachment, so that performing it allocates nothing here,
    // even when it last ran on another worker. A waiter still in use (the op performed twice at once, which the
    // reactor refuses) is left alone.
    val waiter = b.attachment(op) match {
      case reused: OpWaiter if !reused.inUse =>
        reused.reset(worker)
        reused
      case _ =>
        val fresh = new OpWaiter(worker, op)
        b.attach(op, fresh)
        fresh
    }
    waiter.inUse = true
    try {
      // The fast path: an op the reactor finishes at once returns here without suspending, which skips the whole await
      // and resume. Taken within a small budget per turn of the caller, so a future whose ops rarely wait still gives
      // the turn to those queued beside it, and beyond that only while no other task is queued, within a budget per
      // round of the scheduler, so the reactor still polls for futures waiting on events.
      // A refused op, such as a one-shot op submitted twice, throws from either call.
      val finished =
        if worker.turnFastPathsLeft > 0 then {
          val now = r.trySubmit(op, waiter.completion)
          if now then worker.turnFastPathsLeft -= 1
          now
        } else if worker.fastPathsLeft > 0 && worker.isIdle then {
          val now = r.trySubmit(op, waiter.completion)
          if now then worker.fastPathsLeft -= 1
          now
        } else {
          r.submit(op, waiter.completion)
          false
        }
      if !finished then {
        waiter.link() // Cancelling the caller now cancels the op; if it already was, this cancels it at once.
        // Awaited outside the caller's group, so cancellation cannot resume it before the reactor has confirmed.
        try ac.withGroup(CompletionGroup.Unlinked).await(waiter).get
        finally waiter.unlink()
      }
    } finally waiter.inUse = false
  }

  /** `perform` from a caller on no worker: the op is handed to `driver`'s thread, which submits it, and the caller
    * suspends until its completion, which runs there, resumes it.
    */
  private def performElsewhere(b: Backend, op: b.Op, driver: ReactorDriver)(using ac: Async): Unit = {
    if ac.group.isCancelled then throw new CancellationException()
    val waiter = b.attachment(op) match {
      case reused: RemoteWaiter if !reused.inUse => reused
      case _                                     =>
        val fresh = new RemoteWaiter(driver, op)
        b.attach(op, fresh)
        fresh
    }
    // An op the backend can finish here, on the caller's thread, skips both hand-offs. A run of them gives up the
    // thread now and then, as a pool's fast path gives up its turn, so the futures beside the caller still run.
    if waiter.finishedHere >= RemoteWaiter.HereBudget then {
      waiter.finishedHere = 0
      Thread.`yield`()
    }
    if b.tryNow(op) then {
      waiter.finishedHere += 1
      return
    }
    waiter.finishedHere = 0
    waiter.reset(driver)
    waiter.inUse = true
    try {
      // Linked first: a cancel is handed to the same thread after the submission, so it finds the op pending.
      waiter.link()
      driver.sync(
        try submitOn(driver, op, waiter.completion)
        catch case t: Throwable => waiter.refuse(t) // a refused op, such as a one-shot op submitted twice
      )
      try ac.withGroup(CompletionGroup.Unlinked).await(waiter).get
      finally waiter.unlink()
    } finally waiter.inUse = false
  }

  /** Submits `op`, which must have been built by `b.ops`, to the reactor of the worker running the caller, with a
    * completion of the caller's own instead of suspending: the completion runs on that worker's thread, which may not
    * be the caller's by then, since a task may resume on another worker. The returned handle cancels the op there.
    */
  def submit(b: Backend, op: b.Op, completion: Completion[b.Op])(using ac: Async): Submission = {
    val router = ac.scheduler match {
      case pool: ReactorPool if pool.backend eq b => pool // the common case, matched on the class
      case _                                      => routerFor(b)
    }
    val worker = router.currentWorker
    if worker != null then {
      worker.reactor.asInstanceOf[Reactor.Of[b.Op]].submit(op, completion) // its reactor is one of `b`'s
      new Submission(worker, op)
    } else {
      val driver = router.driverFor(Thread.currentThread())
      driver.sync(submitOn(driver, op, completion.asInstanceOf[Completion[Any]])) // its ops are `b`'s, as is the reactor
      new Submission(driver, op)
    }
  }

  /** An op submitted with `submit`, which it cancels on the reactor it went to. */
  final class Submission private[ReactorAsync] (driver: ReactorDriver, op: Any) {

    /** Cancels the op, from any thread: at once if called on the worker it went to, and returns true; otherwise on that
      * worker's thread, soon, and returns false, so the op is not free to submit again until its completion's
      * `onCancel` has run.
      */
    def cancel(): Boolean =
      if driver.isCurrent then {
        cancelOn(driver, op)
        true
      } else {
        driver.sync(cancelOn(driver, op))
        false
      }
  }

  /** Submits `op` to `driver`'s reactor, on its thread; the op was built by the backend whose reactor it is. */
  private def submitOn(driver: ReactorDriver, op: Any, completion: Completion[Any]): Unit = {
    val r = driver.reactor
    r.submit(op.asInstanceOf[r.Op], completion)
  }

  /** Cancels `op` on `driver`'s reactor, on its thread (see `submitOn`). */
  private def cancelOn(driver: ReactorDriver, op: Any): Unit = {
    val r = driver.reactor
    r.cancel(op.asInstanceOf[r.Op]): Unit
  }

  /** The reactors of the caller's program, which must have been started with `b`: through the caller's scheduler, if
    * it is a pool, or else those the runner set up beside gears' own scheduler.
    */
  private def routerFor(b: Backend)(using ac: Async): OpRouter = {
    val router = ac.scheduler match {
      case router: OpRouter => router
      case _                   => OpRouter.ambient.get()
    }
    if router == null || !(router.backend eq b) then
      throw new IllegalStateException("Ops must be performed by a program that ReactorFutures.run started with their backend")
    router
  }

  /** Gives up the caller's turn: suspends it and queues its resume behind the tasks already queued, so the futures
    * sharing its thread run first, as a long computation should between slices. Cheaper than awaiting a future spawned
    * to the same end. Throws `CancellationException` if the caller has been cancelled by the time it resumes.
    */
  def yieldNow()(using ac: Async): Unit = {
    ac.withGroup(CompletionGroup.Unlinked).await(Yield)
    if ac.group.isCancelled then throw new CancellationException()
  }

  /** Never ready when polled, and completes a listener as soon as it is added, so that awaiting it resumes the caller
    * from the back of its scheduler's queue.
    */
  private object Yield extends Async.Source[Unit] {
    override def poll(): Option[Unit] = None
    def poll(k: Listener[Unit]): Boolean = false
    def onComplete(k: Listener[Unit]): Unit = k.completeNow((), this)
    def dropListener(k: Listener[Unit]): Unit = ()
  }

  private val done = Success(())

  /** An op's submissions, one at a time, each as a source that its completion completes once and as a member of the
    * caller's cancellation group. It has at most one listener, the suspended caller. Kept as the op's attachment and
    * reset for each submission, so it, its state and its completion are allocated once per op. Each submission may be
    * on a different worker; `scheduler` is the current one's.
    */
  private final class OpWaiter(first: ReactorWorker, op: Any) extends Async.Source[Try[Unit]] with Cancellable {

    /** Whether a `perform` is using it. On the thread performing the op only. */
    var inUse = false

    /** The worker, and so the reactor, of the current submission. Read by `cancel` from any thread. */
    @volatile private var worker = first

    // Counts submissions, so that a cancel requested from another thread for one cannot reach the next.
    @volatile private var generation = 0

    /** Readies it for the op's next submission, on `next`'s reactor, after the previous one has finished. The worker
      * changes before the generation, so a `cancel` that reads the new generation also finds the new worker.
      */
    def reset(next: ReactorWorker): Unit = {
      if worker ne next then worker = next // usually the same, and a volatile write costs more than a read
      generation += 1
      state = OpWaiter.Empty
    }

    // Empty, then either the suspended caller's listener or the outcome, and finally the outcome, so exactly one side
    // delivers the outcome: `finish` to a listener it finds, or `onComplete` to itself when it finds the outcome
    // already there. On the reactor thread only, where completions run, where the caller awaits, and where `cancel`
    // reads it, so a plain field: Scala Native's `AtomicReference` computes a `ClassTag` on every operation.
    private var state: AnyRef = OpWaiter.Empty

    // Completions are contravariant, so this one fits the op of any reactor.
    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    private def finish(outcome: Try[Unit]): Unit = {
      val previous = state
      state = outcome
      previous match {
        case k: Listener[Try[Unit]] @unchecked => k.completeNow(outcome, this)
        case _                                 => ()
      }
    }

    // gears' `await` calls this first; the default builds a listener and an `Option` to find out.
    override def poll(): Option[Try[Unit]] =
      state match {
        case outcome: Try[Unit] @unchecked => Some(outcome)
        case _                             => None
      }

    def poll(k: Listener[Try[Unit]]): Boolean =
      state match {
        case outcome: Try[Unit] @unchecked =>
          k.completeNow(outcome, this)
          true
        case _ => false
      }

    def onComplete(k: Listener[Try[Unit]]): Unit =
      state match {
        case outcome: Try[Unit] @unchecked => k.completeNow(outcome, this)
        case OpWaiter.Empty                => state = k
        case _                             => () // Only the one caller ever waits.
      }

    def dropListener(k: Listener[Try[Unit]]): Unit = if state eq k then state = OpWaiter.Empty

    // Cancellation can come from any thread, so it is carried out on the thread of the reactor the op was submitted to.
    // By then the op may have finished, and a repeatable op may even have been submitted again, there or elsewhere,
    // which must not be cancelled.
    def cancel(): Unit = {
      val submission = generation
      val target = worker
      target.sync(
        if submission == generation && !state.isInstanceOf[Try[?]] then
          cancelOn(target, op)
      )
    }
  }

  private object OpWaiter {
    val Empty = new Object
  }

  private object RemoteWaiter {

    /** How many submissions of one op in a row may finish on the caller's thread before it yields. */
    inline val HereBudget = 32
  }

  /** An op's submissions from a caller on no worker, as `OpWaiter`, except that the caller awaits on its own thread
    * while the completion runs on the reactor's, so its state is guarded by its lock.
    */
  private final class RemoteWaiter(first: ReactorDriver, op: Any) extends Async.Source[Try[Unit]] with Cancellable {

    /** Whether a `perform` is using it. On the thread performing the op only. */
    var inUse = false

    /** Submissions in a row that the backend finished on the caller's thread (`Backend.tryNow`). Caller's thread only. */
    var finishedHere = 0

    @volatile private var driver = first
    @volatile private var generation = 0

    // Empty, then the caller's listener or the outcome, and finally the outcome (see `OpWaiter`). Guarded by `this`.
    private var state: AnyRef = OpWaiter.Empty

    def reset(next: ReactorDriver): Unit = {
      driver = next
      generation += 1
      synchronized { state = OpWaiter.Empty }
    }

    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    /** The reactor refused the submission: it fails with `failure`. */
    def refuse(failure: Throwable): Unit = finish(Failure(failure))

    private def finish(outcome: Try[Unit]): Unit = {
      val waiting = synchronized {
        val previous = state
        state = outcome
        previous
      }
      waiting match {
        case k: Listener[Try[Unit]] @unchecked => k.completeNow(outcome, this)
        case _                                 => ()
      }
    }

    private def outcome: Try[Unit] | Null = synchronized {
      state match {
        case outcome: Try[Unit] @unchecked => outcome
        case _                             => null
      }
    }

    override def poll(): Option[Try[Unit]] = Option(outcome)

    def poll(k: Listener[Try[Unit]]): Boolean = {
      val now = outcome
      if now != null then k.completeNow(now, this)
      now != null
    }

    def onComplete(k: Listener[Try[Unit]]): Unit = {
      val now = synchronized {
        state match {
          case outcome: Try[Unit] @unchecked => outcome
          case OpWaiter.Empty                =>
            state = k
            null
          case _ => null // Only the one caller ever waits.
        }
      }
      if now != null then k.completeNow(now, this)
    }

    def dropListener(k: Listener[Try[Unit]]): Unit = synchronized { if state eq k then state = OpWaiter.Empty }

    // Carried out on the driver's thread, where the submission went, if it is still the current one and pending.
    def cancel(): Unit = {
      val submission = generation
      val target = driver
      target.sync(
        if submission == generation && outcome == null then
          cancelOn(target, op)
      )
    }
  }
}
