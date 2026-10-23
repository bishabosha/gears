package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Listener

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicReference
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import asyncio.Backend
import asyncio.CallerBackend
import asyncio.Completion
import asyncio.Reactor
import asyncio.ReactorBackend

/** What a program performs its ops with: a backend's ops, handles and capabilities, since it is a `Backend`, bound to
  * where the program runs, so that `perform` can run an op there. A runner makes one for the program it starts
  * (`ReactorFutures.run`), and nothing else does, so an op of a reactor backend is only ever performed where there is a
  * worker, and its reactor, to run it; a caller backend is its own `Io`, since its ops may be performed from any
  * thread. Each capability narrows a backend's `Io` to itself (`Backend.Io`), so a program over a
  * `SocketBackend & FileBackend` is given an `Io & SocketBackend & FileBackend`. A program's `Io` is lexical: a future
  * it spawns captures it, and runs where the program runs. The two kinds are `OnPool` and `OnCaller`.
  *
  * It is also the program's gears `AsyncOperations`, given to it by the runner beside its `Async`: `sleep` is a timer
  * op performed, so gears' `sleep`, `withTimeout` and `Timer` run on the backend's timers, and `yield` gives up the
  * caller's turn the way the kind can.
  */
trait Io extends Backend with AsyncOperations {

  /** Performs `op` with no time limit (see the other `perform`). */
  final def perform(op: Op)(using Async): Unit = perform(op, Backend.NoTimeout)

  /** Performs `op` and suspends the caller until it is done: it returns when the op finishes, throws its failure, or
    * throws `CancellationException` when the caller is cancelled first; and, unless `timeoutMillis` is negative
    * (`Backend.NoTimeout`), throws `java.util.concurrent.TimeoutException` if the op is not done that many
    * milliseconds after it was started, the op stopped as `Submitter.submit` says, so that a read with a timeout is
    * one call, with no timer raced against it. Once it returns or throws, the op's buffers and slots are the caller's
    * again. An op marked `Repeatable` may be performed again once it has returned; any other op only once. What
    * cancelling the caller does to the op is the kind's to say.
    */
  def perform(op: Op, timeoutMillis: Int)(using Async): Unit

  /** Sleeps on a timer op of this backend's. */
  override def sleep(millis: Long)(using Async): Unit = {
    require(millis >= 0 && millis <= Int.MaxValue, s"A sleep of $millis ms is out of a timer's range")
    perform(ops.timer(millis.toInt))
  }
}

/** An `Io` on a pool of workers driving a reactor backend's reactors: an op is submitted to the reactor of the worker
  * running the caller, which suspends until the reactor finishes with it (`OnPool.performOnReactor`), and may also be
  * submitted with a completion of the caller's own (`submit`). Cancelling the caller asks the reactor to cancel the
  * op, and waits until it has.
  */
trait OnPool extends Io {

  /** The reactor backend whose ops these are, and whose reactors the pool's workers drive. */
  val backend: ReactorBackend { type Op = OnPool.this.Op }

  /** The pool's scheduler, which knows the worker running the caller. */
  private[asyncio] def scheduler: ReactorScheduler

  final def perform(op: Op, timeoutMillis: Int)(using Async): Unit =
    OnPool.performOnReactor(backend, op, timeoutMillis, scheduler)

  /** Submits `op` to the reactor of the worker running the caller, with a completion of the caller's own instead of
    * suspending: the completion runs on that worker's thread, which may not be the caller's by then, since a task may
    * resume on another worker. The returned handle cancels the op there. With a time limit, which the reactor keeps
    * (see `Submitter.submit`).
    */
  final def submit(op: Op, completion: Completion[Op], timeoutMillis: Int = Backend.NoTimeout)(using
      Async
  ): OnPool.Submission = OnPool.submitOn(backend, op, completion, timeoutMillis, scheduler)

  /** Gives up the caller's turn: suspends it and queues its resume behind the tasks already queued on its worker, so
    * the futures sharing the worker run first, as a long computation should between slices. Cheaper than gears'
    * own, which spawns a task to the same end. Throws `CancellationException` if the caller has been cancelled by the
    * time it resumes.
    */
  override def `yield`()(using ac: Async): Unit = {
    ac.withGroup(CompletionGroup.Unlinked).await(OnPool.Yield)
    if ac.group.isCancelled then throw new CancellationException()
  }

  // An `Io` is a program's, already running.
  private[asyncio] final def start(runner: Runner, workers: Int, policy: TakePolicy)(
      program: Io => (Async.Spawn, AsyncOperations) ?=> Unit
  ): Unit = throw new IllegalStateException(s"$this is a running program's: start a program over $backend")
}

/** How an `OnPool` performs and submits its ops on the pool: the same on every platform. */
object OnPool {

  /** Submits `op`, which must have been built by `b.ops`, to the reactor of the worker running the caller, on
    * `scheduler`, the pool's, and suspends the caller until the reactor finishes with it: it returns when the op
    * completes, throws its failure, throws `CancellationException` when it is cancelled, or throws
    * `TimeoutException` when its time limit, `timeoutMillis` unless negative, ran out first (see `Submitter.submit`).
    * A repeatable op may go to a different reactor each time.
    *
    * Cancelling the caller asks the reactor to cancel the op, but this keeps waiting until the reactor confirms, so
    * once it returns or throws, the op's buffers and slots belong to the caller again.
    */
  private[asyncio] def performOnReactor(b: ReactorBackend, op: b.Op, timeoutMillis: Int, scheduler: ReactorScheduler)(
      using ac: Async
  ): Unit = {
    // Pinned to its thread from finding the worker to submitting on it (see `ReactorScheduler.pin`): `performHere`
    // unpins once the op is submitted, before it waits.
    scheduler.pin()
    val worker =
      try scheduler.currentWorker
      catch {
        case t: Throwable =>
          scheduler.unpin()
          throw t
      }
    performHere(b, op, worker, scheduler, timeoutMillis)
  }

  /** `perform` on the worker running the caller, which `pinned` keeps there until the op is submitted: an op the
    * reactor finishes at once returns without suspending. The reactor keeps the time limit.
    */
  private def performHere(
      b: ReactorBackend,
      op: b.Op,
      worker: ReactorWorker,
      pinned: ReactorScheduler,
      timeoutMillis: Int
  )(using ac: Async): Unit = {
    val r = worker.reactor.asInstanceOf[Reactor.Of[b.Op]] // its reactor is one of `b`'s
    // An op performed again reuses its waiter, kept as its attachment (`Submitter.attachment`, which belongs to the op,
    // so any reactor of the backend reads it), so that performing it allocates nothing here, even when it last ran on
    // another worker. A waiter still in use (the op performed twice at once, which the reactor refuses) is left alone.
    val waiter =
      try {
        if ac.group.isCancelled then throw new CancellationException()
        r.attachment(op) match {
          case reused: OpWaiter if !reused.inUse =>
            reused.reset(worker)
            reused
          case _ =>
            val fresh = new OpWaiter(worker, op)
            r.attach(op, fresh)
            fresh
        }
      } catch {
        case t: Throwable =>
          pinned.unpin()
          throw t
      }
    waiter.inUse = true
    try {
      // The fast path: an op the reactor finishes at once returns here without suspending, which skips the whole await
      // and resume. Taken within a small budget per turn of the caller, so a future whose ops rarely wait still gives
      // the turn to those queued beside it, and beyond that only while no other task is queued, within a budget per
      // round of the scheduler, so the reactor still polls for futures waiting on events.
      // A refused op, such as a one-shot op submitted twice, throws from either call.
      val finished =
        try
          if worker.turnFastPathsLeft > 0 then {
            val now = r.trySubmit(op, waiter.completion, timeoutMillis)
            if now then worker.turnFastPathsLeft -= 1
            now
          } else if worker.fastPathsLeft > 0 && worker.isIdle then {
            val now = r.trySubmit(op, waiter.completion, timeoutMillis)
            if now then worker.fastPathsLeft -= 1
            now
          } else {
            r.submit(op, waiter.completion, timeoutMillis)
            false
          }
        finally pinned.unpin() // before the await parks, which pinned would park the worker's thread
      if !finished then {
        waiter.link() // Cancelling the caller now cancels the op; if it already was, this cancels it at once.
        // Awaited outside the caller's group, so cancellation cannot resume it before the reactor has confirmed.
        try ac.withGroup(CompletionGroup.Unlinked).await(waiter).get
        finally waiter.unlink()
      }
    } finally waiter.inUse = false
  }

  /** Submits `op`, which must have been built by `b.ops`, to the reactor of the worker running the caller, on
    * `scheduler`, the pool's, with a completion of the caller's own (see `OnPool.submit`).
    */
  private[asyncio] def submitOn(
      b: ReactorBackend,
      op: b.Op,
      completion: Completion[b.Op],
      timeoutMillis: Int,
      scheduler: ReactorScheduler
  )(using ac: Async): Submission = {
    scheduler.pin() // as `performOnReactor` does, until the op is submitted on the worker found
    val worker =
      try {
        val worker = scheduler.currentWorker
        worker.reactor.asInstanceOf[Reactor.Of[b.Op]].submit(op, completion, timeoutMillis) // one of `b`'s
        worker
      } finally scheduler.unpin()
    new Submission(worker, op)
  }

  /** An op submitted with `submit`, which it cancels on the reactor it went to. */
  final class Submission private[OnPool] (worker: ReactorWorker, op: Any) {

    /** Cancels the op, from any thread: at once if called on the worker it went to, and returns true; otherwise on that
      * worker's thread, soon, and returns false, so the op is not free to submit again until its completion's
      * `onCancel` has run.
      */
    def cancel(): Boolean =
      if worker.isCurrent then {
        cancelOn(worker, op)
        true
      } else {
        worker.sync(cancelOn(worker, op))
        false
      }
  }

  /** Cancels `op` on `worker`'s reactor, on its thread; the op was built by the backend whose reactor it is. */
  private def cancelOn(worker: ReactorWorker, op: Any): Unit = {
    val r = worker.reactor
    r.cancel(op.asInstanceOf[r.Op]): Unit
  }

  /** Never ready when polled, and completes a listener as soon as it is added, so that awaiting it resumes the caller
    * from the back of its scheduler's queue (see `OnPool.yield`).
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
      state.set(OpWaiter.Empty)
    }

    // Empty, then either the suspended caller's listener or the outcome, and finally the outcome, so exactly one side
    // delivers the outcome: `finish` to a listener it finds, or `onComplete` to itself when it finds the outcome
    // already there. The completion runs on the reactor's thread; the caller registers its listener on the thread
    // it is on by then, which on the JVM may be another worker's, since a virtual thread is pinned only until its op
    // is submitted (see `ReactorScheduler.pin`), so the two race, and the field is atomic. On Scala Native nothing
    // moves the caller, and a plain field would do, but measured against this it is within noise (kqueue ping-pong
    // 8.74 against 8.67 µs), so one shared field it is.
    private val state = new AtomicReference[AnyRef](OpWaiter.Empty)

    // Completions are contravariant, so this one fits the op of any reactor.
    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    private def finish(outcome: Try[Unit]): Unit =
      state.getAndSet(outcome) match {
        case k: Listener[Try[Unit]] @unchecked => k.completeNow(outcome, this)
        case _                                 => ()
      }

    // gears' `await` calls this first; the default builds a listener and an `Option` to find out.
    override def poll(): Option[Try[Unit]] =
      state.get() match {
        case outcome: Try[Unit] @unchecked => Some(outcome)
        case _                             => None
      }

    def poll(k: Listener[Try[Unit]]): Boolean =
      state.get() match {
        case outcome: Try[Unit] @unchecked =>
          k.completeNow(outcome, this)
          true
        case _ => false
      }

    def onComplete(k: Listener[Try[Unit]]): Unit = {
      var done = false
      while !done do
        state.get() match {
          case outcome: Try[Unit] @unchecked =>
            k.completeNow(outcome, this)
            done = true
          case OpWaiter.Empty => done = state.compareAndSet(OpWaiter.Empty, k) // or the outcome arrived: look again
          case _              => done = true // Only the one caller ever waits.
        }
    }

    def dropListener(k: Listener[Try[Unit]]): Unit = state.compareAndSet(k, OpWaiter.Empty): Unit

    // Cancellation can come from any thread, so it is carried out on the thread of the reactor the op was submitted to.
    // By then the op may have finished, and a repeatable op may even have been submitted again, there or elsewhere,
    // which must not be cancelled.
    def cancel(): Unit = {
      val submission = generation
      val target = worker
      target.sync(
        if submission == generation && !state.get().isInstanceOf[Try[?]] then
          cancelOn(target, op)
      )
    }
  }

  private object OpWaiter {
    val Empty = new Object
  }
}

/** An `Io` on the calling thread, which may block: a caller backend's own, so its ops may be performed from any
  * thread, with no runner. Its `yield` is gears' own, a task spawned on the scheduler.
  */
trait OnCaller extends Io { self: CallerBackend => }

/** How a platform runs a program over a backend, one way per kind of backend: `ReactorFutures.run` is the platform's,
  * and `Backend.start` dispatches to it by the backend's kind. The program gets its `Io`, which is also its
  * `AsyncOperations`.
  */
private[asyncio] trait Runner {

  /** Runs `program` on a pool of `workers` workers driving `b`'s reactors, with `b`'s `Io` bound to the pool. */
  def onPool(b: ReactorBackend, workers: Int, policy: TakePolicy)(
      program: b.Io => (Async.Spawn, AsyncOperations) ?=> Unit
  ): Unit

  /** Runs `program` on threads that may block, each op performed on the calling thread. */
  def onCaller(b: CallerBackend)(program: b.Io => (Async.Spawn, AsyncOperations) ?=> Unit): Unit
}
