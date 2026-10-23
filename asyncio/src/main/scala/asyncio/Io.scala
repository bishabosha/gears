package asyncio

import _root_.gears.async.Async
import _root_.gears.async.AsyncOperations
import _root_.gears.async.CompletionGroup

import java.util.concurrent.CancellationException

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
  * running the caller, which suspends until the reactor finishes with it (`asyncio.gears.Pools.performOnReactor`), and may also be
  * submitted with a completion of the caller's own (`submit`). Cancelling the caller asks the reactor to cancel the
  * op, and waits until it has.
  */
trait OnPool extends Io {

  /** The reactor backend whose ops these are, and whose reactors the pool's workers drive. */
  val backend: ReactorBackend { type Op = OnPool.this.Op }

  /** The pool's scheduler, which knows the worker running the caller. */
  private[asyncio] def scheduler: asyncio.gears.ReactorScheduler

  final def perform(op: Op, timeoutMillis: Int)(using Async): Unit =
    asyncio.gears.Pools.performOnReactor(backend, op, timeoutMillis, scheduler)

  /** Submits `op` to the reactor of the worker running the caller, with a completion of the caller's own instead of
    * suspending: the completion runs on that worker's thread, which may not be the caller's by then, since a task may
    * resume on another worker. The returned handle cancels the op there. With a time limit, which the reactor keeps
    * (see `Submitter.submit`).
    */
  final def submit(op: Op, completion: Completion[Op], timeoutMillis: Int = Backend.NoTimeout)(using
      Async
  ): Submission = asyncio.gears.Pools.submitOn(backend, op, completion, timeoutMillis, scheduler)

  /** Gives up the caller's turn: suspends it and queues its resume behind the tasks already queued on its worker, so
    * the futures sharing the worker run first, as a long computation should between slices. Cheaper than gears'
    * own, which spawns a task to the same end. Throws `CancellationException` if the caller has been cancelled by the
    * time it resumes.
    */
  override def `yield`()(using ac: Async): Unit = {
    ac.withGroup(CompletionGroup.Unlinked).await(asyncio.gears.Pools.Yield)
    if ac.group.isCancelled then throw new CancellationException()
  }
}

/** An op submitted with `OnPool.submit`, which it cancels on the reactor it went to. */
trait Submission {

  /** Cancels the op, from any thread: at once if called on the worker it went to, and returns true; otherwise on that
    * worker's thread, soon, and returns false, so the op is not free to submit again until its completion's
    * `onCancel` has run.
    */
  def cancel(): Boolean
}

/** An `Io` on the calling thread, which may block: a caller backend's own, so its ops may be performed from any
  * thread, with no runner. Its `yield` is gears' own, a task spawned on the scheduler.
  */
trait OnCaller extends Io { self: CallerBackend => }

