package asyncio.gears

import gears.async.Async

import asyncio.Backend
import asyncio.CallerBackend
import asyncio.Completion
import asyncio.ReactorBackend

/** What a program performs its ops with: a backend's ops, handles and capabilities, since it is a `Backend`, bound to
  * where the program runs, so that `perform` can run an op there. A runner makes one for the program it starts
  * (`ReactorFutures.run`), and nothing else does, so an op of a reactor backend is only ever performed where there is a
  * worker, and its reactor, to run it; a caller backend is its own `Io`, since its ops may be performed from any
  * thread. Each capability narrows a backend's `Io` to itself (`Backend.Io`), so a program over a
  * `SocketBackend & FileBackend` is given an `Io & SocketBackend & FileBackend`. A program's `Io` is lexical: a future
  * it spawns captures it, and runs where the program runs. The two kinds are `OnPool` and `OnCaller`.
  */
trait Io extends Backend {

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
}

/** An `Io` on a pool of workers driving a reactor backend's reactors: an op is submitted to the reactor of the worker
  * running the caller, which suspends until the reactor finishes with it (`ReactorAsync.performOnReactor`), and may
  * also be submitted with a completion of the caller's own (`submit`). Cancelling the caller asks the reactor to
  * cancel the op, and waits until it has.
  */
trait OnPool extends Io {

  /** The reactor backend whose ops these are, and whose reactors the pool's workers drive. */
  val backend: ReactorBackend { type Op = OnPool.this.Op }

  /** The pool's scheduler, which knows the worker running the caller. */
  private[asyncio] def scheduler: ReactorScheduler

  final def perform(op: Op, timeoutMillis: Int)(using Async): Unit =
    ReactorAsync.performOnReactor(backend, op, timeoutMillis, scheduler)

  /** Submits `op` to the reactor of the worker running the caller, with a completion of the caller's own instead of
    * suspending: the completion runs on that worker's thread, which may not be the caller's by then, since a task may
    * resume on another worker. The returned handle cancels the op there. With a time limit, which the reactor keeps
    * (see `Submitter.submit`).
    */
  final def submit(op: Op, completion: Completion[Op], timeoutMillis: Int = Backend.NoTimeout)(using
      Async
  ): ReactorAsync.Submission = ReactorAsync.submitOn(backend, op, completion, timeoutMillis, scheduler)

  // An `Io` is a program's, already running.
  private[asyncio] final def start(runner: Runner, workers: Int, policy: TakePolicy)(
      program: Io => Async.Spawn ?=> Unit
  ): Unit = throw new IllegalStateException(s"$this is a running program's: start a program over $backend")
}

/** An `Io` on the calling thread, which may block: a caller backend's own, so its ops may be performed from any
  * thread, with no runner.
  */
trait OnCaller extends Io { self: CallerBackend => }

/** How a platform runs a program over a backend, one way per kind of backend: `ReactorFutures.run` is the platform's,
  * and `Backend.start` dispatches to it by the backend's kind.
  */
private[asyncio] trait Runner {

  /** Runs `program` on a pool of `workers` workers driving `b`'s reactors, with `b`'s `Io` bound to the pool. */
  def onPool(b: ReactorBackend, workers: Int, policy: TakePolicy)(program: b.Io => Async.Spawn ?=> Unit): Unit

  /** Runs `program` on threads that may block, each op performed on the calling thread. */
  def onCaller(b: CallerBackend)(program: b.Io => Async.Spawn ?=> Unit): Unit
}
