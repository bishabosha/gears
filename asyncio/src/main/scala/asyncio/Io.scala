package asyncio

import _root_.gears.async.Async
import _root_.gears.async.AsyncOperations
import _root_.gears.async.Cancellable

/** What a program performs its ops with: a backend's ops, handles and capabilities, since it is a `Backend`, bound to
  * where the program runs, so that `perform` can run an op there. The platform's runner makes one for the program it
  * starts, and nothing else does, so an op of a reactor backend is only ever performed where there is a worker, and
  * its reactor, to run it; a caller backend is its own `Io`, since its ops may be performed from any thread. Each
  * capability narrows a backend's `Io` to itself (`Backend.Io`), so a program over a `SocketBackend & FileBackend`
  * is given an `Io & SocketBackend & FileBackend`. A program's `Io` is lexical: a future it spawns captures it, and
  * runs where the program runs. The two kinds are `OnPool` and `OnCaller`.
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
    * one call, with no timer raced against it.
    *
    * Whichever way it ends, once it returns or throws the op is no longer in use anywhere: its buffers and slots are
    * the caller's again, and its handle is as open as it was, so a cancelled or timed-out read leaves the connection
    * usable. Cancelling the caller asks the kind to stop the op, and `perform` keeps waiting until it has. An op marked
    * `Repeatable` may be performed again once it has returned; any other op only once.
    */
  def perform(op: Op, timeoutMillis: Int)(using Async): Unit

  /** Sleeps on a timer op of this backend's. */
  override def sleep(millis: Long)(using Async): Unit = {
    require(millis >= 0 && millis <= Int.MaxValue, s"A sleep of $millis ms is out of a timer's range")
    perform(ops.timer(millis.toInt))
  }
}

/** An `Io` on a pool of workers driving a reactor backend's reactors: an op is submitted to the reactor of the worker
  * running the caller, which suspends until the reactor finishes with it, and may also be submitted with a completion
  * of the caller's own (`submit`). Cancelling the caller asks the reactor to cancel the op, and waits until it has.
  * The pool itself is the runtime's (`Pool`): this only says what a program may do on it, and `submit` and `yield`
  * are its alone, so a program that uses them runs on reactor backends only.
  */
trait OnPool extends Io {

  /** The reactor backend whose ops these are, and whose reactors the pool's workers drive. */
  val backend: ReactorBackend { type Op = OnPool.this.Op }

  /** Submits `op` to the reactor of the worker running the caller, with a completion of the caller's own instead of
    * suspending: the completion runs on that worker's thread, which may not be the caller's by then, since a task may
    * resume on another worker. With a time limit, which the reactor keeps (see `Submitter.submit`). The submission
    * returned is a member of the caller's cancellation group, like a future spawned there: cancelling the group
    * cancels the op, and the group's end waits for the completion, so a submission outlives the scope it was made in
    * only if unlinked (`Cancellable.unlink`).
    */
  def submit(op: Op, completion: Completion[Op], timeoutMillis: Int = Backend.NoTimeout)(using Async): Submission

  /** Gives up the caller's turn the pool's way: its resume queued behind the tasks already queued on its worker, so
    * the futures sharing the worker run first, as a long computation should between slices.
    */
  override def `yield`()(using Async): Unit
}

/** A pool of workers driving a reactor backend's reactors, as the interface sees it: what a program on it may do with
  * an op of the backend's, which it is bound to once (`ReactorBackend.bind`). The runtime implements it.
  */
private[asyncio] trait Pool {

  /** The op type of the backend this pool is bound to. */
  type Op

  /** Performs `op` on the reactor of the worker running the caller, as `Io.perform` says. */
  def perform(op: Op, timeoutMillis: Int)(using Async): Unit

  /** Submits `op` with a completion of the caller's own, as `OnPool.submit` says. */
  def submit(op: Op, completion: Completion[Op], timeoutMillis: Int)(using Async): Submission

  /** Gives up the caller's turn, as `OnPool.yield` says. */
  def `yield`()(using Async): Unit
}

private[asyncio] object Pool {

  /** A pool bound to a backend whose `Op` is `O`. */
  type Of[O] = Pool { type Op = O }
}

/** An op submitted with `OnPool.submit`: a gears `Cancellable`, linked to the caller's group when submitted and
  * unlinked once its completion has run.
  */
trait Submission extends Cancellable {

  /** Asks the reactor the op went to to cancel it, from any thread; nothing if it has already finished. The
    * completion's `onCancel` runs on that reactor's thread once the op is stopped, which is before this returns when
    * called on that thread, and soon afterwards otherwise: the op is not free to submit again until it has run.
    */
  def cancel(): Unit
}

/** An `Io` on the calling thread, which may block: a caller backend's own, so its ops may be performed from any
  * thread, with no runner. Its `yield` is gears' own, a task spawned on the scheduler.
  */
trait OnCaller extends Io { self: CallerBackend => }
