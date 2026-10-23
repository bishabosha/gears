package asyncio

import _root_.gears.async.Async
import _root_.gears.async.AsyncOperations
import _root_.gears.async.Cancellable

import scala.util.Try

/** What a program performs its ops with: where its backend's ops run. It has a backend (`backend`), whose ops,
  * handles and capabilities are what the program uses, and adds only where they run: `perform`, and on a pool `submit`
  * and `yield`. It is not a backend and has no ops of its own; its type names the backend it is over, so a program's
  * helper asks for an `Io` over a backend with the capabilities it needs, `Io[SocketBackend]`, and one over a
  * backend with more, `Io[SocketBackend & FileBackend]`, is one of those. The two kinds are `OnPool`, which the
  * runtime implements and the platform's runner gives the program it starts, so that an op of a reactor backend is
  * performed where there is a worker, and its reactor, to run it, and a backend alone performs nothing; and
  * `OnCaller`, which anyone may make, since a caller backend's ops may be performed from any thread. A program's `Io`
  * is lexical: a future it spawns captures it, and runs where the program runs.
  *
  * It is also the program's gears `AsyncOperations`, given to it by the runner beside its `Async`: `sleep` is a timer
  * op performed, so gears' `sleep`, `withTimeout` and `Timer` run on the backend's timers, and `yield` gives up the
  * caller's turn the way the kind can.
  */
trait Io[+B <: Backend] extends AsyncOperations {

  /** The backend whose ops this performs. */
  val backend: B

  /** What this performs: its backend's ops. */
  type Op = backend.Op

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

  /** Sleeps on a timer op of the backend's. */
  override def sleep(millis: Long)(using Async): Unit = {
    require(millis >= 0 && millis <= Int.MaxValue, s"A sleep of $millis ms is out of a timer's range")
    perform(backend.ops.timer(millis.toInt))
  }
}

/** An `Io` on a pool of workers driving a reactor backend's reactors: an op is submitted to the reactor of the worker
  * running the caller, which suspends until the reactor finishes with it, and may also be submitted with a completion
  * of the caller's own (`submit`). Cancelling the caller asks the reactor to cancel the op, and waits until it has.
  * The pool itself is the runtime's, which implements this: it says only what a program may do on the pool, and
  * `submit` and `yield` are its alone, so a program that uses them runs on reactor backends only.
  */
trait OnPool[+B <: ReactorBackend] extends Io[B] {

  /** Submits `op` to the reactor of the worker running the caller, with a completion of the caller's own instead of
    * suspending: the completion runs on that worker's thread, which may not be the caller's by then, since a task may
    * resume on another worker. With a time limit, which the reactor keeps (see `Submitter.submit`). The submission
    * returned is a member of the caller's cancellation group, like a future spawned there: cancelling the group
    * cancels the op, and the group's end waits for the completion, so a submission outlives the scope it was made in
    * only if unlinked (`Cancellable.unlink`).
    */
  def submit(op: Op, completion: Completion[Op], timeoutMillis: Int)(using Async): Submission

  /** Submits `op` with no completion of the caller's own: its outcome is the submission's, as a source. */
  final def submit(op: Op)(using Async): Submission = submit(op, Completion.ignore, Backend.NoTimeout)

  /** Submits `op` with no completion of the caller's own, and a time limit (see `Submitter.submit`). */
  final def submit(op: Op, timeoutMillis: Int)(using Async): Submission = submit(op, Completion.ignore, timeoutMillis)

  /** Submits `op` with a completion of the caller's own and no time limit. */
  final def submit(op: Op, completion: Completion[Op])(using Async): Submission =
    submit(op, completion, Backend.NoTimeout)

  /** Gives up the caller's turn the pool's way: its resume queued behind the tasks already queued on its worker, so
    * the futures sharing the worker run first, as a long computation should between slices.
    */
  override def `yield`()(using Async): Unit
}

/** An op submitted with `OnPool.submit`: a gears `Cancellable`, linked to the caller's group when submitted and
  * unlinked once its completion has run, and a gears `Async.Source` of the op's outcome, completed once, after the
  * completion: `Success` when the op finished, its failure, or a `CancellationException` when it was cancelled. As a
  * source it composes as any other does: awaited, or raced with a channel or a timer in `Async.select`.
  */
trait Submission extends Cancellable with Async.Source[Try[Unit]] {

  /** Asks the reactor the op went to to cancel it, from any thread; nothing if it has already finished. The
    * completion's `onCancel` runs on that reactor's thread once the op is stopped, which is before this returns when
    * called on that thread, and soon afterwards otherwise: the op is not free to submit again until it has run.
    */
  def cancel(): Unit
}

/** An `Io` on the calling thread, which may block: a caller backend's ops performed by the caller, so it may be
  * made by anyone and used from any thread, with no runner. Its `yield` is gears' own, a task spawned on the
  * scheduler.
  */
final class OnCaller[+B <: CallerBackend](val backend: B) extends Io[B] {
  def perform(op: Op, timeoutMillis: Int)(using Async): Unit = backend.performHere(op, timeoutMillis)
}
