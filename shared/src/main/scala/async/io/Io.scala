package gears.async.io

import scala.util.Try

import _root_.gears.async.Async
import _root_.gears.async.AsyncOperations
import _root_.gears.async.Cancellable

/** What a program performs its ops with: where its backend's ops run. It has a backend (`backend`), whose ops, handles
  * and capabilities are what the program uses, and adds only where they run: `perform`, and on a pool `yield`. It is
  * not a backend and has no ops of its own; its type names the backend it is over, so a program's helper asks for an
  * `Io` over a backend with the capabilities it needs, `Io[SocketBackend]`, and one over a backend with more,
  * `Io[SocketBackend & FileBackend]`, is one of those. The two kinds are `OnPool`, which the runtime implements and the
  * platform's runner gives the program it starts, so that an op of a reactor backend is performed where there is a
  * worker, and its reactor, to run it, and a backend alone performs nothing; and `OnCaller`, which anyone may make,
  * since a caller backend's ops may be performed from any thread. A program's `Io` is lexical: a future it spawns
  * captures it, and runs where the program runs.
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

  /** The handles of the backend's I/O objects. */
  type Handle = backend.Handle

  /** Performs `op` with no time limit (see the other `perform`). */
  final def perform(op: Op)(using Async): Unit = perform(op, Backend.NoTimeout)

  /** Performs `op` and suspends the caller until it is done: it returns when the op finishes, throws its failure, or
    * throws `CancellationException` when the caller is cancelled first; and, unless `timeoutMillis` is negative
    * (`Backend.NoTimeout`), throws `java.util.concurrent.TimeoutException` if the op is not done that many milliseconds
    * after it was started, the op stopped as `Submitter.submit` says, so that a read with a timeout is one call, with
    * no timer raced against it.
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
  * running the caller, which suspends until the reactor finishes with it. Cancelling the caller asks the reactor to
  * cancel the op, and waits until it has. Ops in flight together are futures performing them
  * (`Future(io.perform(op))`), each cancelled with its future. The pool itself is the runtime's, which implements this:
  * it says only what a program may do on the pool, and `yield` is its alone.
  */
trait OnPool[+B <: ReactorBackend] extends Io[B] {

  /** Gives up the caller's turn the pool's way: its resume queued behind the tasks already queued on its worker, so the
    * futures sharing the worker run first, as a long computation should between slices.
    */
  override def `yield`()(using Async): Unit
}

/** An `Io` on the calling thread, which may block: a caller backend's ops performed by the caller, so it may be made by
  * anyone and used from any thread, with no runner. Its `yield` is gears' own, a task spawned on the scheduler.
  */
final class OnCaller[+B <: CallerBackend](val backend: B) extends Io[B] {
  def perform(op: Op, timeoutMillis: Int)(using Async): Unit = backend.performHere(op, timeoutMillis)
}
