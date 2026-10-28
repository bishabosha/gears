package gears.async.io

import _root_.gears.async.Async
import _root_.gears.async.AsyncOperations

/** What a program performs its ops with: where its backend's ops run. It has a backend (`backend`), whose ops, handles
  * and capabilities are what the program uses, and adds only where they run: `perform`, and on a pool `yield`. It is
  * not a backend and has no ops of its own; its type names the backend it is over, so a program's helper asks for an
  * `Io` over a backend with the capabilities it needs, `Io[IpSockets]`, and one over a backend with more,
  * `Io[IpSockets & Files]`, is one of those. A program gets its `Io` from the platform's runner (`Io.run`), which over
  * a reactor backend performs each op where there is a worker, and its reactor, to run it, since a backend alone
  * performs nothing, and over a caller backend performs it on the calling thread; how it does is internal. A program's
  * `Io` is lexical: a future it spawns captures it, and runs where the program runs.
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

  /** The handles of the backend's I/O objects, and their kinds (see `Ops`). */
  type Handle = backend.Handle
  type Readable = backend.Readable
  type Writable = backend.Writable
  type Connection = backend.Connection
  type Listener = backend.Listener
  type BoundDatagram = backend.BoundDatagram
  type ConnectedDatagram = backend.ConnectedDatagram
  type UnixBoundDatagram = backend.UnixBoundDatagram
  type UnixConnectedDatagram = backend.UnixConnectedDatagram
  type File = backend.File

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

object Io {

  /** Runs `program` over `backend` and returns what it returns, rethrowing its failure: the program is given an `Io`
    * over the backend, as a context parameter, with which it performs its ops and which is its `AsyncOperations`,
    * beside its `Async`. Over a backend with reactors it runs on a pool of workers, one per available processor,
    * driving them; over one without, on gears' own threads, each op performed on the calling thread. A program's `Io`
    * is not a `Backend`, so starting a run over one is refused when compiling.
    */
  def run[T](using backend: Backend)(program: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    internal.Runner.run(program)
}
