package gears.async.io
package internal

import _root_.gears.async.Async

/** A backend that performs each op on the calling thread, which may block, as a virtual thread may: NIO's
  * `NioOnCaller`. Its ops may be performed from anywhere, so a program over it needs no runner: its `Io` is an `Io`
  * from the runner (`Io.run`) performs them on the calling thread.
  */
trait CallerBackend extends Backend {

  /** The backend's ops, which its `Io` has (`Io.ops`). */
  val ops: Interface

  /** Performs `op` on the calling thread, as `Io.perform` says: what its `Io` calls. */
  private[io] def performHere(op: ops.Op, timeoutMillis: Long)(using Async): Unit

  /** Runs `program` on gears' own threads, each op performed on the calling thread (see `Runner.onCaller`). */
  private[io] final def run[T](program: Io[Interface] ?=> Async.Spawn ?=> T): T = Runner.onCaller(this)(program)
}

/** A backend whose ops run on reactors, `Reactor`s it opens: an op built by `ops` may be submitted to any of them, one
  * submission at a time, so that a task moving from one reactor's thread to another's can take its ops along: once a
  * submission has completed, failed or been cancelled on one reactor, a `Repeatable` op may go to another. A program
  * over this backend runs on a pool of workers driving its reactors, started by the platform's runner, and performs its
  * ops with the `Io` the runner gives it, which submits each to the reactor of the worker running the caller.
  */
trait ReactorBackend extends Backend with Reactor.Factory[Reactor] {

  /** Its ops name handles, and carry their Gears completion and cancellation state directly. */
  type Interface <: Handles { type Op <: ReactorOperation }

  /** The backend's ops, which its `Io` has (`Io.ops`) and every reactor it opens runs. */
  val ops: Interface

  /** Opens a reactor that runs this backend's ops. Close it when done. */
  def open(): Reactor.Of[ops.Op, ops.Handle]

  /** Runs `program` on a pool of workers, one per available processor, driving this backend's reactors, under the
    * `Affine` policy (see `Runner.onPool`; `ReactorRuns.run` chooses them).
    */
  private[io] final def run[T](program: Io[Interface] ?=> Async.Spawn ?=> T): T =
    Runner.onPool(Runner.Workers, TakePolicy.Affine)(using this)(program)
}
