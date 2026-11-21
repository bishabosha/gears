package gears.async.io.oxflow

import gears.async.Async
import gears.async.default.given
import gears.async.io.Flow

import _root_.ox.flow.Flow as OxFlow

/** EXPERIMENT. Ox's flows and gears' are the same thing made twice: a stage that is run with what to call for each
  * element. So each is the other with nothing between them but that call.
  *
  * An Ox flow blocks the thread it is run on; a gears flow suspends the future it is run in. On the JVM a future is a
  * virtual thread, for which the two are one, so a flow of either kind runs inside the other's.
  */
extension [A](flow: OxFlow[A]) {

  /** This Ox flow as a gears flow: run where the gears flow is, its elements emitted there. What Ox does beside it,
    * `mapPar`'s forks say, it does in scopes of its own.
    */
  def toGears: Flow[A] = Flow[A](flow.runForeach(element => Flow.emit(element)))
}

extension [A](flow: Flow[A]) {

  /** This gears flow as an Ox flow. Run in the future that asked for it, as an Ox flow made and run in one expression
    * is, it runs with that future's `Async`. Ox operators that run their upstream in a fork of their own (`buffer`,
    * `mapPar`, `merge`) run it on another thread, and there it is given an `Async` of that thread: a future is a
    * virtual thread on the JVM, so which of the two it is on is which thread it is on.
    */
  def toOx(using async: Async): OxFlow[A] = {
    val origin = Thread.currentThread()
    OxFlow.usingEmit { emit =>
      if Thread.currentThread() eq origin then flow.foreach(element => emit(element))(using async)
      else Async.blocking(flow.foreach(element => emit(element)))
    }
  }

  /** This gears flow as an Ox flow, for a caller with no `Async`: each run is given one of the thread it is on. What
    * the flow awaits blocks that thread, a virtual one in Ox; an op of a pool backend, which is performed on the
    * pool's own threads, is not for it.
    */
  def toOxOnAnyThread: OxFlow[A] =
    OxFlow.usingEmit(emit => Async.blocking(flow.foreach(element => emit(element))))

  /** A stage of Ox's in this gears flow: what `pipe` makes of it, as a gears flow. Ox runs the stage, forks and all,
    * where this flow is run, and what comes out is emitted there.
    */
  def throughOx[B](pipe: OxFlow[A] => OxFlow[B]): Flow[B] =
    Flow[B](pipe(flow.toOx).runForeach(element => Flow.emit(element)))
}

extension [A](flow: OxFlow[A]) {

  /** A stage of gears' in this Ox flow: what `pipe` makes of it, as an Ox flow, to go on with Ox's stages. It needs no
    * `Async` of its caller's: it is given one where it is run.
    */
  def throughGears[B](pipe: Flow[A] => Flow[B]): OxFlow[B] = pipe(flow.toGears).toOxOnAnyThread
}
