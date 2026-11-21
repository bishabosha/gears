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

  /** This gears flow as an Ox flow, run with the `Async` of the future that asked for it: for an Ox flow that is run
    * in that same future, as one made and run in one expression is. Ox operators that run their upstream in a fork
    * of their own (`buffer`, `mapPar`, `merge`) run it on another thread, for which use `toOxOnAnyThread`.
    */
  def toOx(using Async): OxFlow[A] = OxFlow.usingEmit(emit => flow.foreach(element => emit(element)))

  /** This gears flow as an Ox flow that may be run on any thread: each run is given an `Async` of the thread it is
    * on. What the flow awaits blocks that thread, a virtual one in Ox; an op of a pool backend, which is performed on
    * the pool's own threads, is not for it.
    */
  def toOxOnAnyThread: OxFlow[A] =
    OxFlow.usingEmit(emit => Async.blocking(flow.foreach(element => emit(element))))
}
