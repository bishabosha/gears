package higherlevel

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Io
import gears.async.io.ReactorFutures
import gears.async.io.internal.ReactorRuns
import gears.async.io.kqueue.KqueueBackend
import gears.async.io.nwframework.NWFrameworkBackend

/** The higher level over kqueue's reactors, on a pool of several workers. */
class KqueueHigherLevelSuite extends HigherLevelChecks {
  val backend: KqueueBackend.type = KqueueBackend

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorRuns.run(4)(using backend)(body)
}

/** The higher level over the Network.framework backend's reactors, on a pool of several workers. */
class NWFrameworkHigherLevelSuite extends HigherLevelChecks {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorRuns.run(4)(using backend)(body)
}

/** The same with every IP socket on Network.framework, loopback's too, which the checks use. */
class NWFrameworkEverywhereHigherLevelSuite extends HigherLevelChecks {
  val backend: NWFrameworkBackend.Everywhere.type = NWFrameworkBackend.Everywhere

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorRuns.run(4)(using backend)(body)
}
