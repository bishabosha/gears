package gears.async.io.nwframework

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Io
import gears.async.io.internal.ReactorRuns
import gears.io.higherlevel.HigherLevelChecks

/** The higher level over the Network.framework backend's reactors, on a pool of four workers. */
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
