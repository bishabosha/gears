package gears.async.io
package nwframework

import gears.async.io.Io
import gears.async.Async
import gears.async.AsyncOperations

import gears.async.io.nwframework.NWFrameworkBackend
import gears.async.io.testkit.FuturesContract

/** `FuturesContract` over the NWFramework backend's reactors, on a pool of `workers` workers; its sockets are TCP. */
abstract class NWFrameworkFuturesSuite(workers: Int) extends FuturesContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class NWFrameworkFuturesOnOneSuite extends NWFrameworkFuturesSuite(workers = 1)
class NWFrameworkFuturesOnFourSuite extends NWFrameworkFuturesSuite(workers = 4)
