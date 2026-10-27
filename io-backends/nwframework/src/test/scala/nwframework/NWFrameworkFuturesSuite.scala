package gears.async.io
package nwframework

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Io
import gears.async.io.testkit.FuturesContract

/** `FuturesContract` over the backend's reactors, on a pool of `workers` workers, with its sockets on
  * Network.framework, loopback's too (`NWFrameworkBackend.Everywhere`), since on loopback they would be kqueue's.
  */
abstract class NWFrameworkFuturesSuite(workers: Int) extends FuturesContract {
  val backend: NWFrameworkBackend.Everywhere.type = NWFrameworkBackend.Everywhere

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class NWFrameworkFuturesOnOneSuite extends NWFrameworkFuturesSuite(workers = 1)
class NWFrameworkFuturesOnFourSuite extends NWFrameworkFuturesSuite(workers = 4)
