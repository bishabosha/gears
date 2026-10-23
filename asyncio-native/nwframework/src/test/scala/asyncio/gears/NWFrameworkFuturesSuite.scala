package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations

import asyncio.nwframework.NWFrameworkBackend

/** `FuturesContract` over the NWFramework backend's reactors, on a pool of `workers` workers; its sockets are TCP. */
abstract class NWFrameworkFuturesSuite(workers: Int) extends FuturesContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend

  protected def running[T](body: backend.Io => (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class NWFrameworkFuturesOnOneSuite extends NWFrameworkFuturesSuite(workers = 1)
class NWFrameworkFuturesOnFourSuite extends NWFrameworkFuturesSuite(workers = 4)
