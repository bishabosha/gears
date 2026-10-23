package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations

import asyncio.kqueue.KqueueBackend
import higherlevel.HigherLevelChecks

/** `FuturesContract` over kqueue's reactors, on a pool of `workers` workers. */
abstract class KqueueFuturesSuite(workers: Int) extends FuturesContract with ReadinessFuturesChecks with HigherLevelChecks {
  val backend: KqueueBackend.type = KqueueBackend

  protected def running[T](body: backend.Io => (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class KqueueFuturesOnOneSuite extends KqueueFuturesSuite(workers = 1)
class KqueueFuturesOnFourSuite extends KqueueFuturesSuite(workers = 4)
