package asyncio.gears

import gears.async.Async

import asyncio.darwin.DarwinBackend

/** `FuturesContract` over the Darwin backend's reactors, on a pool of `workers` workers; its sockets are TCP. */
abstract class DarwinFuturesSuite(workers: Int) extends FuturesContract {
  val backend: DarwinBackend.type = DarwinBackend

  protected def running(body: backend.type => Async.Spawn ?=> Unit): Unit =
    ReactorFutures.run(workers)(using backend)(body)
}

class DarwinFuturesOnOneSuite extends DarwinFuturesSuite(workers = 1)
class DarwinFuturesOnFourSuite extends DarwinFuturesSuite(workers = 4)
