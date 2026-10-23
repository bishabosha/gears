package asyncio.gears

import gears.async.Async

import asyncio.kqueue.KqueueBackend

/** `FuturesContract` over kqueue's reactors, on a pool of `workers` workers. */
abstract class KqueueFuturesSuite(workers: Int) extends FuturesContract with ReadinessFuturesChecks {
  val backend: KqueueBackend.type = KqueueBackend

  protected def running(body: backend.Io => Async.Spawn ?=> Unit): Unit =
    ReactorFutures.run(workers)(using backend)(body)
}

class KqueueFuturesOnOneSuite extends KqueueFuturesSuite(workers = 1)
class KqueueFuturesOnFourSuite extends KqueueFuturesSuite(workers = 4)
