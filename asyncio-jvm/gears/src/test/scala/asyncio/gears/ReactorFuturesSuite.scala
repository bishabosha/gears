package asyncio.gears

import gears.async.Async

import asyncio.nio.NioBackend

/** `FuturesContract` over NIO's reactors, carried by `carriers`, on `workers` reactors. */
abstract class ReactorFuturesSuite(carriers: Carriers, workers: Int) extends FuturesContract {
  val backend: NioBackend.type = NioBackend

  protected def running(body: backend.type => Async.Spawn ?=> Unit): Unit =
    ReactorFutures.run(workers, carriers = carriers)(using backend)(body)
}

class JdkCarriersSuite extends ReactorFuturesSuite(Carriers.Jdk, workers = 1)
class JdkCarriersOnTwoSuite extends ReactorFuturesSuite(Carriers.Jdk, workers = 2)
class ReactorCarriersSuite extends ReactorFuturesSuite(Carriers.Reactors, workers = 1) {
  test("futures run on the pool's workers, whose reactors their ops go to") {
    var onWorker = false
    ReactorFutures.run(1, carriers = Carriers.Reactors)(using NioBackend) { r =>
      onWorker = summon[Async].scheduler match {
        case s: ReactorScheduler => s.currentWorker != null
        case _                   => false
      }
    }
    assert(onWorker)
  }
}
class ReactorCarriersOnFourSuite extends ReactorFuturesSuite(Carriers.Reactors, workers = 4)
