package gears.async.io
package kqueue

import gears.async.io.Io
import gears.async.Async
import gears.async.AsyncOperations

import gears.async.io.kqueue.KqueueBackend
import gears.async.io.testkit.FuturesContract
import gears.async.io.testkit.ReadinessFuturesChecks

/** `FuturesContract` over kqueue's reactors, on a pool of `workers` workers. */
abstract class KqueueFuturesSuite(workers: Int) extends FuturesContract with ReadinessFuturesChecks {
  val backend: KqueueBackend.type = KqueueBackend

  protected def running[T](body: Io[backend.type] => (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class KqueueFuturesOnOneSuite extends KqueueFuturesSuite(workers = 1)
class KqueueFuturesOnFourSuite extends KqueueFuturesSuite(workers = 4)
