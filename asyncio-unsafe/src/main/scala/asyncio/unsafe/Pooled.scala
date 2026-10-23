package asyncio.unsafe

import gears.async.Async

import asyncio.Backend
import asyncio.Completion
import asyncio.OnPool
import asyncio.Pool
import asyncio.Submission

/** An `OnPool` over the `Pool` the runner bound it to: what every reactor backend's `Io` class mixes in, so that
  * `perform`, `submit` and `yield` reach the pool the same way on each. The backend's class adds its capabilities and
  * names its backend.
  */
trait Pooled extends OnPool {

  /** The pool the program runs on, bound to this backend (`ReactorBackend.bind`). */
  protected def pool: Pool.Of[Op]

  final def perform(op: Op, timeoutMillis: Int)(using Async): Unit = pool.perform(op, timeoutMillis)

  final def submit(op: Op, completion: Completion[Op], timeoutMillis: Int = Backend.NoTimeout)(using
      Async
  ): Submission = pool.submit(op, completion, timeoutMillis)

  final override def `yield`()(using Async): Unit = pool.`yield`()
}
