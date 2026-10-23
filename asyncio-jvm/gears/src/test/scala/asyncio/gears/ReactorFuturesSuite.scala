package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import java.nio.charset.StandardCharsets

import asyncio.nio.NioBackend
import asyncio.nio.NioOnCaller
import asyncio.testkit.BufferArena
import higherlevel.HigherLevelChecks

/** `FuturesContract` over `NioOnCaller`: gears' own virtual threads, each op performed on its own thread. */
class NioOnCallerSuite extends FuturesContract with HigherLevelChecks {
  val backend: NioOnCaller.type = NioOnCaller

  protected def running[T](body: backend.Io => (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(using backend)(body)

  test("a program over NioOnCaller needs no runner: it is its own Io, and performs on the calling thread") {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    var outcome = ""
    Async.blocking {
      val r = NioOnCaller
      val (in, out) = r.handles.pipe()
      val arena = BufferArena.open()
      try {
        val buf = arena.allocate(16)
        val reading = Future(r.perform(r.ops.read(in, buf))) // on a virtual thread
        r.perform(r.ops.timer(10)) // on this, a platform thread
        r.perform(r.ops.write(out, arena.of("no runner".getBytes(StandardCharsets.UTF_8))))
        reading.await
        buf.flip()
        outcome = StandardCharsets.UTF_8.decode(buf).toString
      } finally {
        r.perform(r.ops.close(in))
        r.perform(r.ops.close(out))
        arena.close()
      }
    }
    assertEquals(outcome, "no runner")
  }
}

/** `FuturesContract` over `NioBackend`: futures carried by a pool of `workers` workers driving its reactors. */
abstract class ReactorFuturesSuite(workers: Int) extends FuturesContract with HigherLevelChecks {
  val backend: NioBackend.type = NioBackend

  protected def running[T](body: backend.Io => (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class ReactorCarriersSuite extends ReactorFuturesSuite(workers = 1) {
  test("a program's Io is the backend on the pool: its ops go to the reactor of the worker running the caller") {
    var onWorker = false
    running { r =>
      onWorker = summon[Async].scheduler match {
        case s: ReactorScheduler => s.currentWorker.reactor.isInstanceOf[asyncio.nio.NioReactor]
        case _                   => false
      }
    }
    assert(onWorker)
  }

  test("a program's Io cannot start a run: it is not a Backend") {
    val errors = compileErrors("running(r => ReactorFutures.run(using r)(_ => ()))")
    assert(errors.nonEmpty, errors)
  }

  test("an op of a reactor backend cannot be performed with the backend itself, only with a program's Io") {
    val errors = compileErrors("def outside(using Async): Unit = NioBackend.perform(NioBackend.ops.timer(1))")
    assert(errors.nonEmpty, errors)
  }
}
class ReactorCarriersOnFourSuite extends ReactorFuturesSuite(workers = 4)
