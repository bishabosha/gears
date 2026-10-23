package asyncio.gears

import gears.async.Async
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import java.nio.charset.StandardCharsets

import asyncio.nio.NioBackend
import asyncio.nio.NioOnCaller
import asyncio.testkit.BufferArena

/** `FuturesContract` over `NioOnCaller`: gears' own virtual threads, each op performed on its own thread. */
class NioOnCallerSuite extends FuturesContract {
  val backend: NioOnCaller.type = NioOnCaller

  protected def running(body: backend.Io => Async.Spawn ?=> Unit): Unit =
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
        val reading = Future(ReactorAsync.perform(r, r.ops.read(in, buf))) // on a virtual thread
        ReactorAsync.perform(r, r.ops.timer(10)) // on this, a platform thread
        ReactorAsync.perform(r, r.ops.write(out, arena.of("no runner".getBytes(StandardCharsets.UTF_8))))
        reading.await
        buf.flip()
        outcome = StandardCharsets.UTF_8.decode(buf).toString
      } finally {
        ReactorAsync.perform(r, r.ops.close(in))
        ReactorAsync.perform(r, r.ops.close(out))
        arena.close()
      }
    }
    assertEquals(outcome, "no runner")
  }
}

/** `FuturesContract` over `NioBackend`: futures carried by a pool of `workers` workers driving its reactors. */
abstract class ReactorFuturesSuite(workers: Int) extends FuturesContract {
  val backend: NioBackend.type = NioBackend

  protected def running(body: backend.Io => Async.Spawn ?=> Unit): Unit =
    ReactorFutures.run(workers)(using backend)(body)
}

class ReactorCarriersSuite extends ReactorFuturesSuite(workers = 1) {
  test("a program's Io is the backend on the pool: its ops go to the reactor of the worker running the caller") {
    var onWorker = false
    running { r =>
      onWorker = r.scheduler.currentWorker.reactor.isInstanceOf[asyncio.nio.NioReactor]
    }
    assert(onWorker)
  }

  test("an op of a reactor backend cannot be performed with the backend itself, only with a program's Io") {
    val errors = compileErrors("def outside(using Async): Unit = ReactorAsync.perform(NioBackend, NioBackend.ops.timer(1))")
    assert(errors.nonEmpty, errors)
  }
}
class ReactorCarriersOnFourSuite extends ReactorFuturesSuite(workers = 4)
