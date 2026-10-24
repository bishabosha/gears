package gears.async.io
package nio

import gears.async.io.Io
import gears.async.io.OnCaller
import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import java.nio.charset.StandardCharsets

import gears.async.io.nio.NioBackend
import gears.async.io.nio.NioOnCaller
import gears.async.io.testkit.BufferArena
import gears.async.io.testkit.FuturesContract
import gears.async.io.internal.ReactorScheduler

/** `FuturesContract` over `NioOnCaller`: gears' own virtual threads, each op performed on its own thread. */
class NioOnCallerSuite extends FuturesContract {
  val backend: NioOnCaller.type = NioOnCaller

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(using backend)(body)

  test("a program over NioOnCaller needs no runner: its Io is an OnCaller anyone may make, on the calling thread") {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    var outcome = ""
    Async.blocking {
      val io = OnCaller(NioOnCaller)
      val read = Slot[io.Handle]()
      val write = Slot[io.Handle]()
      io.perform(io.backend.ops.pipe(read, write)) // on this, a platform thread
      val (in, out) = (read.clear(), write.clear())
      val arena = BufferArena.open()
      try {
        val buf = arena.allocate(16)
        val reading = Future(io.perform(io.backend.ops.read(in, buf))) // on a virtual thread
        io.perform(io.backend.ops.timer(10)) // on this, a platform thread
        io.perform(io.backend.ops.write(out, arena.of("no runner".getBytes(StandardCharsets.UTF_8))))
        reading.await
        buf.flip()
        outcome = StandardCharsets.UTF_8.decode(buf).toString
      } finally {
        io.perform(io.backend.ops.close(in))
        io.perform(io.backend.ops.close(out))
        arena.close()
      }
    }
    assertEquals(outcome, "no runner")
  }
}

/** `FuturesContract` over `NioBackend`: futures carried by a pool of `workers` workers driving its reactors. */
abstract class ReactorFuturesSuite(workers: Int) extends FuturesContract {
  val backend: NioBackend.type = NioBackend

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class ReactorCarriersSuite extends ReactorFuturesSuite(workers = 1) {
  test("a program's Io is an OnPool over the backend: its ops go to the reactor of the worker running the caller") {
    var onWorker = false
    running { io ?=>
      onWorker = summon[Async].scheduler match {
        case s: ReactorScheduler => s.currentWorker.reactor.isInstanceOf[gears.async.io.nio.NioReactor]
        case _                   => false
      }
    }
    assert(onWorker)
  }

  test("a program's Io cannot start a run: it is not a Backend") {
    val errors = compileErrors("running(io ?=> ReactorFutures.run(using io)(()))")
    assert(errors.nonEmpty, errors)
  }

  test("an op of a reactor backend cannot be performed with the backend itself, only with a program's Io") {
    val errors = compileErrors("def outside(using Async): Unit = NioBackend.perform(NioBackend.ops.timer(1))")
    assert(errors.nonEmpty, errors)
  }
}
class ReactorCarriersOnFourSuite extends ReactorFuturesSuite(workers = 4)
