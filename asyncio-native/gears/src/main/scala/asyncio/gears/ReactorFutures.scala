package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Future
import gears.async.Listener
import gears.async.native.NativeSuspend


import asyncio.Backend
import asyncio.ReactorBackend

/** gears' suspension support on Scala Native, with a pool of reactor workers. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorPool
}

/** Gears futures over thread-owned reactors on Scala Native: `run` starts a pool, and the rest is `OnPool`'s. */
object ReactorFutures {

  /** Runs `program` on one worker, the calling thread, and returns what it returns (see the other `run`). Scala
    * Native suspends futures on a pool's workers, which run reactors, so the backend must be a reactor backend. A
    * program's `Io` is not a `Backend`, so starting a run over one is refused when compiling.
    */
  def run[T](using backend: Backend)(program: backend.Io => (Async.Spawn, AsyncOperations) ?=> T): T =
    backend match {
      case reactors: (ReactorBackend & backend.type) => run(1)(using reactors)(program)
      case _ => throw new IllegalArgumentException(s"$backend has no reactors, and Scala Native runs futures on them")
    }

  /** Runs `program` on a pool of `workers` workers, each with a reactor of `backend`'s (see `ReactorPool`): the
    * calling thread becomes the first. The program starts as a future, is given the backend's `Io`, the backend with
    * its own capabilities bound to the pool, with which it performs its ops and which is its `AsyncOperations`; the
    * workers run until it finishes, and what it returns is returned. `policy` says which tasks a worker out of work
    * may take from another. A failure of the program is rethrown. With `GEARS_POOL_STATS=1`, the pool's counters are
    * printed once its workers stop, and each reactor as it prints itself.
    */
  def run[T](workers: Int, policy: TakePolicy = TakePolicy.Affine)(using backend: ReactorBackend)(
      program: backend.Io => (Async.Spawn, AsyncOperations) ?=> T
  ): T = {
    val stats = sys.env.get("GEARS_POOL_STATS").contains("1")
    ReactorPool.run(backend, workers, policy, stopped = pool => if stats then printStats(pool)) { pool =>
      given ReactorPool = pool
      given ReactorSupport.type = ReactorSupport
      val io = backend.bind(Pools.bound(backend, pool))
      given AsyncOperations = io
      Async.blocking {
        val main = Future(program(io)) // queued on the first worker: it starts with that worker's first round
        main.onComplete(Listener((_, _) => pool.stop()))
        pool.runFirstWorker()
        main.await // already complete, so this only rethrows a failure
      }
    }
  }

  /** Prints the pool's counters, once its workers have stopped: the pool's own, and each worker's reactor as its
    * backend prints it, since what a reactor counts is the backend's.
    */
  private def printStats(pool: ReactorPool): Unit = {
    val workers = pool.workers.filter(_ != null)
    System.err.println(
      s"pool stats (${workers.length} workers, per worker): taken ${workers.map(_.taken).mkString(" ")}; " +
        s"blocked ${workers.map(_.blocked).mkString(" ")}; wake-ups ${pool.wakeUps.get()}; " +
        s"injected ${pool.injections.get()}; reactors ${workers.map(_.reactor).mkString(" ")}"
    )
  }
}
