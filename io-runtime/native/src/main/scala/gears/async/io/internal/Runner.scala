package gears.async.io
package internal

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Future
import gears.async.Listener
import gears.async.io.Backend
import gears.async.io.Io
import gears.async.native.NativeSuspend

/** gears' suspension support on Scala Native, with a pool of reactor workers. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorPool
}

/** What `Io.run` does on Scala Native, through a reactor backend's `run`: a pool of workers over thread-owned reactors,
  * the rest its `Io`'s (`OnPool`).
  */
object Runner {

  /** For `OnCaller.yield`: false, to leave it to gears' own, since no caller backend runs here. */
  private[io] def yieldHere(): Boolean = false

  /** A pool's workers: one per processor available to the program. */
  private[io] def Workers: Int = Runtime.getRuntime().availableProcessors()

  /** For a caller backend's `run`: refused, since Scala Native suspends futures on a pool's workers, so a backend there
    * must have reactors.
    */
  private[io] def onCaller[T](b: CallerBackend)(program: Io[b.Interface] ?=> Async.Spawn ?=> T): T =
    throw new IllegalArgumentException(s"$b has no reactors, and Scala Native runs futures on them")

  /** Runs `program` on a pool of `workers` workers driving `backend`'s reactors, under `policy`: what a reactor
    * backend's `run` does with a worker per processor, and `internal.ReactorRuns`'s, whose callers choose them. The
    * program is given an `Io` over the backend on the pool, as a context parameter, and what it returns is returned; a
    * failure of the program is rethrown.
    */
  private[io] def onPool[T](workers: Int, policy: TakePolicy)(using
      backend: ReactorBackend
  )(
      program: Io[backend.Interface] ?=> Async.Spawn ?=> T
  ): T = {
    FileLimit.raised: Unit // as the JVM does, so that a server's sockets are not capped at a shell's 256
    val stats = sys.env.get("GEARS_POOL_STATS").contains("1")
    ReactorPool.run(backend, workers, policy, stopped = pool => if stats then printStats(pool)) { pool =>
      given ReactorPool = pool
      given ReactorSupport.type = ReactorSupport
      val io = Pools.bound(backend, pool)
      given AsyncOperations = io
      Async.blocking {
        val main = Future(program(using io)) // queued on the first worker: it starts with that worker's first round
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
