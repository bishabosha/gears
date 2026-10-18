package asyncio.gears

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.native.NativeSuspend

import asyncio.Backend

/** gears' suspension support on Scala Native, with a pool of reactor workers. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorPool
}

/** Gears futures over thread-owned reactors on Scala Native: `run` starts a pool, and the rest is `ReactorAsync`'s. */
object ReactorFutures {
  export ReactorAsync.*

  /** Runs `program` on one worker, the calling thread (see the other `run`). */
  def run(program: Backend => Async.Spawn ?=> Unit)(using backend: Backend): Unit = run(1)(program)

  /** Runs `program` on a pool of `workers` workers (see `ReactorPool.run`), suspending futures with `ReactorSupport`.
    * With `GEARS_POOL_STATS=1`, the pool's counters are printed once its workers stop.
    */
  def run(workers: Int, policy: TakePolicy = TakePolicy.Affine)(program: Backend => Async.Spawn ?=> Unit)(using
      backend: Backend
  ): Unit = {
    val stats = sys.env.get("GEARS_POOL_STATS").contains("1")
    ReactorPool.run(ReactorSupport, workers, policy, pool => if stats then printStats(pool))(program)
  }

  /** Prints the pool's counters, once its workers have stopped. */
  private def printStats(pool: ReactorPool): Unit = {
    val workers = pool.workers.filter(_ != null)
    def kqueue(f: asyncio.kqueue.KqueueReactor => Long): String =
      workers.map(_.reactor match { case k: asyncio.kqueue.KqueueReactor => f(k); case _ => 0L }).mkString(" ")
    System.err.println(
      s"pool stats (${workers.length} workers, per worker): taken ${workers.map(_.taken).mkString(" ")}; " +
        s"blocked ${workers.map(_.blocked).mkString(" ")}; taken over ${kqueue(_.takenOver)}; " +
        s"filters dropped ${kqueue(_.filtersDropped)}; wake-ups ${pool.wakeUps.get()}; " +
        s"injected ${pool.injections.get()}"
    )
  }
}
