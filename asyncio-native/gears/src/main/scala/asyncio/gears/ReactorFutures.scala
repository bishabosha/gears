package asyncio.gears

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.Future
import gears.async.Listener
import gears.async.native.NativeSuspend

import asyncio.Backend
import asyncio.CallerBackend
import asyncio.ReactorBackend

/** gears' suspension support on Scala Native, with a pool of reactor workers. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorPool
}

/** Gears futures over thread-owned reactors on Scala Native: `run` starts a pool, and the rest is `ReactorAsync`'s. */
object ReactorFutures extends Runner {
  export ReactorAsync.*

  /** Runs `program` on one worker, the calling thread (see the other `run`). */
  def run(using backend: Backend)(program: backend.Io => Async.Spawn ?=> Unit): Unit = run(1)(program)

  /** Runs `program` on a pool of `workers` workers, each with a reactor of `backend`'s, which must be a
    * `ReactorBackend` (see `ReactorPool`): the calling thread becomes the first. The program starts as a future, is
    * given the backend's `Io`, the backend with its own capabilities bound to the pool, with which it performs its
    * ops; the workers run until it finishes. `policy` says which tasks a worker out of work may take from another. A
    * failure of the program is rethrown. With `GEARS_POOL_STATS=1`, the pool's counters are printed once its workers
    * stop.
    */
  def run(workers: Int, policy: TakePolicy = TakePolicy.Affine)(using backend: Backend)(
      program: backend.Io => Async.Spawn ?=> Unit
  ): Unit = backend.start(this, workers, policy)(program)

  /** Scala Native suspends futures on a pool's workers, which run reactors; there is no other carrier. */
  def onCaller(b: CallerBackend)(program: b.Io => Async.Spawn ?=> Unit): Unit =
    throw new UnsupportedOperationException(s"$b performs ops on the caller, and Scala Native runs futures on reactors")

  def onPool(b: ReactorBackend, workers: Int, policy: TakePolicy)(program: b.Io => Async.Spawn ?=> Unit): Unit = {
    val stats = sys.env.get("GEARS_POOL_STATS").contains("1")
    ReactorPool.run(b, workers, policy, stopped = pool => if stats then printStats(pool)) { pool =>
      given ReactorPool = pool
      given ReactorSupport.type = ReactorSupport
      val io = b.bind(pool)
      Async.blocking {
        val main = Future(program(io)) // queued on the first worker: it starts with that worker's first round
        main.onComplete(Listener((_, _) => pool.stop()))
        pool.runFirstWorker()
        main.await // already complete, so this only rethrows a failure
      }
    }
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
