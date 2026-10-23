package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.FactoryVThreadSupport
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import asyncio.Backend
import asyncio.CallerBackend
import asyncio.ReactorBackend

/** Gears futures on the JVM, always virtual threads, so that suspending one blocks only it: `run` starts a program
  * over a backend and gives it the backend's `Io`, and the rest is `OnPool`'s, so that code calling `perform`
  * is the same here as on Scala Native. The backend's kind says how its futures are carried. A `ReactorBackend`,
  * such as `NioBackend`, runs its program on a `ReactorPool` whose workers carry the virtual threads and drive the
  * reactors, as gears' futures are on Scala Native: an op goes to the reactor of the worker carrying its future,
  * which may finish it at once, and its completion resumes the future on that worker, under the pool's `TakePolicy`.
  * That needs `--add-opens java.base/java.lang=ALL-UNNAMED`, since the JDK keeps both a virtual thread's scheduler
  * and its carrier to itself, and `--add-opens java.base/jdk.internal.vm=ALL-UNNAMED`, for the pin that keeps a
  * future on its worker while its op is submitted there (see `VirtualCarriers.pin`). A `CallerBackend`, such as
  * `NioOnCaller`, the default, runs its program on gears' own virtual threads on the JDK's scheduler, each op
  * performed on the future's own thread, and needs nothing of the JDK beyond its public interface.
  */
object ReactorFutures extends Runner {
  /** Runs `program` with one worker (see the other `run`). */
  def run(using backend: Backend)(program: backend.Io => (Async.Spawn, AsyncOperations) ?=> Unit): Unit = run(1)(program)

  /** Runs `program` as a future on a virtual thread, and gives it the backend's `Io`: the backend, with its own
    * capabilities, bound to where the program runs, with which it performs its ops, and which is also its
    * `AsyncOperations`, so that gears' `sleep` and `withTimeout` run on the backend's timers. A failure of the program
    * is rethrown.
    *
    * A `ReactorBackend` gets `workers` reactors of its own, on a pool of workers that carry the program's futures,
    * which run until the program finishes and are closed afterwards; `policy` says which tasks a worker out of work
    * may take from another. A `CallerBackend` runs the program on gears' own virtual threads, where each op is
    * performed on the future's own thread, and has no use for workers or a policy.
    */
  def run(workers: Int, policy: TakePolicy = TakePolicy.Affine)(using
      backend: Backend
  )(program: backend.Io => (Async.Spawn, AsyncOperations) ?=> Unit): Unit = backend.start(this, workers, policy)(program)

  def onCaller(b: CallerBackend)(program: b.Io => (Async.Spawn, AsyncOperations) ?=> Unit): Unit = {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    val io = b.bind()
    given AsyncOperations = io
    // `Async.blocking` runs its body on the calling thread, which may be a platform thread; a future's is virtual.
    Async.blocking(Future(program(io)).await)
  }

  /** Runs `program` on virtual threads carried by a pool's workers, with the backend bound to the pool; the calling
    * thread is the first worker.
    */
  def onPool(b: ReactorBackend, workers: Int, policy: TakePolicy)(program: b.Io => (Async.Spawn, AsyncOperations) ?=> Unit): Unit = {
    VirtualCarriers.check()
    ReactorPool.run(b, workers, policy, newPool = new CarriedPool(_, _, _)) { pool =>
      val factory = VirtualCarriers.factory(pool.execute(_))
      val scheduler = new VirtualReactorScheduler(pool, factory)
      val io = b.bind(scheduler)
      given AsyncOperations = io
      given support: FactoryVThreadSupport[VirtualReactorScheduler] = new FactoryVThreadSupport(factory)
      given VirtualReactorScheduler = scheduler
      @volatile var failure: Throwable | Null = null
      // Started from this thread, the first worker's, so the program's first continuation is queued there.
      factory
        .newThread { () =>
          try Async.blocking(program(io))
          catch case t: Throwable => failure = t
          finally pool.stop()
        }
        .start()
      pool.runFirstWorker()
      if failure != null then throw failure
    }
  }
}
