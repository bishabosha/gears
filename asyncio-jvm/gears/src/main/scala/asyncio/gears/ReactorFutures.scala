package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.FactoryVThreadSupport
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import scala.util.NotGiven
import scala.util.Try

import asyncio.Backend
import asyncio.CallerBackend
import asyncio.OnPool
import asyncio.ReactorBackend

/** Gears futures on the JVM, always virtual threads, so that suspending one blocks only it: `run` starts a program
  * over a backend and gives it the backend's `Io`, which performs its ops and is its `AsyncOperations`, so that code
  * calling `perform` is the same here as on Scala Native. The backend's kind says how its futures are carried. A
  * `ReactorBackend`, such as `NioBackend`, runs its program on a `ReactorPool` whose workers carry the virtual threads
  * and drive the reactors, as gears' futures are on Scala Native: an op goes to the reactor of the worker carrying its
  * future, which may finish it at once, and its completion resumes the future on that worker, under the pool's
  * `TakePolicy`. That needs `--add-opens java.base/java.lang=ALL-UNNAMED`, since the JDK keeps both a virtual thread's
  * scheduler and its carrier to itself, and `--add-opens java.base/jdk.internal.vm=ALL-UNNAMED`, for the pin that keeps
  * a future on its worker while its op is submitted there (see `VirtualCarriers.pin`). A `CallerBackend`, such as
  * `NioOnCaller`, the default, runs its program on gears' own virtual threads on the JDK's scheduler, each op
  * performed on the future's own thread, and needs nothing of the JDK beyond its public interface.
  */
object ReactorFutures {

  /** Runs `program` over `backend`, whichever its kind, and returns what it returns: on a pool of one worker for a
    * reactor backend (see the other `run`), on gears' own virtual threads for a caller backend. A program's `Io` on a
    * pool is an `Io` and so a `Backend`, but not one to start a run over: that is refused when compiling.
    */
  def run[T](using backend: Backend)(using NotGiven[backend.type <:< OnPool])(
      program: backend.Io => (Async.Spawn, AsyncOperations) ?=> T
  ): T =
    backend match {
      case reactors: (ReactorBackend & backend.type) => run(1)(using reactors)(program)
      case caller: (CallerBackend & backend.type)    => onCaller(caller)(program)
      case other => throw new IllegalArgumentException(s"$other is neither a reactor backend nor a caller backend")
    }

  /** Runs `program` on a pool of `workers` workers driving reactors of `backend`'s, which carry the program's futures,
    * run until it finishes and are closed afterwards; `policy` says which tasks a worker out of work may take from
    * another. The program is given the backend's `Io`, bound to the pool, and what it returns is returned; a failure
    * of the program is rethrown.
    */
  def run[T](workers: Int, policy: TakePolicy = TakePolicy.Affine)(using
      backend: ReactorBackend
  )(program: backend.Io => (Async.Spawn, AsyncOperations) ?=> T): T = {
    VirtualCarriers.check()
    ReactorPool.run(backend, workers, policy, newPool = new CarriedPool(_, _, _)) { pool =>
      val factory = VirtualCarriers.factory(pool.execute(_))
      val scheduler = new VirtualReactorScheduler(pool, factory)
      val io = backend.bind(Pools.bound(backend, scheduler))
      given AsyncOperations = io
      given support: FactoryVThreadSupport[VirtualReactorScheduler] = new FactoryVThreadSupport(factory)
      given VirtualReactorScheduler = scheduler
      @volatile var outcome: Try[T] | Null = null
      // Started from this thread, the first worker's, so the program's first continuation is queued there.
      factory
        .newThread { () =>
          try outcome = Try(Async.blocking(program(io)))
          finally pool.stop()
        }
        .start()
      pool.runFirstWorker()
      outcome.nn.get
    }
  }

  /** Runs `program` on gears' own virtual threads, each op performed on the future's own thread. */
  private def onCaller[T](b: CallerBackend)(program: b.Io => (Async.Spawn, AsyncOperations) ?=> T): T = {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    val io = b.bind()
    given AsyncOperations = io
    // `Async.blocking` runs its body on the calling thread, which may be a platform thread; a future's is virtual.
    Async.blocking(Future(program(io)).await)
  }
}
