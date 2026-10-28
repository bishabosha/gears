package gears.async.io

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.FactoryVThreadSupport
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport
import gears.async.io.Backend
import gears.async.io.Io
import gears.async.io.internal.*
import gears.async.io.internal.CallerBackend
import gears.async.io.internal.ReactorBackend

import scala.util.Try

/** Gears futures on the JVM, always virtual threads, so that suspending one blocks only it: `run` starts a program over
  * a backend and gives it an `Io` over it, which performs its ops and is its `AsyncOperations`, so that code calling
  * `perform` is the same here as on Scala Native. The backend's kind says how its futures are carried. A
  * `ReactorBackend`, such as `NioBackend`, runs its program on a `ReactorPool` whose workers carry the virtual threads
  * and drive the reactors, as gears' futures are on Scala Native: an op goes to the reactor of the worker carrying its
  * future, which may finish it at once, and its completion resumes the future on that worker, under the pool's
  * `TakePolicy`. That needs `--add-opens java.base/java.lang=ALL-UNNAMED`, since the JDK keeps both a virtual thread's
  * scheduler and its carrier to itself, and `--add-opens java.base/jdk.internal.vm=ALL-UNNAMED`, for the pin that keeps
  * a future on its worker while its op is submitted there (see `VirtualCarriers.pin`). A `CallerBackend`, such as
  * `NioOnCaller`, the JVM's caller backend, runs its program on gears' own virtual threads on the JDK's scheduler, each
  * op performed on the future's own thread, and needs nothing of the JDK beyond its public interface.
  */
object ReactorFutures {

  /** Runs `program` over `backend`, whichever its kind, and returns what it returns: on a pool of one worker for a
    * reactor backend (see the other `run`), on gears' own virtual threads for a caller backend. A program's `Io` is not
    * a `Backend`, so starting a run over one is refused when compiling.
    */
  def run[T](using backend: Backend)(program: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    backend match {
      case reactors: (ReactorBackend & backend.type) => onPool(1, TakePolicy.Affine)(using reactors)(program)
      case caller: (CallerBackend & backend.type)    => onCaller(caller)(program)
      case other => throw new IllegalArgumentException(s"$other is neither a reactor backend nor a caller backend")
    }

  /** Runs `program` on a pool of `workers` workers driving reactors of `backend`'s, which carry the program's futures,
    * run until it finishes and are closed afterwards; `policy` says which tasks a worker out of work may take from
    * another. The program is given an `Io` over the backend on the pool, as a context parameter, and what it returns is
    * returned; a failure of the program is rethrown.
    */
  def run[T](workers: Int, policy: TakePolicy = TakePolicy.Affine)(using
      backend: Backend
  )(program: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    backend match {
      case reactors: (ReactorBackend & backend.type) => onPool(workers, policy)(using reactors)(program)
      case _                                         =>
        throw new IllegalArgumentException(
          s"$backend performs its ops on the caller, so a pool of workers has nothing to run; use `run` without workers"
        )
    }

  /** The run on a pool, over a reactor backend. */
  private def onPool[T](workers: Int, policy: TakePolicy)(using
      backend: ReactorBackend
  )(program: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T = {
    VirtualCarriers.check()
    ReactorPool.run(backend, workers, policy, newPool = new CarriedPool(_, _, _)) { pool =>
      val factory = VirtualCarriers.factory(pool.execute(_))
      val scheduler = new VirtualReactorScheduler(pool, factory)
      val io = Pools.bound(backend, scheduler)
      given AsyncOperations = io
      given support: FactoryVThreadSupport[VirtualReactorScheduler] = new FactoryVThreadSupport(factory)
      given VirtualReactorScheduler = scheduler
      @volatile var outcome: Try[T] | Null = null
      // Started from this thread, the first worker's, so the program's first continuation is queued there.
      factory
        .newThread { () =>
          try outcome = Try(Async.blocking(program(using io)))
          finally pool.stop()
        }
        .start()
      pool.runFirstWorker()
      outcome.nn.get
    }
  }

  /** Runs `program` on gears' own virtual threads, each op performed on the future's own thread. */
  private def onCaller[T](b: CallerBackend)(program: Io[b.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T = {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    val io = OnCaller[b.type](b)
    given AsyncOperations = io
    // `Async.blocking` runs its body on the calling thread, which may be a platform thread; a future's is virtual.
    Async.blocking(Future(program(using io)).await)
  }
}
