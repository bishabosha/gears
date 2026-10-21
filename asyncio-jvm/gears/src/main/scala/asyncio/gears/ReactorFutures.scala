package asyncio.gears

import gears.async.Async
import gears.async.FactoryVThreadSupport
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import asyncio.Backend
import asyncio.ReactorBackend

/** What carries a JVM program's futures: always virtual threads, so that suspending one blocks only it. */
enum Carriers {

  /** Gears' own virtual threads, on the JDK's scheduler, with the reactors on threads of their own: each op is handed
    * to a reactor's thread, which runs its completion and resumes the future. Needs nothing of the JDK beyond its
    * public interface. The default.
    */
  case Jdk

  /** Virtual threads carried by a `ReactorPool`'s workers, which also drive the reactors, as gears' futures are on
    * Scala Native: an op goes to the reactor of the worker carrying its future, which may finish it at once, and its
    * completion resumes the future on that worker, under the pool's `TakePolicy`. Needs
    * `--add-opens java.base/java.lang=ALL-UNNAMED`, since the JDK keeps both a virtual thread's scheduler and its
    * carrier to itself, and `--add-opens java.base/jdk.internal.vm=ALL-UNNAMED`, for the pin that keeps a future on
    * its worker while its op is submitted there (see `VirtualCarriers.pin`).
    */
  case Reactors
}

object Carriers {

  /** What carries a program's futures when `run` is not told: the system property `asyncio.gears.carriers`, `jdk` or
    * `reactors`, so that code written for every platform can be run either way; `Jdk` without it.
    */
  def default: Carriers =
    sys.props.get("asyncio.gears.carriers") match {
      case None       => Carriers.Jdk
      case Some(name) =>
        Carriers.values
          .find(_.toString.equalsIgnoreCase(name))
          .getOrElse(throw new IllegalArgumentException(s"asyncio.gears.carriers is jdk or reactors, not `$name`"))
    }
}

/** Gears futures on the JVM: `run` starts a program over a backend's reactors, or over none for a backend that has
  * none, and the rest is `ReactorAsync`'s, so that code calling `perform` is the same here as on Scala Native.
  */
object ReactorFutures {
  export ReactorAsync.*

  /** Runs `program` with one reactor (see the other `run`). */
  def run(using backend: Backend)(program: backend.type => Async.Spawn ?=> Unit): Unit = run(1)(program)

  /** Runs `program` as a future on a virtual thread, and gives it the backend, with its own type, so that it may use
    * the capabilities that backend has; it performs its ops with `perform`. A failure of the program is rethrown.
    *
    * A `ReactorBackend` gets `workers` reactors of its own, which run until the program finishes and are closed
    * afterwards, its futures carried by `carriers` (see `Carriers`; by default, as `Carriers.default` says); `policy`
    * says, with `Carriers.Reactors`, which tasks a worker out of work may take from another. Any other backend performs
    * its ops on the futures' own threads, which are gears' virtual threads, and has no use for workers or a policy;
    * carrying its futures on reactors that are not there is refused.
    */
  def run(workers: Int, policy: TakePolicy = TakePolicy.Affine, carriers: Carriers = Carriers.default)(using
      backend: Backend
  )(program: backend.type => Async.Spawn ?=> Unit): Unit =
    backend match {
      case reactors: ReactorBackend =>
        carriers match {
          case Carriers.Jdk      => runBesideGears(reactors, workers)(program(backend))
          case Carriers.Reactors => runOnReactors(reactors, workers, policy)(program(backend))
        }
      case _ =>
        if carriers == Carriers.Reactors then
          throw new IllegalArgumentException(s"$backend has no reactors to carry futures on: use Carriers.Jdk")
        given VThreadSupport.type = VThreadSupport
        given VThreadScheduler.type = VThreadScheduler
        // `Async.blocking` runs its body on the calling thread, which may be a platform thread; a future's is virtual.
        Async.blocking(Future(program(backend)).await)
    }

  /** Runs `program` on gears' own virtual threads, which find the reactors, a pool of workers carrying no futures,
    * through `OpRouter.ambient`.
    */
  private def runBesideGears(backend: ReactorBackend, workers: Int)(program: Async.Spawn ?=> Unit): Unit = {
    val pool = ReactorPool.start(backend, workers)
    val previous = OpRouter.ambient.get()
    OpRouter.ambient.set(pool) // inherited by the virtual thread that `Async.blocking` runs the program on
    try {
      given VThreadSupport.type = VThreadSupport
      given VThreadScheduler.type = VThreadScheduler
      Async.blocking(program)
    } finally {
      OpRouter.ambient.set(previous)
      pool.stop() // Pending ops are cancelled as each reactor closes, rejecting any futures still waiting on them.
      pool.join()
    }
  }

  /** Runs `program` on virtual threads carried by a pool's workers; the calling thread is the first worker. */
  private def runOnReactors(backend: ReactorBackend, workers: Int, policy: TakePolicy)(
      program: Async.Spawn ?=> Unit
  ): Unit = {
    VirtualCarriers.check()
    ReactorPool.run(backend, workers, policy, newPool = new CarriedPool(_, _, _)) { pool =>
      val factory = VirtualCarriers.factory(pool.execute(_))
      val scheduler = new VirtualReactorScheduler(pool, factory)
      given support: FactoryVThreadSupport[VirtualReactorScheduler] = new FactoryVThreadSupport(factory)
      given VirtualReactorScheduler = scheduler
      @volatile var failure: Throwable | Null = null
      // Started from this thread, the first worker's, so the program's first continuation is queued there.
      factory
        .newThread { () =>
          try Async.blocking(program)
          catch case t: Throwable => failure = t
          finally pool.stop()
        }
        .start()
      pool.runFirstWorker()
      if failure != null then throw failure
    }
  }
}
