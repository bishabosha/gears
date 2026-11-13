package gears.async.io
package internal

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.FactoryVThreadSupport
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport
import gears.async.io.Backend
import gears.async.io.Io

import scala.util.Try

/** What `Io.run` does on the JVM: gears futures on virtual threads always, so that suspending one blocks only it: `run`
  * starts a program over a backend and gives it an `Io` over it, which performs its ops and is its `AsyncOperations`,
  * so that code calling `perform` is the same here as on Scala Native. The backend's kind says how its futures are
  * carried. A `ReactorBackend`, such as `NioBackend`, runs its program on a `ReactorPool` whose workers carry the
  * virtual threads and drive the reactors, as gears' futures are on Scala Native: an op goes to the reactor of the
  * worker carrying its future, which may finish it at once, and its completion resumes the future on that worker, under
  * the pool's `TakePolicy`. That needs `--add-opens java.base/java.lang=ALL-UNNAMED`, since the JDK keeps both a
  * virtual thread's scheduler and its carrier to itself, and `--add-opens java.base/jdk.internal.vm=ALL-UNNAMED`, for
  * the pin that keeps a future on its worker while its op is submitted there (see `VirtualCarriers.pin`). A
  * `CallerBackend`, such as `NioOnCaller`, the JVM's caller backend, runs its program on gears' own virtual threads on
  * the JDK's scheduler, each op performed on the future's own thread, and needs nothing of the JDK beyond its public
  * interface.
  */
object Runner {

  /** A pool's workers: one per processor available to the program. */
  private[io] def Workers: Int = Runtime.getRuntime().availableProcessors()

  /** Runs `program` on a pool of `workers` workers driving `backend`'s reactors, under `policy`: what a reactor
    * backend's `run` does with a worker per processor, and `internal.ReactorRuns`'s, whose callers choose them. The
    * program is given an `Io` over the backend on the pool, as a context parameter, and what it returns is returned; a
    * failure of the program is rethrown.
    */
  private[io] def onPool[T](workers: Int, policy: TakePolicy)(using
      backend: ReactorBackend
  )(program: Io[backend.Interface] ?=> Async.Spawn ?=> T): T = {
    VirtualCarriers.check()
    // The first worker is a thread of its own, not the caller's: a carrier's JDK caches are shared with the virtual
    // threads it carries, which the JDK fills assuming its carriers do nothing else. Its cache of native path buffers
    // (`sun.nio.fs.NativeBuffers`) remembers the path a platform thread last copied into a buffer, but not one a
    // virtual thread copied there since, so a caller that was a carrier and goes on to use the file system after the
    // run is given the buffer with a virtual thread's path in it: on JDK 27, `Files.isDirectory(dir)` after a run that
    // opened `dir/file` stats `dir/file`.
    @volatile var outcome: Try[T] | Null = null
    val first = new Thread(() => outcome = Try(onWorkers(workers, policy)(program)), "gears-io-worker-0")
    first.start()
    var interrupted = false
    while first.isAlive do
      try first.join()
      catch case _: InterruptedException => interrupted = true
    if interrupted then Thread.currentThread().interrupt()
    outcome.nn.get
  }

  /** `onPool` on the calling thread, which becomes the pool's first worker. */
  private def onWorkers[T](workers: Int, policy: TakePolicy)(using
      backend: ReactorBackend
  )(program: Io[backend.Interface] ?=> Async.Spawn ?=> T): T =
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

  /** Gives up the calling virtual thread's turn behind those already woken, for `OnCaller.yield`, and returns true.
    * Parking for a nanosecond unmounts it, and the scheduler's timer wakes it from outside the carriers' pool, so it
    * waits behind the threads woken before it, those whose ops' handles became ready among them. Neither gears' own
    * `yield` nor `Thread.yield` does: the first resumes the caller through a task spawned on the carrier, the second,
    * on JDK 27, gives the carrier back while it has nothing queued locally; either way it runs again before the
    * wake-ups from outside the pool, which a pool takes once its carriers' own queues are empty. Futures computing in
    * slices on every carrier, yielding between them, kept the ops' wake-ups from running: `starvation`'s round trips
    * once never ran again in 600 s, and every run with `Thread.yield` stalled.
    */
  private[io] def yieldHere(): Boolean = {
    java.util.concurrent.locks.LockSupport.parkNanos(1)
    true
  }

  /** Runs `program` on gears' own virtual threads, each op performed on the future's own thread. */
  private[io] def onCaller[T](b: CallerBackend)(program: Io[b.Interface] ?=> Async.Spawn ?=> T): T = {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    val io = OnCaller(b)
    given AsyncOperations = io
    // `Async.blocking` runs its body on the calling thread, which may be a platform thread; a future's is virtual.
    Async.blocking(Future(program(using io)).await)
  }
}
