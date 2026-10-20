package asyncio.gears

import gears.async.Cancellable

import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.concurrent.Executor
import java.util.concurrent.ThreadFactory
import scala.concurrent.duration.FiniteDuration

import asyncio.ReactorBackend

/** Virtual threads carried by a `ReactorPool`'s workers, through what the JDK keeps to itself: a virtual thread builder
  * that takes the executor to run its threads on, and the carrier of the running virtual thread. Both need
  * `--add-opens java.base/java.lang=ALL-UNNAMED`.
  */
private[gears] object VirtualCarriers {

  private def opened[A](what: String)(get: => A): A =
    try get
    catch {
      case e: (ReflectiveOperationException | RuntimeException) =>
        throw new UnsupportedOperationException(
          s"Running virtual threads on the reactor pool needs $what, which the JDK opens only with " +
            "--add-opens java.base/java.lang=ALL-UNNAMED",
          e
        )
    }

  private lazy val builder = opened("ThreadBuilders.VirtualThreadBuilder(Executor)") {
    val constructor =
      Class.forName("java.lang.ThreadBuilders$VirtualThreadBuilder").getDeclaredConstructor(classOf[Executor])
    constructor.setAccessible(true)
    constructor
  }

  private lazy val carrier: MethodHandle = opened("Thread.currentCarrierThread") {
    MethodHandles
      .privateLookupIn(classOf[Thread], MethodHandles.lookup())
      .findStatic(classOf[Thread], "currentCarrierThread", MethodType.methodType(classOf[Thread]))
  }

  /** Checks, before any thread starts, that the JDK lets the pool carry virtual threads. */
  def check(): Unit = {
    builder
    carrier: Unit
  }

  /** Makes virtual threads that `executor` runs: it is given each thread's continuation whenever it is to run. */
  def factory(executor: Executor): ThreadFactory =
    builder.newInstance(executor).asInstanceOf[Thread.Builder.OfVirtual].name("reactor-virtual-", 0L).factory()

  /** The platform thread running the caller: the carrier of a virtual thread, or the caller's own thread. */
  def currentCarrier(): Thread = carrier.invoke().asInstanceOf[Thread]
}

/** A pool whose workers carry virtual threads, so the worker running a caller is its carrier's. */
private[gears] final class CarriedPool(backend: ReactorBackend, size: Int, policy: TakePolicy)
    extends ReactorPool(backend, size, policy) {
  override protected def callerThread(): Thread = VirtualCarriers.currentCarrier()
}

/** The gears scheduler of a program whose futures are virtual threads carried by `pool`: a task is a new virtual
  * thread, whose continuations the pool runs; a delayed one waits on a reactor's timer.
  */
private[gears] final class VirtualReactorScheduler(val pool: ReactorPool, factory: ThreadFactory)
    extends ReactorScheduler {
  def backend: ReactorBackend = pool.backend
  def currentWorker: ReactorWorker | Null = pool.currentWorker
  def driverFor(caller: Thread): ReactorDriver = pool.driverFor(caller)

  def execute(body: Runnable): Unit = factory.newThread(body).start()

  def schedule(delay: FiniteDuration, body: Runnable): Cancellable = pool.schedule(delay, () => execute(body))
}
