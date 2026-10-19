package asyncio.gears

import gears.async.Cancellable

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.FiniteDuration

import asyncio.Backend

/** The thread of a pool's worker other than the first, which runs on the thread that started the pool. It holds its
  * worker, so that finding the worker running the caller takes no thread-local lookup beyond the current thread.
  */
private[gears] final class ReactorWorkerThread(
    val pool: ReactorPool,
    index: Int,
    started: java.util.concurrent.CountDownLatch
) extends Thread(s"reactor-worker-$index") {
  var worker: ReactorWorker | Null = null // on this thread only

  override def run(): Unit = {
    val mine = new ReactorWorker(pool, index, pool.backend.open()) // opened here, where it hears of closes
    worker = mine
    pool.workers(index) = mine
    started.countDown()
    started.await() // so that every worker this one may take tasks from is in `workers`
    mine.loop()
  }
}

/** A gears scheduler over a pool of workers, each a thread driving a reactor of `backend`'s (see `ReactorWorker`).
  * Futures inherit their parent's scheduler, so a program started by `ReactorPool.run`, and everything it spawns,
  * runs on the pool. A task queued on a worker runs there unless another worker, out of work, takes it; queued from
  * outside the pool, it goes to a shared queue, which every worker takes from. With one worker, everything runs on the
  * thread that called `run`.
  */
class ReactorPool private[gears] (val backend: Backend, size: Int, val policy: TakePolicy) extends ReactorScheduler {
  require(size >= 1 && size <= 64, "A pool has between 1 and 64 workers")

  /** The workers, by index; filled in as each starts on its own thread. */
  private[gears] val workers = new Array[ReactorWorker](size)

  // Tasks queued from outside the pool, and those that overflowed a worker's queue.
  private val injector = new ConcurrentLinkedQueue[Runnable]()

  // A bit per worker blocked in its reactor, or about to be, so that work can be offered to one of them.
  private val idle = new java.util.concurrent.atomic.AtomicLong(0L)

  /** Workers looking for work on others; while one is, offering work wakes no one else. */
  private[gears] val searching = new java.util.concurrent.atomic.AtomicInteger(0)

  @volatile private[gears] var stopped = false

  /** For measurement: wake-ups sent to idle workers, offering work and for tasks from outside. */
  private[gears] val wakeUps = new java.util.concurrent.atomic.AtomicLong
  private[gears] val injections = new java.util.concurrent.atomic.AtomicLong

  /** The thread the caller runs on, as the pool knows its workers: on the JVM, a virtual thread's carrier. */
  protected def callerThread(): Thread = Thread.currentThread()

  /** The worker running the caller, if it is one of this pool's. */
  def currentWorker: ReactorWorker | Null =
    callerThread() match {
      case thread: ReactorWorkerThread => if thread.pool eq this then thread.worker else null
      case thread                      =>
        val first = workers(0)
        if first != null && (first.owner eq thread) then first else null
    }

  /** Where an op performed from outside the pool goes: a worker picked by the caller's thread. */
  def driverFor(caller: Thread): ReactorDriver = workers(Math.floorMod(System.identityHashCode(caller), workers.length))

  /** Queues `body` on the worker running the caller, or on the shared queue from outside the pool. */
  def execute(body: Runnable): Unit = {
    val worker = currentWorker
    if worker != null then worker.push(body) else inject(body)
  }

  /** Queues `task` on the shared queue and wakes an idle worker other than `from`, the worker queueing it if any. */
  private[gears] def inject(task: Runnable, from: Int = -1): Unit = {
    injections.incrementAndGet()
    injector.add(task)
    wakeOne(from)
  }

  private[gears] def injected(): Runnable | Null = injector.poll()

  /** Worker `from` has more queued than it is about to run: wakes another idle worker to take some, unless one is
    * searching. `from` itself may still be marked idle, since a worker runs completions, which queue tasks, in the turn
    * it blocked in; but it is running, so it is not the one to wake.
    */
  private[gears] def offerWork(from: Int): Unit =
    if (idle.get() & ~(1L << from)) != 0L && searching.get() == 0 then wakeOne(from)

  /** Wakes an idle worker other than `except` (-1 for none). */
  private def wakeOne(except: Int = -1): Unit = {
    val others = if except >= 0 then ~(1L << except) else -1L
    var mask = idle.get() & others
    while mask != 0L do {
      val i = java.lang.Long.numberOfTrailingZeros(mask)
      val current = idle.get()
      if (current & (1L << i)) != 0L && idle.compareAndSet(current, current & ~(1L << i)) then {
        wakeUps.incrementAndGet()
        workers(i).reactor.wakeup()
        return
      }
      mask = idle.get() & others
    }
  }

  private[gears] def markIdle(index: Int): Unit = {
    var mask = idle.get()
    while !idle.compareAndSet(mask, mask | (1L << index)) do mask = idle.get()
  }

  private[gears] def unmarkIdle(index: Int): Unit = {
    var mask = idle.get()
    while (mask & (1L << index)) != 0L && !idle.compareAndSet(mask, mask & ~(1L << index)) do mask = idle.get()
  }

  /** Whether there is work `worker` could take: from outside the pool, or spare on another worker. */
  private[gears] def hasWorkFor(worker: ReactorWorker): Boolean =
    !injector.isEmpty || workers.exists(other => (other ne worker) && other.hasSpare)

  /** Runs `body` after `delay` on the worker running the caller, or on the first worker from outside the pool. */
  def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val worker = currentWorker
    (if worker != null then worker else workers(0)).schedule(delay, body)
  }

  /** Runs the first worker, on the thread that started the pool, until the pool stops. */
  private[gears] def runFirstWorker(): Unit = workers(0).loop()

  /** Stops every worker after its current round. */
  private[gears] def stop(): Unit = {
    stopped = true
    workers.foreach(worker => if worker != null then worker.reactor.wakeup())
  }
}

object ReactorPool {

  /** Runs a pool of `workers` workers, each with a reactor of `backend`'s: the calling thread becomes the first, and the
    * others get threads of their own, each opening its reactor there. `drive` then runs on the calling thread: it starts
    * the program, runs the first worker (`runFirstWorker`) until the program finishes and stops the pool, and rethrows
    * the program's failure. Each reactor is closed afterwards, on its own thread, then `stopped` is called with the
    * pool. `newPool` builds the pool, for a platform whose workers are found another way.
    */
  def run(
      backend: Backend,
      workers: Int,
      policy: TakePolicy,
      newPool: (Backend, Int, TakePolicy) => ReactorPool = new ReactorPool(_, _, _),
      stopped: ReactorPool => Unit = _ => ()
  )(drive: ReactorPool => Unit): Unit = {
    val pool = newPool(backend, workers, policy)
    // Each worker opens its reactor on its own thread, where the reactor hears of descriptors closed (see
    // `DescriptorHooks`).
    pool.workers(0) = new ReactorWorker(pool, 0, backend.open())
    val started = new java.util.concurrent.CountDownLatch(workers - 1)
    val threads = (1 until workers).map { i =>
      val thread = new ReactorWorkerThread(pool, i, started)
      thread.setDaemon(true)
      thread.start()
      thread
    }
    started.await() // the workers array is complete, and published to every worker, before any task runs
    try drive(pool)
    finally {
      pool.stop()
      threads.foreach(_.join())
      stopped(pool)
    }
  }
}
