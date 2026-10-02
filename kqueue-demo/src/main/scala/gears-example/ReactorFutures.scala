package gearsexample

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.Cancellable
import gears.async.Future
import gears.async.Listener
import gears.async.Scheduler
import gears.async.native.NativeSuspend

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

import asyncio.Completion
import asyncio.Promise
import asyncio.Reactor

/** A gears scheduler that runs everything on the thread that created it, using that thread's reactor as its event loop:
  * queued tasks, such as starting a future or resuming a suspended one, run from a completion on the reactor. So code
  * running under this scheduler submits ops to the reactor directly.
  *
  * Futures inherit their parent's scheduler, so a program started here, and everything it spawns, runs on this one
  * thread. There is no work stealing or balancing yet.
  */
final class ReactorScheduler private[gearsexample] (val reactor: Reactor) extends Scheduler {
  private val owner = Thread.currentThread()
  private val tasks = new ConcurrentLinkedQueue[Runnable]()
  @volatile private var wakeup: Wakeup | Null = null

  ReactorScheduler.current.set(this)
  arm()

  def isCurrent: Boolean = Thread.currentThread() eq owner

  /** Queues `body` to run on the reactor thread, from any thread. */
  def execute(body: Runnable): Unit = {
    tasks.add(body)
    val w = wakeup
    if w != null then w.fire()
  }

  /** Runs `body` after `delay` on the reactor thread, using a reactor timer. */
  def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val cancelTimer = new AtomicReference[(() => Unit) | Null](null)
    onOwner {
      val timer = reactor.ops.timer(delay.toMillis.toInt)
      reactor.submit(timer, _ => body.run())
      cancelTimer.set(() => reactor.cancel(timer))
    }
    () =>
      onOwner {
        val cancel = cancelTimer.get()
        if cancel != null then cancel()
      }
  }

  /** Runs `body` now if called on the reactor thread, or queues it there otherwise. */
  private[ReactorScheduler] inline def onOwner(body: => Unit): Unit =
    if isCurrent then body else execute(() => body)

  def sync(body: => Unit): Unit = onOwner(body)

  /** One pending promise that wakes the loop for queued tasks. It fires at most once. */
  private final class Wakeup(promise: Promise[Unit]) {
    private val fired = new AtomicBoolean()
    def fire(): Unit = if fired.compareAndSet(false, true) then promise.complete(())
  }

  /** Submits a fresh wakeup and publishes it before draining, so a task queued concurrently is either seen by the drain
    * or wakes the new promise.
    */
  private def arm(): Unit = {
    val promise = reactor.ops.promise()
    reactor.submit(
      promise,
      _ => {
        arm()
        drain()
      }
    )
    wakeup = new Wakeup(promise)
  }

  private def drain(): Unit = {
    var task = tasks.poll()
    while task != null do {
      try task.nn.run()
      catch case t: Throwable => t.printStackTrace()
      task = tasks.poll()
    }
  }
}

object ReactorScheduler {
  private val current = new ThreadLocal[ReactorScheduler]()

  /** The scheduler running on this thread, if any. */
  def onThisThread: ReactorScheduler | Null = current.get()
}

/** gears' suspension support on Scala Native, with a reactor scheduler. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorScheduler
}

/** Gears futures over a thread-owned reactor. */
object ReactorFutures {

  /** Submits `op`, which must have been built by `r.ops`, and returns a future that completes when the op does. It must
    * be called on the thread that owns `r`, which is where a program started by `run` executes. The future is linked to
    * the caller's group, so cancelling the caller, or the future itself, asks the reactor to cancel the op; the future
    * is rejected as cancelled when the reactor confirms it, which also happens if the reactor closes first.
    */
  def submit(r: Reactor, op: r.Op)(using Async): Future[Unit] = {
    val scheduler = {
      val local = ReactorScheduler.onThisThread
      if local == null || (local.reactor ne r) then
        throw new IllegalStateException("Ops must be submitted on the thread that owns the reactor")
      local
    }
    Future
      .withResolver[Unit] { resolver =>
        // Cancellation can come from any thread, so it is carried out on the reactor's own.
        resolver.onCancel(() => scheduler.sync(r.cancel(op)))
        // An op the reactor refuses, such as a one-shot op submitted twice, fails the future instead of the caller.
        try
          scheduler.sync {
            r.submit(
              op,
              new Completion[r.Op] {
                def onComplete(op: r.Op): Unit = resolver.resolve(())
                override def onFailure(op: r.Op, failure: Throwable): Unit = resolver.reject(failure)
                override def onCancel(op: r.Op): Unit = resolver.rejectAsCancelled()
              }
            )
          }
        catch case t: Throwable => resolver.reject(t)
      }
      .link()
  }

  /** Runs `program` on the calling thread, which becomes the reactor thread: it opens a reactor from the factory,
    * starts the program as a future under a scheduler bound to this thread, and runs the reactor's loop until the
    * program finishes. The reactor is closed afterwards, and a failure of the program is rethrown.
    */
  def run(program: Reactor => Async.Spawn ?=> Unit)(using factory: Reactor.Factory[Reactor]): Unit = {
    val reactor = factory.open()
    try {
      given ReactorScheduler = new ReactorScheduler(reactor)
      given ReactorSupport.type = ReactorSupport
      Async.blocking {
        val main = Future(program(reactor)) // queued: it starts once the loop runs
        main.onComplete(Listener((_, _) => reactor.stop()))
        reactor.run()
        main.await // already complete, so this only rethrows a failure
      }
    } finally reactor.close() // Pending ops are cancelled, rejecting any futures still waiting on them.
  }
}
