package gearsexample

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Future
import gears.async.Listener
import gears.async.Scheduler
import gears.async.native.NativeSuspend

import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import asyncio.Completion
import asyncio.Reactor

/** A gears scheduler that runs everything on the thread that created it, which also drives that thread's reactor:
  * `ReactorFutures.run` alternates between running queued tasks, such as starting a future or resuming a suspended one,
  * and turns of the reactor's loop. So code running under this scheduler submits ops to the reactor directly.
  *
  * Futures inherit their parent's scheduler, so a program started here, and everything it spawns, runs on this one
  * thread. There is no work stealing or balancing yet.
  */
final class ReactorScheduler private[gearsexample] (val reactor: Reactor) extends Scheduler {
  private val owner = Thread.currentThread()

  // Tasks queued on the reactor thread, almost all of them: resuming a continuation is queued by a completion, which
  // runs there. A plain array deque, used only from that thread, allocates nothing per task.
  private val local = new java.util.ArrayDeque[Runnable]()

  // Tasks queued from other threads, which wake the loop. `drain` moves them to `local`.
  private val remote = new ConcurrentLinkedQueue[Runnable]()

  ReactorScheduler.current.set(this)

  def isCurrent: Boolean = Thread.currentThread() eq owner

  /** Queues `body` to run on the reactor thread, from any thread. Only another thread has to wake the loop: on the
    * reactor thread itself the loop is not blocked, and the task runs before it next blocks.
    */
  def execute(body: Runnable): Unit =
    if isCurrent then local.add(body)
    else {
      remote.add(body)
      reactor.wakeup()
    }

  /** Ops that `perform` may still finish without suspending in this round while no other task is queued, reset by each
    * `drain`. Spent, `perform` suspends even for an op that could finish at once, so the reactor's poll gets to run.
    */
  private[gearsexample] var fastPathsLeft = ReactorScheduler.FastPathBudget

  /** Ops that `perform` may still finish without suspending in the running task's turn, even with other tasks queued,
    * reset before each task. Spent, `perform` suspends, so a future whose ops rarely wait still gives up its turn.
    */
  private[gearsexample] var turnFastPathsLeft = 0

  /** Whether no task is queued, so the loop may block. On the reactor thread only. */
  private[gearsexample] def isIdle: Boolean = local.isEmpty && remote.isEmpty

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

  /** Runs the tasks queued when it starts. Tasks they queue wait for the next drain, after the reactor's turn: work
    * that keeps queueing more, such as a computation yielding between slices, would otherwise keep I/O from ever being
    * polled.
    */
  private[gearsexample] def drain(): Unit = {
    fastPathsLeft = ReactorScheduler.FastPathBudget
    var fromElsewhere = remote.poll()
    while fromElsewhere != null do {
      local.add(fromElsewhere)
      fromElsewhere = remote.poll()
    }
    var remaining = local.size
    while remaining > 0 do {
      val task = local.poll()
      remaining -= 1
      turnFastPathsLeft = ReactorScheduler.TurnFastPathBudget
      try task.run()
      catch case t: Throwable => t.printStackTrace()
    }
  }
}

object ReactorScheduler {

  /** How many ops `perform` may finish without suspending in one round of the scheduler. */
  inline val FastPathBudget = 32

  /** How many ops `perform` may finish without suspending in one task's turn while other tasks are queued. */
  inline val TurnFastPathBudget = 1

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

  /** Submits `op`, which must have been built by `r.ops`, and suspends the caller until the reactor finishes with it:
    * it returns when the op completes, throws its failure, or throws `CancellationException` when it is cancelled. It
    * must be called on the thread that owns `r`, which is where a program started by `run` executes. To run an op
    * alongside other work, call this inside a `Future`.
    *
    * Cancelling the caller asks the reactor to cancel the op, but this keeps waiting until the reactor confirms, so
    * once it returns or throws, the op's buffers and slots belong to the caller again.
    */
  def perform(r: Reactor, op: r.Op)(using ac: Async): Unit = {
    // The caller's scheduler, if it is the reactor's, runs everything on the reactor's thread, so the caller is on it
    // too; asking it saves a thread-local lookup, which on macOS is a call.
    val scheduler = ac.scheduler match {
      case own: ReactorScheduler if own.reactor eq r => own
      case _ => throw new IllegalStateException("Ops must be submitted on the thread that owns the reactor")
    }
    if ac.group.isCancelled then throw new CancellationException()
    // An op performed again reuses its waiter, kept as its attachment, so that performing it allocates nothing here.
    // A waiter still in use (the op performed twice at once, which the reactor refuses) is left alone.
    val waiter = r.attachment(op) match {
      case reused: OpWaiter if !reused.inUse && (reused.scheduler eq scheduler) =>
        reused.reset()
        reused
      case _ =>
        val fresh = new OpWaiter(scheduler, r, op)
        r.attach(op, fresh)
        fresh
    }
    waiter.inUse = true
    try {
      // The fast path: an op the reactor finishes at once returns here without suspending, which skips the whole await
      // and resume. Taken within a small budget per turn of the caller, so a future whose ops rarely wait still gives
      // the turn to those queued beside it, and beyond that only while no other task is queued, within a budget per
      // round of the scheduler, so the reactor still polls for futures waiting on events.
      // A refused op, such as a one-shot op submitted twice, throws from either call.
      val finished =
        if scheduler.turnFastPathsLeft > 0 then {
          val now = r.trySubmit(op, waiter.completion)
          if now then scheduler.turnFastPathsLeft -= 1
          now
        } else if scheduler.fastPathsLeft > 0 && scheduler.isIdle then {
          val now = r.trySubmit(op, waiter.completion)
          if now then scheduler.fastPathsLeft -= 1
          now
        } else {
          r.submit(op, waiter.completion)
          false
        }
      if !finished then {
        waiter.link() // Cancelling the caller now cancels the op; if it already was, this cancels it at once.
        // Awaited outside the caller's group, so cancellation cannot resume it before the reactor has confirmed.
        try ac.withGroup(CompletionGroup.Unlinked).await(waiter).get
        finally waiter.unlink()
      }
    } finally waiter.inUse = false
  }

  private val done = Success(())

  /** An op's submissions, one at a time, each as a source that its completion completes once and as a member of the
    * caller's cancellation group. It has at most one listener, the suspended caller. Kept as the op's attachment and
    * reset for each submission, so it, its state and its completion are allocated once per op.
    */
  private final class OpWaiter(val scheduler: ReactorScheduler, reactor: Reactor, op: Any)
      extends Async.Source[Try[Unit]]
      with Cancellable {

    /** Whether a `perform` is using it. On the reactor thread only. */
    var inUse = false

    // Counts submissions, so that a cancel requested from another thread for one cannot reach the next.
    @volatile private var generation = 0

    /** Readies it for the op's next submission, after the previous one has finished. */
    def reset(): Unit = {
      generation += 1
      state = OpWaiter.Empty
    }

    // Empty, then either the suspended caller's listener or the outcome, and finally the outcome, so exactly one side
    // delivers the outcome: `finish` to a listener it finds, or `onComplete` to itself when it finds the outcome
    // already there. On the reactor thread only, where completions run, where the caller awaits, and where `cancel`
    // reads it, so a plain field: Scala Native's `AtomicReference` computes a `ClassTag` on every operation.
    private var state: AnyRef = OpWaiter.Empty

    // Completions are contravariant, so this one fits the op of any reactor.
    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    private def finish(outcome: Try[Unit]): Unit = {
      val previous = state
      state = outcome
      previous match {
        case k: Listener[Try[Unit]] @unchecked => k.completeNow(outcome, this)
        case _                                 => ()
      }
    }

    // gears' `await` calls this first; the default builds a listener and an `Option` to find out.
    override def poll(): Option[Try[Unit]] =
      state match {
        case outcome: Try[Unit] @unchecked => Some(outcome)
        case _                             => None
      }

    def poll(k: Listener[Try[Unit]]): Boolean =
      state match {
        case outcome: Try[Unit] @unchecked =>
          k.completeNow(outcome, this)
          true
        case _ => false
      }

    def onComplete(k: Listener[Try[Unit]]): Unit =
      state match {
        case outcome: Try[Unit] @unchecked => k.completeNow(outcome, this)
        case OpWaiter.Empty                => state = k
        case _                             => () // Only the one caller ever waits.
      }

    def dropListener(k: Listener[Try[Unit]]): Unit = if state eq k then state = OpWaiter.Empty

    // Cancellation can come from any thread, so it is carried out on the reactor's own. By then the op may have
    // finished, and a repeatable op may even have been submitted again, which must not be cancelled.
    def cancel(): Unit = {
      val submission = generation
      scheduler.sync(
        if submission == generation && !state.isInstanceOf[Try[?]] then
          reactor.cancel(op.asInstanceOf[reactor.Op]) // `op` was built by `reactor`, as `perform` requires
      )
    }
  }

  private object OpWaiter {
    private val Empty = new Object
  }

  /** Runs `program` on the calling thread, which becomes the reactor thread: it opens a reactor from the factory,
    * starts the program as a future under a scheduler bound to this thread, and runs the reactor's loop until the
    * program finishes. The reactor is closed afterwards, and a failure of the program is rethrown.
    */
  def run(program: Reactor => Async.Spawn ?=> Unit)(using factory: Reactor.Factory[Reactor]): Unit = {
    val reactor = factory.open()
    try {
      val scheduler = new ReactorScheduler(reactor)
      given ReactorScheduler = scheduler
      given ReactorSupport.type = ReactorSupport
      Async.blocking {
        val main = Future(program(reactor)) // queued: it starts with the first drain
        // Set once, by the reactor thread, when the program completes; polling `main` would allocate every turn.
        var finished = false
        main.onComplete(Listener((_, _) => finished = true))
        while !finished do {
          scheduler.drain()
          // Block for events only with no task left; a task queued from another thread wakes the loop.
          if !finished then reactor.runOnce(block = scheduler.isIdle)
        }
        main.await // already complete, so this only rethrows a failure
      }
    } finally reactor.close() // Pending ops are cancelled, rejecting any futures still waiting on them.
  }
}
