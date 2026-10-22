package asyncio.gears

import gears.async.Async
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Listener
import gears.async.cancellationScope

import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import asyncio.Backend
import asyncio.Completion
import asyncio.Reactor
import asyncio.ReactorBackend

/** What gears futures do with a program's backend: perform ops, on any backend, and, on a reactor backend, submit
  * them; and yield. The same on every platform. For a reactor backend, each platform's `ReactorFutures.run` starts the
  * program and its reactors: on a `ReactorPool`, whose workers run both the futures and the reactors, so an op goes to
  * the reactor of the worker running the caller; or, on the JVM by default, beside gears' own scheduler, so that an op
  * is handed to a reactor's thread (see `OpRouter`).
  */
object ReactorAsync {

  /** Performs `op`, which must have been built by `b.ops`, and suspends the caller until it is done, as `b.perform`
    * says. To run an op alongside other work, call this inside a `Future`.
    */
  def perform(b: Backend, op: b.Op)(using Async): Unit = b.perform(op)

  /** Performs `op` with a time limit, as `b.perform` says: `TimeoutException` if it is not done `timeoutMillis`
    * milliseconds after it was started, the op stopped and its buffers and slots the caller's again. A read with a
    * timeout is this, not a race against a timer.
    */
  def perform(b: Backend, op: b.Op, timeoutMillis: Int)(using Async): Unit = b.perform(op, timeoutMillis)

  /** Submits `op`, which must have been built by `b.ops`, to a reactor of the caller's program, and suspends the
    * caller until the reactor finishes with it: it returns when the op completes, throws its failure, throws
    * `CancellationException` when it is cancelled, or throws `TimeoutException` when its time limit, `timeoutMillis`
    * unless negative, ran out first (see `Submitter.submit`). It must be called from a program that
    * `ReactorFutures.run` started with `b`. `b` only says what the op is; which reactor runs it depends on where the
    * caller runs: on a pool's worker, that worker's, and otherwise one picked by the caller's thread. A repeatable op
    * may go to a different reactor each time. This is `ReactorBackend.perform`.
    *
    * Cancelling the caller asks the reactor to cancel the op, but this keeps waiting until the reactor confirms, so
    * once it returns or throws, the op's buffers and slots belong to the caller again.
    */
  def performOnReactor(b: ReactorBackend, op: b.Op, timeoutMillis: Int)(using ac: Async): Unit = {
    val router = ac.scheduler match {
      // A pool's own scheduler, as on Scala Native: matched on the class, the common case, ahead of the general one.
      case pool: ReactorPool if pool.backend eq b => pool
      case _                                      => routerFor(b)
    }
    // Pinned to its thread from finding the worker to submitting on it (see `OpRouter.pin`): `performHere` unpins
    // once the op is submitted, before it waits.
    router.pin()
    val worker = router.currentWorker
    if worker != null then performHere(b, op, worker, router, timeoutMillis)
    else {
      router.unpin()
      performElsewhere(b, op, router.driverFor(Thread.currentThread()), timeoutMillis)
    }
  }

  /** `perform` on the worker running the caller, which `pinned` keeps there until the op is submitted: an op the
    * reactor finishes at once returns without suspending. The reactor keeps the time limit.
    */
  private def performHere(
      b: ReactorBackend,
      op: b.Op,
      worker: ReactorWorker,
      pinned: OpRouter,
      timeoutMillis: Int
  )(using ac: Async): Unit = {
    val r = worker.reactor.asInstanceOf[Reactor.Of[b.Op]] // its reactor is one of `b`'s
    // An op performed again reuses its waiter, kept as its attachment, so that performing it allocates nothing here,
    // even when it last ran on another worker. A waiter still in use (the op performed twice at once, which the
    // reactor refuses) is left alone.
    val waiter =
      try {
        if ac.group.isCancelled then throw new CancellationException()
        b.attachment(op) match {
          case reused: OpWaiter if !reused.inUse =>
            reused.reset(worker)
            reused
          case _ =>
            val fresh = new OpWaiter(worker, op)
            b.attach(op, fresh)
            fresh
        }
      } catch {
        case t: Throwable =>
          pinned.unpin()
          throw t
      }
    waiter.inUse = true
    try {
      // The fast path: an op the reactor finishes at once returns here without suspending, which skips the whole await
      // and resume. Taken within a small budget per turn of the caller, so a future whose ops rarely wait still gives
      // the turn to those queued beside it, and beyond that only while no other task is queued, within a budget per
      // round of the scheduler, so the reactor still polls for futures waiting on events.
      // A refused op, such as a one-shot op submitted twice, throws from either call.
      val finished =
        try
          if worker.turnFastPathsLeft > 0 then {
            val now = r.trySubmit(op, waiter.completion, timeoutMillis)
            if now then worker.turnFastPathsLeft -= 1
            now
          } else if worker.fastPathsLeft > 0 && worker.isIdle then {
            val now = r.trySubmit(op, waiter.completion, timeoutMillis)
            if now then worker.fastPathsLeft -= 1
            now
          } else {
            r.submit(op, waiter.completion, timeoutMillis)
            false
          }
        finally pinned.unpin() // before the await parks, which pinned would park the worker's thread
      if !finished then {
        waiter.link() // Cancelling the caller now cancels the op; if it already was, this cancels it at once.
        // Awaited outside the caller's group, so cancellation cannot resume it before the reactor has confirmed.
        try ac.withGroup(CompletionGroup.Unlinked).await(waiter).get
        finally waiter.unlink()
      }
    } finally waiter.inUse = false
  }

  /** `perform` from a caller on no worker: the op is handed to `driver`'s thread, which submits it, and the caller
    * suspends until its completion, which runs there, resumes it. An op's time limit runs from here: what the op
    * spends being tried and waited for on the caller counts, and an op whose time runs out there, having done nothing
    * yet, throws `TimeoutException` without reaching a reactor.
    */
  private def performElsewhere(b: ReactorBackend, op: b.Op, driver: ReactorDriver, timeoutMillis: Int)(using
      ac: Async
  ): Unit = {
    if ac.group.isCancelled then throw new CancellationException()
    val deadline = if timeoutMillis < 0 then -1L else System.nanoTime() + timeoutMillis * 1_000_000L
    val waiter = b.attachment(op) match {
      case reused: RemoteWaiter if !reused.inUse => reused
      case _                                     =>
        val fresh = new RemoteWaiter(driver, op)
        b.attach(op, fresh)
        fresh
    }
    // A blocking op runs here: the caller may block anyway, and is on no reactor's thread. Cancelling the caller
    // interrupts it, as cancelling the op on a reactor would. One with a time limit goes to a reactor, whose loop
    // interrupts it when its time is up, as nothing here could.
    val here = if deadline < 0 then b.blockingHere(op) else null
    if here != null then {
      cancellationScope(new Cancellable { def cancel(): Unit = here.cancel() })(here.run())
      return
    }
    // An op the backend can finish here, on the caller's thread, skips both hand-offs. A run of them gives up the
    // thread now and then, as a pool's fast path gives up its turn, so the futures beside the caller still run.
    if waiter.finishedHere >= RemoteWaiter.HereBudget then {
      waiter.finishedHere = 0
      Thread.`yield`()
    }
    if b.tryNow(op) then {
      waiter.finishedHere += 1
      return
    }
    // Before handing the op over, yield and try again a few times: the futures beside the caller run meanwhile, and
    // the data often arrives, so the op needs no reactor and no wake-up. A yield puts the caller behind every other
    // thread ready to run, so this stays fair. Only for an op the backend can finish here at all, and only while few
    // ops wait on the reactors: with many, each turn of a reactor serves many of them at once, which tries on every
    // caller's thread, each a system call on a handle, would only slow.
    val contends = b.triesNow(op)
    val few = RemoteWaiter.waiting.get() < RemoteWaiter.RetryBelow
    var tries = if contends && few then RemoteWaiter.RetriesHere else 0
    while tries > 0 do {
      tries -= 1
      Thread.`yield`()
      if ac.group.isCancelled then throw new CancellationException()
      if b.tryNow(op) then {
        waiter.finishedHere += 1
        return
      }
      if deadline >= 0 && System.nanoTime() >= deadline then throw new TimeoutException(s"$op timed out")
    }
    waiter.finishedHere = 0
    // Wait here, parked in the caller's own thread, rather than on a reactor: no thread is woken to register the
    // wait, and none to resume the caller, which under load the OS may keep from the reactor's thread for
    // milliseconds. An op whose tries contend for the kernel waits here only while few ops wait; one that contends
    // for nothing, such as a timer, always. Cancelling the caller ends the wait.
    if RemoteWaiter.WaitHere && (few || !contends) then {
      val here = b.waitHere(op)
      if here != null then {
        RemoteWaiter.waiting.incrementAndGet()
        try
          cancellationScope(here) {
            while true do {
              val remaining = if deadline < 0 then -1L else math.max(deadline - System.nanoTime(), 0L)
              val done = here.await(remaining)
              if ac.group.isCancelled then throw new CancellationException()
              if done || b.tryNow(op) then return
              if deadline >= 0 && System.nanoTime() >= deadline then throw new TimeoutException(s"$op timed out")
            }
          }
        finally RemoteWaiter.waiting.decrementAndGet()
      }
    }
    waiter.reset(driver)
    waiter.inUse = true
    try {
      // Linked first: a cancel is handed to the same thread after the submission, so it finds the op pending.
      waiter.link()
      // The reactor takes over the time limit: what is left of it, since the op was started here.
      val left = if deadline < 0 then timeoutMillis else (math.max(deadline - System.nanoTime(), 0L) / 1_000_000L).toInt
      driver.sync(
        try submitOn(driver, op, waiter.completion, left)
        catch case t: Throwable => waiter.refuse(t) // a refused op, such as a one-shot op submitted twice
      )
      RemoteWaiter.waiting.incrementAndGet()
      try waiter.await().get
      finally {
        RemoteWaiter.waiting.decrementAndGet()
        waiter.unlink()
      }
    } finally waiter.inUse = false
  }

  /** Submits `op`, which must have been built by `b.ops`, to the reactor of the worker running the caller, with a
    * completion of the caller's own instead of suspending: the completion runs on that worker's thread, which may not
    * be the caller's by then, since a task may resume on another worker. The returned handle cancels the op there.
    */
  def submit(b: ReactorBackend, op: b.Op, completion: Completion[b.Op])(using ac: Async): Submission =
    submit(b, op, completion, Backend.NoTimeout)

  /** `submit` with a time limit, which the reactor keeps (see `Submitter.submit`). */
  def submit(b: ReactorBackend, op: b.Op, completion: Completion[b.Op], timeoutMillis: Int)(using
      ac: Async
  ): Submission = {
    val router = ac.scheduler match {
      case pool: ReactorPool if pool.backend eq b => pool // the common case, matched on the class
      case _                                      => routerFor(b)
    }
    router.pin() // as `performOnReactor` does, until the op is submitted on the worker found
    val worker = router.currentWorker
    if worker != null then {
      // Its reactor is one of `b`'s.
      try worker.reactor.asInstanceOf[Reactor.Of[b.Op]].submit(op, completion, timeoutMillis)
      finally router.unpin()
      new Submission(worker, op)
    } else {
      router.unpin()
      val driver = router.driverFor(Thread.currentThread())
      // Its ops are `b`'s, as is the reactor.
      driver.sync(submitOn(driver, op, completion.asInstanceOf[Completion[Any]], timeoutMillis))
      new Submission(driver, op)
    }
  }

  /** An op submitted with `submit`, which it cancels on the reactor it went to. */
  final class Submission private[ReactorAsync] (driver: ReactorDriver, op: Any) {

    /** Cancels the op, from any thread: at once if called on the worker it went to, and returns true; otherwise on that
      * worker's thread, soon, and returns false, so the op is not free to submit again until its completion's
      * `onCancel` has run.
      */
    def cancel(): Boolean =
      if driver.isCurrent then {
        cancelOn(driver, op)
        true
      } else {
        driver.sync(cancelOn(driver, op))
        false
      }
  }

  /** Submits `op` to `driver`'s reactor, on its thread; the op was built by the backend whose reactor it is. */
  private def submitOn(driver: ReactorDriver, op: Any, completion: Completion[Any], timeoutMillis: Int): Unit = {
    val r = driver.reactor
    r.submit(op.asInstanceOf[r.Op], completion, timeoutMillis)
  }

  /** Cancels `op` on `driver`'s reactor, on its thread (see `submitOn`). */
  private def cancelOn(driver: ReactorDriver, op: Any): Unit = {
    val r = driver.reactor
    r.cancel(op.asInstanceOf[r.Op]): Unit
  }

  /** The reactors of the caller's program, which must have been started with `b`: through the caller's scheduler, if
    * it is a pool, or else those the runner set up beside gears' own scheduler.
    */
  private def routerFor(b: ReactorBackend)(using ac: Async): OpRouter = {
    val router = ac.scheduler match {
      case router: OpRouter => router
      case _                   => OpRouter.ambient.get()
    }
    if router == null || !(router.backend eq b) then
      throw new IllegalStateException("Ops must be performed by a program that ReactorFutures.run started with their backend")
    router
  }

  /** Gives up the caller's turn: suspends it and queues its resume behind the tasks already queued, so the futures
    * sharing its thread run first, as a long computation should between slices. Cheaper than awaiting a future spawned
    * to the same end. Throws `CancellationException` if the caller has been cancelled by the time it resumes.
    */
  def yieldNow()(using ac: Async): Unit = {
    ac.withGroup(CompletionGroup.Unlinked).await(Yield)
    if ac.group.isCancelled then throw new CancellationException()
  }

  /** Never ready when polled, and completes a listener as soon as it is added, so that awaiting it resumes the caller
    * from the back of its scheduler's queue.
    */
  private object Yield extends Async.Source[Unit] {
    override def poll(): Option[Unit] = None
    def poll(k: Listener[Unit]): Boolean = false
    def onComplete(k: Listener[Unit]): Unit = k.completeNow((), this)
    def dropListener(k: Listener[Unit]): Unit = ()
  }

  private val done = Success(())

  /** An op's submissions, one at a time, each as a source that its completion completes once and as a member of the
    * caller's cancellation group. It has at most one listener, the suspended caller. Kept as the op's attachment and
    * reset for each submission, so it, its state and its completion are allocated once per op. Each submission may be
    * on a different worker; `scheduler` is the current one's.
    */
  private final class OpWaiter(first: ReactorWorker, op: Any) extends Async.Source[Try[Unit]] with Cancellable {

    /** Whether a `perform` is using it. On the thread performing the op only. */
    var inUse = false

    /** The worker, and so the reactor, of the current submission. Read by `cancel` from any thread. */
    @volatile private var worker = first

    // Counts submissions, so that a cancel requested from another thread for one cannot reach the next.
    @volatile private var generation = 0

    /** Readies it for the op's next submission, on `next`'s reactor, after the previous one has finished. The worker
      * changes before the generation, so a `cancel` that reads the new generation also finds the new worker.
      */
    def reset(next: ReactorWorker): Unit = {
      if worker ne next then worker = next // usually the same, and a volatile write costs more than a read
      generation += 1
      state.set(OpWaiter.Empty)
    }

    // Empty, then either the suspended caller's listener or the outcome, and finally the outcome, so exactly one side
    // delivers the outcome: `finish` to a listener it finds, or `onComplete` to itself when it finds the outcome
    // already there. The completion runs on the reactor's thread; the caller registers its listener on the thread
    // it is on by then, which on the JVM may be another worker's, since a virtual thread is pinned only until its op
    // is submitted (see `OpRouter.pin`), so the two race, and the field is atomic. On Scala Native nothing moves the
    // caller, and a plain field would do, but measured against this it is within noise (kqueue ping-pong 8.74 against
    // 8.67 µs), so one shared field it is.
    private val state = new AtomicReference[AnyRef](OpWaiter.Empty)

    // Completions are contravariant, so this one fits the op of any reactor.
    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    private def finish(outcome: Try[Unit]): Unit =
      state.getAndSet(outcome) match {
        case k: Listener[Try[Unit]] @unchecked => k.completeNow(outcome, this)
        case _                                 => ()
      }

    // gears' `await` calls this first; the default builds a listener and an `Option` to find out.
    override def poll(): Option[Try[Unit]] =
      state.get() match {
        case outcome: Try[Unit] @unchecked => Some(outcome)
        case _                             => None
      }

    def poll(k: Listener[Try[Unit]]): Boolean =
      state.get() match {
        case outcome: Try[Unit] @unchecked =>
          k.completeNow(outcome, this)
          true
        case _ => false
      }

    def onComplete(k: Listener[Try[Unit]]): Unit = {
      var done = false
      while !done do
        state.get() match {
          case outcome: Try[Unit] @unchecked =>
            k.completeNow(outcome, this)
            done = true
          case OpWaiter.Empty => done = state.compareAndSet(OpWaiter.Empty, k) // or the outcome arrived: look again
          case _              => done = true // Only the one caller ever waits.
        }
    }

    def dropListener(k: Listener[Try[Unit]]): Unit = state.compareAndSet(k, OpWaiter.Empty): Unit

    // Cancellation can come from any thread, so it is carried out on the thread of the reactor the op was submitted to.
    // By then the op may have finished, and a repeatable op may even have been submitted again, there or elsewhere,
    // which must not be cancelled.
    def cancel(): Unit = {
      val submission = generation
      val target = worker
      target.sync(
        if submission == generation && !state.get().isInstanceOf[Try[?]] then
          cancelOn(target, op)
      )
    }
  }

  private object OpWaiter {
    val Empty = new Object
  }

  private object RemoteWaiter {

    /** How many submissions of one op in a row may finish on the caller's thread before it yields. */
    inline val HereBudget = 32

    /** How many times an op the backend could not finish on the caller's thread is tried again there, after a yield
      * each time, before it is handed to a reactor (`-Dasyncio.gears.retriesHere`): 8, as `VThreadBackend` yields
      * before it waits.
      */
    val RetriesHere: Int = sys.props.get("asyncio.gears.retriesHere").map(_.toInt).getOrElse(8)

    /** An op is tried again on the caller, and waited for there, only while fewer than this many ops are waiting,
      * on a reactor or on their callers (`-Dasyncio.gears.retryBelow`): with more, the reactor's one `select` serves
      * them all. 32, where the bench loses no throughput to threads contending in the kernel; higher cuts the
      * worst-case lateness of a wake-up at that price (see the NIO TODO, item 11).
      */
    val RetryBelow: Int = sys.props.get("asyncio.gears.retryBelow").map(_.toInt).getOrElse(32)

    /** Whether an op that the retries did not finish waits on the caller's thread, where the backend can wait there,
      * rather than on a reactor (`-Dasyncio.gears.waitHere`, true by default); for measuring the one against the other.
      */
    val WaitHere: Boolean = sys.props.get("asyncio.gears.waitHere").forall(_.toBoolean)

    /** Ops waiting, on a reactor or on their caller's thread, not yet finished, from every caller. */
    val waiting = new java.util.concurrent.atomic.AtomicInteger()
  }

  /** An op's submissions from a caller on no worker, which waits for each by parking its own thread, as a thread that
    * may block anyway can: the completion, on the reactor's thread, leaves the outcome and unparks it. A member of the
    * caller's cancellation group while the op is pending. Kept as the op's attachment and reset for each submission.
    */
  private final class RemoteWaiter(first: ReactorDriver, op: Any) extends Cancellable {

    /** Whether a `perform` is using it. On the thread performing the op only. */
    var inUse = false

    /** Submissions in a row that the backend finished on the caller's thread (`ReactorBackend.tryNow`). Caller's
      * thread only.
      */
    var finishedHere = 0

    @volatile private var driver = first
    @volatile private var generation = 0
    @volatile private var outcome: Try[Unit] | Null = null
    @volatile private var caller: Thread | Null = null

    /** Readies it for the op's next submission, on `next`'s reactor, by the calling thread. */
    def reset(next: ReactorDriver): Unit = {
      if driver ne next then driver = next
      generation += 1
      outcome = null
      caller = Thread.currentThread()
    }

    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    /** The reactor refused the submission: it fails with `failure`. */
    def refuse(failure: Throwable): Unit = finish(Failure(failure))

    private def finish(result: Try[Unit]): Unit = {
      outcome = result
      LockSupport.unpark(caller)
    }

    /** Parks the caller until the submission's outcome is in. An interrupt does not end the wait: the op is still the
      * reactor's until it says otherwise.
      */
    def await(): Try[Unit] = {
      var result = outcome
      while result == null do {
        LockSupport.park(this)
        result = outcome
      }
      result
    }

    // Carried out on the driver's thread, where the submission went, if it is still the current one and pending.
    def cancel(): Unit = {
      val submission = generation
      val target = driver
      target.sync(
        if submission == generation && outcome == null then
          cancelOn(target, op)
      )
    }
  }

}
