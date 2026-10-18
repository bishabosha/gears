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

import asyncio.Backend
import asyncio.Completion
import asyncio.Reactor

/** One worker of a `ReactorPool`: a thread, the reactor it drives, and the tasks queued for it. Its loop alternates
  * between running queued tasks, such as starting a future or resuming a suspended one, and turns of the reactor; with
  * nothing to run, it looks for work on the other workers, and failing that blocks in its reactor until an event, or a
  * worker with work to spare, wakes it. Code running on a worker submits ops to its reactor directly.
  *
  * Its reactor is one of `backend`'s, opened on the worker's own thread, so it runs any op the backend builds.
  */
final class ReactorWorker private[gearsexample] (val pool: ReactorPool, val index: Int, val reactor: Reactor) {

  /** The thread it runs on, which created it. */
  private[gearsexample] val owner = Thread.currentThread()

  // Tasks for this worker, almost all of them: resuming a continuation is queued by a completion, which runs here. A
  // fixed ring that allocates nothing per task, and from which other workers take tasks when they run out.
  private[gearsexample] val local = new RunQueue

  // Tasks that must run on this worker in particular, queued from other threads, which wake it: cancelling an op
  // pending on its reactor, or arming a timer there. `drain` runs them first, never through `local`, from which other
  // workers take tasks.
  private val inbox = new ConcurrentLinkedQueue[Runnable]()

  // Under the affine and hybrid policies, tasks resumed by this worker's reactor, which no other worker may take: a
  // task doing I/O here stays, with its descriptors. On this worker's thread only.
  private val home = new java.util.ArrayDeque[Runnable]()

  // Whether the lone task in `local` waits behind resumes in `home`, so that other workers may take it: set when that
  // happens, and cleared once a drain has run the resumes. Read by other workers; `loneShared` is the owner's copy, so
  // that the usual case, with nothing to share, neither writes nor reads it.
  @volatile private var loneSpare = false
  private var loneShared = false

  /** Offers the lone task in `local`, which waits behind resumes, to idle workers. */
  private def shareLone(): Unit = {
    if !loneShared then {
      loneShared = true
      loneSpare = true
    }
    pool.offerWork(index)
  }

  // Whether this worker is in a turn of its reactor, which runs completions: a task queued meanwhile from this thread
  // is a resume from the reactor, of an op finished there.
  private var inTurn = false

  def isCurrent: Boolean = Thread.currentThread() eq owner

  /** Queues `task` here, on this worker's thread only. A resume from this worker's reactor stays here under the affine
    * and hybrid policies; anything else goes where other workers may take it, a full queue overflowing to the pool's
    * shared queue, and with more queued there than this worker is about to run, an idle worker is woken to take some.
    */
  private[gearsexample] def push(task: Runnable): Unit =
    if inTurn && pool.policy != TakePolicy.Free then home.add(task)
    else if !local.push(task) then pool.inject(task, index)
    else {
      val queued = local.size
      if queued > 1 then pool.offerWork(index)
      else if queued == 1 && !home.isEmpty then shareLone()
    }

  /** Whether another worker may take some of this one's tasks: one is queued behind others that this worker is about
    * to run first, its resumes or another queued task. A lone queued task is left to it, since taking it would only move
    * it. Read from any thread.
    */
  private[gearsexample] def hasSpare: Boolean = {
    val queued = local.size
    queued > 1 || queued == 1 && loneSpare
  }

  /** A turn of this worker's reactor. */
  private def turn(block: Boolean): Unit = {
    inTurn = true
    try reactor.runOnce(block)
    finally inTurn = false
    if !home.isEmpty && local.size == 1 then shareLone()
  }

  /** Runs `body` on this worker's thread: now if called there, or queued and the worker woken otherwise. */
  def sync(body: => Unit): Unit =
    if isCurrent then body
    else {
      inbox.add(() => body)
      reactor.wakeup()
    }

  /** Ops that `perform` may still finish without suspending in this round while no other task is queued, reset by each
    * `drain`. Spent, `perform` suspends even for an op that could finish at once, so the reactor's poll gets to run.
    */
  private[gearsexample] var fastPathsLeft = ReactorWorker.FastPathBudget

  /** Ops that `perform` may still finish without suspending in the running task's turn, even with other tasks queued,
    * reset before each task. Spent, `perform` suspends, so a future whose ops rarely wait still gives up its turn.
    */
  private[gearsexample] var turnFastPathsLeft = 0

  /** For measurement, on this worker's thread only: tasks taken from other workers, and the times it blocked. */
  private[gearsexample] var taken = 0L
  private[gearsexample] var blocked = 0L

  /** Whether no task is queued here. On this worker's thread only. */
  private[gearsexample] def isIdle: Boolean = local.isEmpty && home.isEmpty && inbox.isEmpty

  /** Runs `body` after `delay`, using a timer of this worker's reactor; the returned cancel may be called anywhere. */
  private[gearsexample] def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val cancelTimer = new AtomicReference[(() => Unit) | Null](null)
    sync {
      val timer = reactor.ops.timer(delay.toMillis.toInt)
      reactor.submit(timer, _ => body.run())
      cancelTimer.set(() => reactor.cancel(timer))
    }
    () =>
      sync {
        val cancel = cancelTimer.get()
        if cancel != null then cancel()
      }
  }

  /** Runs the tasks queued when it starts. Tasks they queue wait for the next drain, after the reactor's turn: work
    * that keeps queueing more, such as a computation yielding between slices, would otherwise keep I/O from ever being
    * polled.
    */
  private[gearsexample] def drain(): Unit = {
    fastPathsLeft = ReactorWorker.FastPathBudget
    // Tasks for this worker in particular run first, here: in `local`, another worker could take one and call this
    // worker's reactor from its own thread. They run as though from the reactor's turn, so that a task they resume,
    // such as one whose op a cancel finished, stays here under the affine and hybrid policies. Those queued meanwhile
    // wait for the next drain, which the loop does not block before.
    var mine = ReactorWorker.InboxPerDrain
    var task = inbox.poll()
    inTurn = true
    try
      while task != null do {
        runTask(task)
        mine -= 1
        task = if mine > 0 then inbox.poll() else null
      }
    finally inTurn = false
    // Then some from outside the pool join the local ones. Once `local` is full, the one that did not fit runs now
    // and the rest wait for the next drain: a task it runs could queue more here meanwhile.
    var fromOutside = ReactorWorker.InjectedPerDrain
    task = if fromOutside > 0 then pool.injected() else null
    while task != null do {
      fromOutside -= 1
      if !local.push(task) then {
        runTask(task)
        task = null
      } else task = if fromOutside > 0 then pool.injected() else null
    }
    // Under the hybrid policy, a backlog of resumes here is shared: half of it goes where idle workers may take it.
    if pool.policy == TakePolicy.Hybrid && home.size > ReactorWorker.HybridBacklog then {
      var shared = home.size / 2
      while shared > 0 && local.push(home.poll()) do shared -= 1
      pool.offerWork(index)
    }
    // Resumes from this worker's reactor first, then the rest.
    var resumes = home.size
    while resumes > 0 do {
      runTask(home.poll())
      resumes -= 1
    }
    if loneShared then {
      loneShared = false
      loneSpare = false
    }
    var remaining = local.size
    while remaining > 0 do {
      val next = local.pop()
      remaining -= 1
      if next != null then runTask(next)
    }
  }

  private def runTask(task: Runnable): Unit = {
    turnFastPathsLeft = ReactorWorker.TurnFastPathBudget
    try task.run()
    catch case t: Throwable => t.printStackTrace()
  }

  /** Takes tasks from another worker with some to spare (see `hasSpare`), or from outside the pool, and returns whether
    * it found any.
    */
  private def takeWork(): Boolean = {
    val injected = pool.injected()
    if injected != null then {
      push(injected)
      return true
    }
    val workers = pool.workers
    if workers.length > 1 then {
      pool.searching.incrementAndGet()
      try {
        val start = java.util.concurrent.ThreadLocalRandom.current().nextInt(workers.length)
        var i = 0
        while i < workers.length do {
          val source = workers((start + i) % workers.length)
          if (source ne this) && source.hasSpare then {
            val moved = source.local.moveHalfTo(local)
            if moved > 0 then {
              taken += moved
              return true
            }
          }
          i += 1
        }
      } finally pool.searching.decrementAndGet()
    }
    false
  }

  /** Runs until the pool stops, then closes the reactor. */
  private[gearsexample] def loop(): Unit = {
    try
      while !pool.stopped do {
        drain()
        if !pool.stopped then
          if !isIdle || takeWork() then turn(block = false)
          else {
            // About to block: say so first, then look again, so that a worker queueing work meanwhile either sees this
            // one idle and wakes it, or has its work seen here.
            pool.markIdle(index)
            if !isIdle || pool.hasWorkFor(this) then {
              pool.unmarkIdle(index)
              turn(block = false)
            } else {
              blocked += 1
              turn(block = true)
              pool.unmarkIdle(index)
            }
          }
      }
    finally reactor.close() // Pending ops are cancelled, rejecting any futures still waiting on them.
  }
}

object ReactorWorker {

  /** How many ops `perform` may finish without suspending in one round of a worker. */
  inline val FastPathBudget = 32

  /** How many ops `perform` may finish without suspending in one task's turn while other tasks are queued. */
  inline val TurnFastPathBudget = 1

  /** How many tasks queued for a worker in particular it runs in each round, at most. */
  inline val InboxPerDrain = 256

  /** How many tasks from outside the pool a worker takes into each round, at most. */
  inline val InjectedPerDrain = 32

  /** Under the hybrid policy, how many resumes a worker keeps to itself before sharing half of them. */
  inline val HybridBacklog = 16
}

/** Which of a worker's tasks other workers may take when they run out of work. */
enum TakePolicy {

  /** Any queued task. Resumes then join a worker's queue in order with everything else, so a task a resume spawns waits
    * for the next round, after a turn of the reactor, where under the other policies it runs in the same round: a new
    * connection's first read, in `connections`, takes twice as long, even with one worker.
    */
  case Free

  /** Only tasks that are not resumes from the worker's own reactor, such as new futures and yields: a task doing I/O
    * stays on the worker whose reactor has its descriptors. The default.
    */
  case Affine

  /** As `Affine`, except that a worker with a backlog of resumes shares half of it. */
  case Hybrid
}

/** The thread of a pool's worker other than the first, which runs on the thread that started the pool. It holds its
  * worker, so that finding the worker running the caller takes no thread-local lookup beyond the current thread.
  */
private[gearsexample] final class ReactorWorkerThread(
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
  * Futures inherit their parent's scheduler, so a program started by `ReactorFutures.run`, and everything it spawns,
  * runs on the pool. A task queued on a worker runs there unless another worker, out of work, takes it; queued from
  * outside the pool, it goes to a shared queue, which every worker takes from. With one worker, everything runs on the
  * thread that called `run`.
  */
final class ReactorPool private[gearsexample] (val backend: Backend, size: Int, val policy: TakePolicy)
    extends Scheduler {
  require(size >= 1 && size <= 64, "A pool has between 1 and 64 workers")

  /** The workers, by index; filled in as each starts on its own thread. */
  private[gearsexample] val workers = new Array[ReactorWorker](size)

  // Tasks queued from outside the pool, and those that overflowed a worker's queue.
  private val injector = new ConcurrentLinkedQueue[Runnable]()

  // A bit per worker blocked in its reactor, or about to be, so that work can be offered to one of them.
  private val idle = new java.util.concurrent.atomic.AtomicLong(0L)

  /** Workers looking for work on others; while one is, offering work wakes no one else. */
  private[gearsexample] val searching = new java.util.concurrent.atomic.AtomicInteger(0)

  @volatile private[gearsexample] var stopped = false

  /** For measurement: wake-ups sent to idle workers, offering work and for tasks from outside. */
  private[gearsexample] val wakeUps = new java.util.concurrent.atomic.AtomicLong
  private[gearsexample] val injections = new java.util.concurrent.atomic.AtomicLong

  /** The worker running the caller, if it is one of this pool's. */
  private[gearsexample] def currentWorker: ReactorWorker | Null =
    Thread.currentThread() match {
      case thread: ReactorWorkerThread => if thread.pool eq this then thread.worker else null
      case thread =>
        val first = workers(0)
        if first != null && (first.owner eq thread) then first else null
    }

  /** Queues `body` on the worker running the caller, or on the shared queue from outside the pool. */
  def execute(body: Runnable): Unit = {
    val worker = currentWorker
    if worker != null then worker.push(body) else inject(body)
  }

  /** Queues `task` on the shared queue and wakes an idle worker other than `from`, the worker queueing it if any. */
  private[gearsexample] def inject(task: Runnable, from: Int = -1): Unit = {
    injections.incrementAndGet()
    injector.add(task)
    wakeOne(from)
  }

  private[gearsexample] def injected(): Runnable | Null = injector.poll()

  /** Worker `from` has more queued than it is about to run: wakes another idle worker to take some, unless one is
    * searching. `from` itself may still be marked idle, since a worker runs completions, which queue tasks, in the turn
    * it blocked in; but it is running, so it is not the one to wake.
    */
  private[gearsexample] def offerWork(from: Int): Unit =
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

  private[gearsexample] def markIdle(index: Int): Unit = {
    var mask = idle.get()
    while !idle.compareAndSet(mask, mask | (1L << index)) do mask = idle.get()
  }

  private[gearsexample] def unmarkIdle(index: Int): Unit = {
    var mask = idle.get()
    while (mask & (1L << index)) != 0L && !idle.compareAndSet(mask, mask & ~(1L << index)) do mask = idle.get()
  }

  /** Whether there is work `worker` could take: from outside the pool, or spare on another worker. */
  private[gearsexample] def hasWorkFor(worker: ReactorWorker): Boolean =
    !injector.isEmpty || workers.exists(other => (other ne worker) && other.hasSpare)

  /** Runs `body` after `delay` on the worker running the caller, or on the first worker from outside the pool. */
  def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val worker = currentWorker
    (if worker != null then worker else workers(0)).schedule(delay, body)
  }

  /** Stops every worker after its current round. */
  private[gearsexample] def stop(): Unit = {
    stopped = true
    workers.foreach(worker => if worker != null then worker.reactor.wakeup())
  }
}

/** gears' suspension support on Scala Native, with a pool of reactor workers. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorPool
}

/** Gears futures over a thread-owned reactor. */
object ReactorFutures {

  /** Submits `op`, which must have been built by `b.ops`, to the reactor of the worker running the caller, and
    * suspends the caller until the reactor finishes with it: it returns when the op completes, throws its failure, or
    * throws `CancellationException` when it is cancelled. It must be called from a program started by `run` with `b`.
    * `b` only says what the op is; which reactor runs it depends on where the caller runs, so a repeatable op may go to
    * a different reactor each time. To run an op alongside other work, call this inside a `Future`.
    *
    * Cancelling the caller asks the reactor to cancel the op, but this keeps waiting until the reactor confirms, so
    * once it returns or throws, the op's buffers and slots belong to the caller again.
    */
  def perform(b: Backend, op: b.Op)(using ac: Async): Unit = {
    val worker = workerFor(b)
    val r = worker.reactor
    if ac.group.isCancelled then throw new CancellationException()
    // An op performed again reuses its waiter, kept as its attachment, so that performing it allocates nothing here,
    // even when it last ran on another worker. A waiter still in use (the op performed twice at once, which the
    // reactor refuses) is left alone.
    val waiter = b.attachment(op) match {
      case reused: OpWaiter if !reused.inUse =>
        reused.reset(worker)
        reused
      case _ =>
        val fresh = new OpWaiter(worker, op)
        b.attach(op, fresh)
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
        if worker.turnFastPathsLeft > 0 then {
          val now = r.trySubmit(op, waiter.completion)
          if now then worker.turnFastPathsLeft -= 1
          now
        } else if worker.fastPathsLeft > 0 && worker.isIdle then {
          val now = r.trySubmit(op, waiter.completion)
          if now then worker.fastPathsLeft -= 1
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

  /** Submits `op`, which must have been built by `b.ops`, to the reactor of the worker running the caller, with a
    * completion of the caller's own instead of suspending: the completion runs on that worker's thread, which may not
    * be the caller's by then, since a task may resume on another worker. The returned handle cancels the op there.
    */
  def submit(b: Backend, op: b.Op, completion: Completion[b.Op])(using Async): Submission = {
    val worker = workerFor(b)
    worker.reactor.submit(op, completion)
    new Submission(worker, op)
  }

  /** An op submitted with `submit`, which it cancels on the reactor it went to. */
  final class Submission private[ReactorFutures] (worker: ReactorWorker, op: Any) {

    /** Cancels the op, from any thread: at once if called on the worker it went to, and returns true; otherwise on that
      * worker's thread, soon, and returns false, so the op is not free to submit again until its completion's
      * `onCancel` has run.
      */
    def cancel(): Boolean =
      if worker.isCurrent then {
        worker.reactor.cancel(op.asInstanceOf[worker.reactor.Op]) // built by its backend, as `submit` requires
        true
      } else {
        worker.sync(worker.reactor.cancel(op.asInstanceOf[worker.reactor.Op]))
        false
      }
  }

  /** The worker running the caller, which must be one of a pool of `b`'s reactors. The pool comes from the caller's
    * context, but which of its workers runs the caller only from the thread, a thread-local lookup.
    */
  private def workerFor(b: Backend)(using ac: Async): ReactorWorker { val reactor: Reactor.Of[b.Op] } = {
    val worker = ac.scheduler match {
      case pool: ReactorPool if pool.backend eq b => pool.currentWorker
      case _                                      => null
    }
    if worker == null then
      throw new IllegalStateException("Ops must be performed by a program that ReactorFutures.run started")
    worker.asInstanceOf[ReactorWorker { val reactor: Reactor.Of[b.Op] }] // its reactor is one of `b`'s
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

    // Cancellation can come from any thread, so it is carried out on the thread of the reactor the op was submitted to.
    // By then the op may have finished, and a repeatable op may even have been submitted again, there or elsewhere,
    // which must not be cancelled.
    def cancel(): Unit = {
      val submission = generation
      val target = worker
      target.sync(
        if submission == generation && !state.isInstanceOf[Try[?]] then
          target.reactor.cancel(op.asInstanceOf[target.reactor.Op]) // built by its backend, as `perform` requires
      )
    }
  }

  private object OpWaiter {
    private val Empty = new Object
  }

  /** Runs `program` on one worker, the calling thread (see the other `run`). */
  def run(program: Backend => Async.Spawn ?=> Unit)(using backend: Backend): Unit = run(1)(program)

  /** Runs `program` on a pool of `workers` workers (see `ReactorPool`), each with a reactor of `backend`'s: the calling
    * thread becomes the first, and the others get threads of their own. The program starts as a future, builds its ops
    * and uses its handles through the backend it is given, and performs them with `perform`; the workers run until it
    * finishes. `policy` says which tasks a worker out of work may take from another. Each reactor is closed afterwards,
    * on its own thread, and a failure of the program is rethrown.
    */
  def run(workers: Int, policy: TakePolicy = TakePolicy.Affine)(program: Backend => Async.Spawn ?=> Unit)(using
      backend: Backend
  ): Unit = {
    val pool = new ReactorPool(backend, workers, policy)
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
    try {
      given ReactorPool = pool
      given ReactorSupport.type = ReactorSupport
      Async.blocking {
        val main = Future(program(backend)) // queued on the first worker: it starts with that worker's first round
        main.onComplete(Listener((_, _) => pool.stop()))
        pool.workers(0).loop()
        main.await // already complete, so this only rethrows a failure
      }
    } finally {
      pool.stop()
      threads.foreach(_.join())
      if sys.env.get("GEARS_POOL_STATS").contains("1") then printStats(pool)
    }
  }

  /** Prints the pool's counters, once its workers have stopped. */
  private def printStats(pool: ReactorPool): Unit = {
    val workers = pool.workers.filter(_ != null)
    def kqueue(f: asyncio.kqueue.KqueueReactor => Long): String =
      workers.map(_.reactor match { case k: asyncio.kqueue.KqueueReactor => f(k); case _ => 0L }).mkString(" ")
    System.err.println(
      s"pool stats (${workers.length} workers, per worker): taken ${workers.map(_.taken).mkString(" ")}; " +
        s"blocked ${workers.map(_.blocked).mkString(" ")}; taken over ${kqueue(_.takenOver)}; " +
        s"filters dropped ${kqueue(_.filtersDropped)}; wake-ups ${pool.wakeUps.get()}; " +
        s"injected ${pool.injections.get()}"
    )
  }
}
