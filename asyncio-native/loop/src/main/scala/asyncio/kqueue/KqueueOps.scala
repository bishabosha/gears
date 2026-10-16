package asyncio.kqueue

import java.nio.ByteBuffer

import asyncio.Address
import asyncio.Completion
import asyncio.HandleSlot
import asyncio.Interest
import asyncio.Ops
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot

/** Everything `KqueueReactor` can run: its `Op` member type. Kqueue-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait KqueueOp {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[kqueue] var submitted = false

  /** While the op is pending in the reactor: its completion, so the op is its own record and submitting it allocates
    * nothing. Taken off just before the completion runs, which may then submit the op again. Loop thread only.
    */
  private[kqueue] var completion: Completion[KqueueOp] | Null = null

  /** For a command that failed as soon as it was submitted: its failure, until its completion runs. */
  private[kqueue] var atOnceFailure: Throwable | Null = null

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[kqueue] var attachment: AnyRef | Null = null
}

/** Builds the ops `KqueueReactor` understands. */
object KqueueOps extends Ops[KqueueOp] {
  def read(fd: Int, buf: ByteBuffer): ReadIntoBuffer = ReadIntoBuffer(fd, buf)
  def write(fd: Int, buf: ByteBuffer): WriteFromBuffer = WriteFromBuffer(fd, buf)
  def accept(fd: Int, into: HandleSlot): Accept = Accept(fd, into)
  def connect(fd: Int): KqueueOp = Connect(fd)
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): ReceiveFrom = new ReceiveFrom(fd, buf, from)
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): SendTo = new SendTo(fd, buf, to)
  def timer(milliseconds: Int): KqueueTimer = new KqueueTimer(milliseconds)
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): WhenReady = new WhenReady(fd, interest, perform)
  def blocking(body: () => Unit): KqueueBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
}

/** What to do with a descriptor once kqueue reports it ready. `perform` runs a non-blocking operation on the reactor
  * thread; its return value is the result, and an exception it throws is the op's failure. Commands are `Repeatable`:
  * waiting for readiness again and performing again is always meaningful.
  */
trait Command extends KqueueOp with Repeatable {
  def fd: Int
  def interest: Interest
  def perform(): Boolean

  /** Whether the reactor may perform this command as soon as it is submitted, before waiting for readiness. True for
    * operations that simply report would-block when not ready, so trying first only saves the wait when they are.
    */
  def tryFirst: Boolean = true

  /** After a perform that finished: how many bytes it took, or -1 when that is not a count of bytes. Against the bytes
    * a read event reported available, it tells whether the descriptor is still readable afterwards.
    */
  def transferred: Int = -1

  /** After a perform that finished, with no event to compare against: whether the descriptor may still be ready. */
  def mayStillBeReady: Boolean = true

  /** Whether `perform` returns false only when the descriptor would block, as an operation run until it would block
    * does. Such a read can wait on a persistent edge-triggered filter, which reports only new data. Any other command,
    * one that may return false to mean "not finished yet" with data still waiting, waits on a one-shot, level-triggered
    * filter instead, which fires again while the descriptor stays ready.
    */
  def edgeTriggered: Boolean = false
}

/** A command built from a function, for `Ops.whenReady`. */
final class WhenReady(val fd: Int, val interest: Interest, body: () => Boolean) extends Command {
  def perform(): Boolean = body()
  override def tryFirst: Boolean = false // `Ops.whenReady` runs its body only once the descriptor is ready
}

/** A one-shot kqueue timer. `id` is the `EVFILT_TIMER` identifier assigned on submission, or -1 before it. */
final class KqueueTimer(val milliseconds: Int) extends KqueueOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  // The reactor's bookkeeping for a pending submission, on the loop thread: when it is due (`System.nanoTime`), its
  // place in submission order, and its index in the reactor's timer heap (-1 when not pending).
  private[kqueue] var deadline = 0L
  private[kqueue] var order = 0L
  private[kqueue] var heapIndex = -1
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `KqueueBlocking`, the only kind, completes itself from the blocker pool.
  */
sealed abstract class KqueuePromise extends KqueueOp {
  // `Unsettled` until the first completion or failure wins the compare-and-set; later ones are refused.
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](KqueuePromise.Unsettled)
  @volatile private[kqueue] var reactor: KqueueReactor | Null = null

  private[kqueue] def failure: Throwable | Null = outcome.get() match {
    case KqueuePromise.Failed(failure) => failure
    case _                             => null
  }

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = settle(KqueuePromise.Failed(failure))

  /** Tells the reactor this promise has finished, without a value. */
  private[kqueue] def signal(): Unit = settle(())

  /** Records the outcome, if this is the first, and tells the reactor this promise has finished. */
  protected final def settle(value: Any): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    if !outcome.compareAndSet(KqueuePromise.Unsettled, value) then
      throw new IllegalStateException("A promise can only be completed once")
    owner.resolved(this)
  }

}

object KqueuePromise {
  private[kqueue] object Unsettled
  private[kqueue] final case class Failed(failure: Throwable)
}

/** A promise the reactor completes by running `block` on the blocker pool. An exception thrown by `block` is the op's
  * failure.
  *
  * Cancelling it skips `block` if it has not started, and interrupts the worker thread if it is running. Interruption
  * is cooperative: blocking JDK calls such as `Thread.sleep` throw `InterruptedException`, and long computations should
  * check `Thread.interrupted()`. Native calls like `getaddrinfo` cannot be interrupted and run to the end, with their
  * result discarded. Either way the completion's `onCancel` waits until `block` has returned, since until then it may
  * still write to the op's buffers and slots.
  *
  * Writes made before the cancel are allowed, as with a partly filled buffer, but none may follow it: a subclass must
  * write its final result into its slot through `publish`, which skips the write once cancelled.
  */
abstract class KqueueBlocking extends KqueuePromise {
  def block(): Unit

  // Guarded by `this`: the worker running `block`, whether the task was cancelled, and whether `block` has returned.
  private var runner: Thread | Null = null
  private var cancelled = false
  private var ended = false

  /** The cancelled submission whose `onCancel` waits for `block` to return. Loop thread only. */
  private[kqueue] var cancelling: Completion[KqueueOp] | Null = null

  /** Called by the worker before `block`; false if the task was cancelled before it started. */
  private[kqueue] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Called by the worker after `block`. Once this returns no cancel can interrupt the worker, so any interrupt that
    * raced in is cleared and cannot leak into the worker's next task.
    */
  private[kqueue] def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted()
  }

  /** Marks the task cancelled and interrupts its worker if it is running. Returns true if it was running, so `block`
    * may still touch the op's buffers and slots until it returns: the worker reports that through the reactor, like an
    * outcome, and `awaitEnd` waits for it.
    */
  private[kqueue] def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  /** Writes `value` into `into` unless the task has been cancelled. It holds the lock `interrupt` takes, so the write
    * either happens entirely before the cancel or not at all.
    */
  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then into.set(value)
  }

  /** Waits until a running `block` has returned. An interrupt of the waiting thread is kept for later. */
  private[kqueue] def awaitEnd(): Unit = {
    var interrupted = false
    synchronized {
      while !ended do
        try wait()
        catch case _: InterruptedException => interrupted = true
    }
    if interrupted then Thread.currentThread().interrupt()
  }
}

/** A blocking task built from a function, for `Ops.blocking`. */
final class BlockingTask(body: () => Unit) extends KqueueBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `Ops.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends KqueueBlocking {
  def block(): Unit = {
    publish(into, body())
  }
}

/** The blocker pool: a few daemon threads that run blocking tasks off the reactor thread. */
object Blockers {
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Runnable]()
  private lazy val threads = (1 to 4).map { i =>
    val thread = new Thread(
      () =>
        while true do {
          Thread.interrupted() // Never start a task with a stale interrupt.
          try queue.take().run()
          catch case _: InterruptedException => () // A late interrupt must not end the worker.
        },
      s"reactor-blocker-$i"
    )
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Runs `blocking` on the pool and completes it with the outcome, unless it is cancelled first. */
  def run(blocking: KqueueBlocking): Unit = {
    threads
    queue.put { () =>
      if blocking.begin() then {
        var failure: Throwable | Null = null
        try blocking.block()
        catch case t: Throwable => failure = t
        finally blocking.end()
        // A cancelled task is detached, so neither outcome reaches its completion, but the reactor learns from it that
        // `block` has returned, which is when its `onCancel` runs.
        if failure != null then blocking.fail(failure.nn) else blocking.signal()
      }
    }
  }
}
