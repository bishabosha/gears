package asyncio.kqueue

import java.nio.ByteBuffer

import asyncio.BlockingOps
import asyncio.BufferPool
import asyncio.Received
import asyncio.Completion
import asyncio.FileOps
import asyncio.Handle
import asyncio.Interest
import asyncio.IpAddress
import asyncio.ReadinessOps
import asyncio.Repeatable
import asyncio.ResolveOps
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.UnixAddress
import asyncio.UnixDatagramOps
import asyncio.BlockingRun
import asyncio.unsafe.NativePool

/** Everything `KqueueReactor` can run: its `Op` member type. Kqueue-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait KqueueOp extends asyncio.unsafe.TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

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
object KqueueOps
    extends UnixDatagramOps[KqueueOp]
    with ReadinessOps[KqueueOp]
    with BlockingOps[KqueueOp]
    with FileOps[KqueueOp]
    with ResolveOps[KqueueOp] {
  def read(handle: Handle, buf: ByteBuffer): ReadIntoBuffer = ReadIntoBuffer(fd(handle), buf)
  def write(handle: Handle, buf: ByteBuffer): WriteFromBuffer = WriteFromBuffer(fd(handle), buf)
  def accept(handle: Handle, into: Handle.Slot): Accept = Accept(fd(handle), into)
  def connect(handle: Handle): KqueueOp = Connect(fd(handle))
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]): ReceiveFrom =
    new ReceiveFrom(fd(handle), buf, from, unix = false)
  def send(handle: Handle, buf: ByteBuffer, to: IpAddress): SendTo = new SendTo(fd(handle), buf, to)
  def send(handle: Handle, buf: ByteBuffer): SendTo = new SendTo(fd(handle), buf, null)
  @annotation.targetName("receiveUnix")
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[UnixAddress]): ReceiveFrom =
    new ReceiveFrom(fd(handle), buf, from, unix = true)
  def send(handle: Handle, buf: ByteBuffer, to: UnixAddress): SendTo = new SendTo(fd(handle), buf, to)
  def timer(milliseconds: Int): KqueueTimer = new KqueueTimer(milliseconds)
  def whenReady(handle: Handle, interest: Interest)(perform: () => Boolean): WhenReady =
    new WhenReady(fd(handle), interest, perform)
  def blocking(body: () => Unit): KqueueBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Handle, pool: BufferPool, into: Slot[Received]): PooledRead =
    PooledRead(fd(handle), KqueueOps.native(pool), into)
  def listening(handle: Handle): KqueueOp = new Listening(fd(handle))
  def close(handle: Handle): KqueueOp = new Close(fd(handle))
  def open(path: String, write: Boolean, into: Handle.Slot): KqueueOp = new OpenFile(path, write, into)
  def read(handle: Handle, buf: ByteBuffer, offset: Long): PositionalRead = new PositionalRead(fd(handle), buf, offset)
  def write(handle: Handle, buf: ByteBuffer, offset: Long): PositionalWrite = new PositionalWrite(fd(handle), buf, offset)
  def sync(handle: Handle): FileSync = new FileSync(fd(handle))

  private def fd(handle: Handle): Int = KqueueHandles.fd(handle)

  private def native(pool: BufferPool): NativePool = pool match {
    case native: NativePool => native
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op the reactor performs as soon as it is submitted, with no descriptor to wait for: its completion runs on the
  * next turn, with the failure `perform` threw, if any. One-shot.
  */
sealed abstract class KqueueAtOnce extends KqueueOp {
  def perform(): Unit
}

/** Binds and listens a socket from `KqueueHandles.listener`, or finds a datagram socket bound, as it was when made (see
  * `SocketOps.listening`).
  */
final class Listening(val fd: Int) extends KqueueAtOnce {
  def perform(): Unit = KqueueHandles.listening(fd)
}

/** Closes a descriptor (see `Ops.close`): at once, as `KqueueHandles.close` does. */
final class Close(val fd: Int) extends KqueueAtOnce {
  def perform(): Unit = KqueueHandles.closeDescriptor(fd)
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

  /** Whether waiting for this write command should also add the descriptor's persistent read filter, in the same
    * `kevent` call: true for a connect, since a connected socket is then read, and with the filter added before
    * anything can arrive, its first read can wait for an edge instead of trying.
    */
  def registersReads: Boolean = false
}

/** A command built from a function, for `ReadinessOps.whenReady`. */
final class WhenReady(val fd: Int, val interest: Interest, body: () => Boolean) extends Command {
  def perform(): Boolean = body()
  override def tryFirst: Boolean = false // `ReadinessOps.whenReady` runs its body only once the descriptor is ready
}

/** A one-shot timer, kept in the reactor's heap while pending (`TimerHeap.Entry`). */
final class KqueueTimer(val milliseconds: Int) extends KqueueOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  /** Whether the pending submission's time limit is shorter than the timer, so it fails when due. Loop thread only. */
  private[kqueue] var expires = false
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

  /** Forgets the previous submission's outcome, for a `Repeatable` promise submitted again. Loop thread only, before
    * the submission can complete.
    */
  private[kqueue] def reset(): Unit = outcome.set(KqueuePromise.Unsettled)

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

/** A promise the reactor completes by running `block` on the blocker pool, with `BlockingRun`'s cancellation: skipped
  * if not started, interrupted if running, and `onCancel` waits for `block` to return. One marked `Repeatable`, such
  * as a file op, is restarted on each submission.
  */
abstract class KqueueBlocking extends KqueuePromise with BlockingRun {

  /** The cancelled submission whose `onCancel` waits for `block` to return, or whose `onFailure` does, with
    * `cancelledBy`, for one whose time limit ran out. Loop thread only.
    */
  private[kqueue] var cancelling: Completion[KqueueOp] | Null = null
  private[kqueue] var cancelledBy: Throwable | Null = null

  protected def settled(failure: Throwable | Null): Unit =
    if failure != null then fail(failure) else signal()
}

/** A blocking task built from a function, for `BlockingOps.blocking`. */
final class BlockingTask(body: () => Unit) extends KqueueBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `BlockingOps.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends KqueueBlocking {
  def block(): Unit = {
    publish(into, body())
  }
}
