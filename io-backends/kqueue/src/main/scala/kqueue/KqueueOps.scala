package gears.async.io
package kqueue

import java.nio.ByteBuffer

import gears.async.io.BlockingOps
import gears.async.io.BufferPool
import gears.async.io.Received
import gears.async.io.Completion
import gears.async.io.FileOps
import KqueueBackend.Handle
import gears.async.io.Interest
import gears.async.io.IpAddress
import gears.async.io.ReadinessOps
import gears.async.io.Repeatable
import gears.async.io.ResolveOps
import gears.async.io.ResolvedAddress
import gears.async.io.Slot
import gears.async.io.UnixAddress
import gears.async.io.UnixDatagramOps
import gears.async.io.UnixStreamOps
import gears.async.io.PipeOps
import gears.async.io.FifoOps
import gears.async.io.internal.BlockingRun
import gears.async.io.internal.NativePool
import gears.async.io.internal.NonBlocking

/** Everything `KqueueReactor` can run: its `Op` member type. Kqueue-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait KqueueOp extends gears.async.io.internal.TimerHeap.Entry {
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
    extends UnixStreamOps[KqueueOp, Handle]
    with UnixDatagramOps[KqueueOp, Handle]
    with PipeOps[KqueueOp, Handle]
    with FifoOps[KqueueOp, Handle]
    with ReadinessOps[KqueueOp, Handle]
    with BlockingOps[KqueueOp, Handle]
    with FileOps[KqueueOp, Handle]
    with ResolveOps[KqueueOp, Handle] {
  def read(handle: Handle, buf: ByteBuffer): ReadIntoBuffer = on(handle)(ReadIntoBuffer(fd(handle), buf))
  def write(handle: Handle, buf: ByteBuffer): WriteFromBuffer = on(handle)(WriteFromBuffer(fd(handle), buf))
  def accept(handle: Handle, into: Slot[Handle]): Accept = on(handle)(Accept(fd(handle), into))
  def acceptAll(handle: Handle, into: Slot[Array[Handle]], max: Int): AcceptAll = {
    require(max > 0, s"max must be positive, not $max")
    on(handle)(new AcceptAll(fd(handle), into, max))
  }
  def connect(address: IpAddress, into: Slot[Handle]): ConnectTo = new ConnectTo(address, into)
  def connect(address: UnixAddress, into: Slot[Handle]): ConnectTo = new ConnectTo(address, into)
  def listen(address: IpAddress, into: Slot[Handle], reuseAddr: Boolean = true): KqueueOp =
    new Listen(address, reuseAddr, into)
  def listen(address: UnixAddress, into: Slot[Handle]): KqueueOp = new Listen(address, reuseAddr = false, into)
  def datagram(local: IpAddress | Null, remote: IpAddress | Null, into: Slot[Handle]): KqueueOp = {
    require(
      local == null || remote == null || local.ordinal == remote.ordinal,
      "Local and remote addresses must be of the same family"
    )
    new OpenDatagram(local, remote, into)
  }
  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null, into: Slot[Handle]): KqueueOp =
    new OpenDatagram(local, remote, into)
  def pipe(read: Slot[Handle], write: Slot[Handle]): KqueueOp = new OpenPipe(read, write)
  def openFifo(path: String, write: Boolean, into: Slot[Handle]): KqueueOp = new OpenFifo(path, write, into)
  def readNow(handle: Handle, buf: ByteBuffer): Int = KqueueHandles.readNow(handle, buf)
  def writeNow(handle: Handle, buf: ByteBuffer): Int = KqueueHandles.writeNow(handle, buf)
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, from, unix = false))
  def send(handle: Handle, buf: ByteBuffer, to: IpAddress): SendTo = on(handle)(new SendTo(fd(handle), buf, to))
  def send(handle: Handle, buf: ByteBuffer): SendTo = on(handle)(new SendTo(fd(handle), buf, null))
  @annotation.targetName("receiveUnix")
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[UnixAddress]): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, from, unix = true))
  def send(handle: Handle, buf: ByteBuffer, to: UnixAddress): SendTo = on(handle)(new SendTo(fd(handle), buf, to))
  def timer(milliseconds: Int): KqueueTimer = new KqueueTimer(milliseconds)
  def ready(handle: Handle, interest: Interest): Ready = on(handle)(new Ready(fd(handle), interest))
  def blocking(body: () => Unit): KqueueBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Handle, pool: BufferPool, into: Slot[Received]): PooledRead =
    on(handle)(PooledRead(fd(handle), KqueueOps.native(pool), into))
  def close(handle: Handle): KqueueOp = new Close(handle)
  def shutdownOutput(handle: Handle): KqueueOp = new ShutdownOutput(handle)
  def open(path: String, write: Boolean, into: Slot[Handle]): KqueueOp = new OpenFile(path, write, into)
  def read(handle: Handle, buf: ByteBuffer, offset: Long): PositionalRead = new PositionalRead(handle, buf, offset)
  def write(handle: Handle, buf: ByteBuffer, offset: Long): PositionalWrite = new PositionalWrite(handle, buf, offset)
  def sync(handle: Handle): FileSync = new FileSync(handle)

  private def fd(handle: Handle): Int = KqueueHandles.fd(handle)

  /** `command`, carrying `handle`, which the reactor takes for each use of the descriptor (see `Command.handle`). */
  private def on[C <: Command](handle: Handle)(command: C): C = {
    command.handle = handle
    command
  }

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

/** An op that opens a handle and writes it into a slot (see `Ops`): if its submission is cancelled, by `cancel`, a
  * time limit or the reactor closing, before its completion has run, `abandon` closes what it opened and empties its
  * slot, so that nothing is left open that the caller never got.
  */
sealed trait Opens extends KqueueOp {
  def abandon(): Unit
}

/** An op that opens its handle at once, when submitted (see `KqueueAtOnce`), as kqueue's listeners, datagram sockets,
  * pipes and FIFOs are: `opened` is what it opened, written into `into` by the time the op completes.
  */
sealed abstract class OpensAtOnce(into: Slot[Handle]) extends KqueueAtOnce with Opens {
  private var opened = -1
  protected def open(): Int
  def perform(): Unit = {
    opened = open()
    into.set(KqueueHandles.handle(opened))
  }
  def abandon(): Unit = if opened >= 0 then {
    into.clear(): Unit
    KqueueHandles.closeDescriptor(opened)
    opened = -1
  }
}

/** A stream socket bound and listening on `address` (see `SocketOps.listen`). */
final class Listen(address: SocketAddress, reuseAddr: Boolean, into: Slot[Handle]) extends OpensAtOnce(into) {
  protected def open(): Int = KqueueHandles.listenStream(address, reuseAddr)
}

/** A datagram socket, bound and connected where given (see `SocketOps.datagram`). */
final class OpenDatagram(local: SocketAddress | Null, remote: SocketAddress | Null, into: Slot[Handle])
    extends OpensAtOnce(into) {
  protected def open(): Int = KqueueHandles.datagramSocket(local, remote)
}

/** A FIFO, opened without waiting for its other end (see `FifoOps.openFifo`). */
final class OpenFifo(path: String, write: Boolean, into: Slot[Handle]) extends OpensAtOnce(into) {
  protected def open(): Int = KqueueHandles.fifoDescriptor(path, write)
}

/** A pipe, its read end written into `read` and its write end into `write` (see `PipeOps.pipe`). */
final class OpenPipe(read: Slot[Handle], write: Slot[Handle]) extends KqueueAtOnce with Opens {
  private var ends: (Int, Int) | Null = null
  def perform(): Unit = {
    val (in, out) = KqueueHandles.pipeDescriptors()
    ends = (in, out)
    read.set(KqueueHandles.handle(in))
    write.set(KqueueHandles.handle(out))
  }
  def abandon(): Unit = if ends != null then {
    val (in, out) = ends.nn
    read.clear(): Unit
    write.clear(): Unit
    KqueueHandles.closeDescriptor(in)
    KqueueHandles.closeDescriptor(out)
    ends = null
  }
}

/** A stream socket connected to `address` (see `SocketOps.connect`): the socket is opened, and its non-blocking
  * connect started, when the op is submitted (`open`), and the op then waits as `Connect` does. Done once connected,
  * after writing the socket into `into`; if the connect fails, or the submission is cancelled first, the socket is
  * closed and `into` left empty.
  */
final class ConnectTo(address: SocketAddress, into: Slot[Handle]) extends Command with Opens {
  private var socket = -1
  private var connected = false
  def fd: Int = socket
  def interest: Interest = Interest.Write
  override def tryFirst: Boolean = false // the check passes while the connection is still in progress
  override def registersReads: Boolean = true

  /** Opens the socket and starts connecting it, for the reactor, as the op is submitted. */
  private[kqueue] def open(): Unit = {
    socket = KqueueHandles.connectStream(address)
    connected = false
  }

  def perform(): Boolean = {
    try gears.async.io.internal.PosixSockets.checkConnect(socket)
    catch {
      case t: Throwable =>
        abandon()
        throw t
    }
    connected = true
    into.set(KqueueHandles.handle(socket))
    true
  }

  def abandon(): Unit = if socket >= 0 then {
    if connected then into.clear(): Unit
    KqueueHandles.closeDescriptor(socket)
    socket = -1
    connected = false
  }
}

/** Closes a handle (see `Ops.close`): at once, as `KqueueHandles.close` does. */
final class Close(val handle: Handle) extends KqueueAtOnce {
  def perform(): Unit = KqueueHandles.close(handle)
}

/** Shuts a socket down for writing (see `SocketOps.shutdownOutput`): at once, since the kernel sends what is queued,
  * then the end of the stream, after the call returns.
  */
final class ShutdownOutput(val handle: Handle) extends KqueueAtOnce {
  def perform(): Unit = KqueueHandles.using(handle)(gears.async.io.internal.PosixSockets.shutdownOutput(handle.fd))
}

/** What to do with a descriptor once kqueue reports it ready. `perform` runs a non-blocking operation on the reactor
  * thread; its return value is the result, and an exception it throws is the op's failure. Commands are `Repeatable`:
  * waiting for readiness again and performing again is always meaningful.
  */
trait Command extends KqueueOp with Repeatable {
  def fd: Int
  def interest: Interest
  def perform(): Boolean

  /** The handle the command was built on, through `KqueueOps`, which the reactor takes around each use of the
    * descriptor, the command's system call and its registration, failing the command if it is closed; null for a
    * command built on a descriptor directly, as kqueue's own code does.
    */
  private[kqueue] var handle: KqueueBackend.Handle | Null = null

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

/** Readiness itself, for `ReadinessOps.ready`: done as soon as kqueue reports the descriptor ready, which it does at
  * once while the descriptor stays so, since the command waits on a one-shot, level-triggered filter each time.
  */
final class Ready(val fd: Int, val interest: Interest) extends Command {
  def perform(): Boolean = true
  override def tryFirst: Boolean = false // the wait is the point: only kqueue says the descriptor is ready
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
