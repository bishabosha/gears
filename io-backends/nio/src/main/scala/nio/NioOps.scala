package gears.async.io
package nio

import java.io.IOException
import java.net.Inet4Address
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

import gears.async.io.AddressFamily
import gears.async.io.internal.Blockers
import gears.async.io.BlockingOps
import gears.async.io.internal.BlockingRun
import gears.async.io.BufferPool
import gears.async.io.Received
import gears.async.io.Completion
import gears.async.io.FileOps
import gears.async.io.Handle
import gears.async.io.Interest
import gears.async.io.IpAddress
import gears.async.io.Repeatable
import gears.async.io.ResolveOps
import gears.async.io.ResolvedAddress
import gears.async.io.Slot
import gears.async.io.PipeOps
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamOps

/** Everything `NioReactor` can run: its `Op` member type. NIO-specific code may construct the classes below directly;
  * code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait NioOp extends gears.async.io.internal.TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[nio] var submitted = false

  /** While the op is pending in the reactor: its completion, so the op is its own record. Taken off just before the
    * completion runs, which may then submit the op again. Loop thread only.
    */
  private[nio] var completion: Completion[NioOp] | Null = null

  /** For a command that failed as soon as it was submitted: its failure, until its completion runs. */
  private[nio] var atOnceFailure: Throwable | Null = null

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[nio] var attachment: AnyRef | Null = null
}

/** Builds the ops `NioReactor` understands. */
object NioOps
    extends UnixStreamOps[NioOp]
    with PipeOps[NioOp]
    with BlockingOps[NioOp]
    with FileOps[NioOp]
    with ResolveOps[NioOp] {
  def read(handle: Handle, buf: ByteBuffer): Read = new Read(handle, buf)
  def write(handle: Handle, buf: ByteBuffer): Write = new Write(handle, buf)
  def accept(handle: Handle, into: Handle.Slot): Accept = new Accept(handle, into)
  def connect(address: IpAddress, into: Handle.Slot): ConnectTo = new ConnectTo(address, into)
  def connect(address: UnixAddress, into: Handle.Slot): ConnectTo = new ConnectTo(address, into)
  def listen(address: IpAddress, into: Handle.Slot, reuseAddr: Boolean = true): NioOp =
    new Listen(address, reuseAddr, into)
  def listen(address: UnixAddress, into: Handle.Slot): NioOp = new Listen(address, reuseAddr = false, into)
  def datagram(local: IpAddress | Null, remote: IpAddress | Null, into: Handle.Slot): NioOp =
    new OpenDatagram(local, remote, into)
  def pipe(read: Handle.Slot, write: Handle.Slot): NioOp = new OpenPipe(read, write)
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]): Receive = new Receive(handle, buf, from)
  def send(handle: Handle, buf: ByteBuffer, to: IpAddress): Send = new Send(handle, buf, to)
  def send(handle: Handle, buf: ByteBuffer): Send = new Send(handle, buf, null)
  def timer(milliseconds: Int): NioTimer = new NioTimer(milliseconds)
  def blocking(body: () => Unit): NioBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NioBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Handle, pool: BufferPool, into: Slot[Received]): PooledRead =
    new PooledRead(handle, NioOps.direct(pool), into)
  def close(handle: Handle): NioOp = new Close(handle)
  def open(path: String, write: Boolean, into: Handle.Slot): NioOp = new OpenFile(path, write, into)
  def read(handle: Handle, buf: ByteBuffer, offset: Long): PositionalRead = new PositionalRead(handle, buf, offset)
  def write(handle: Handle, buf: ByteBuffer, offset: Long): PositionalWrite = new PositionalWrite(handle, buf, offset)
  def sync(handle: Handle): FileSync = new FileSync(handle)

  private def direct(pool: BufferPool): DirectPool = pool match {
    case direct: DirectPool => direct
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op the reactor performs as soon as it is submitted, with no handle to wait for: its completion runs on the
  * next turn, with the failure `perform` threw, if any. One-shot.
  */
sealed abstract class NioAtOnce extends NioOp {
  def perform(): Unit
}

/** An op that opens a handle and writes it into a slot (see `Ops`): if its submission is cancelled, by `cancel`, a
  * time limit or the reactor closing, before its completion has run, `abandon` closes what it opened and empties its
  * slot, so that nothing is left open that the caller never got.
  */
sealed trait Opens extends NioOp {
  def abandon(): Unit
}

/** An op that opens its handle at once, when submitted (see `NioAtOnce`), as NIO's listeners, datagram sockets and
  * pipes are.
  */
sealed abstract class OpensAtOnce(into: Handle.Slot) extends NioAtOnce with Opens {
  private var opened: Handle.Opt = Handle.Opt.empty
  protected def open(): Handle
  def perform(): Unit = {
    val handle = open()
    opened = Handle.Opt(handle)
    into.set(handle)
  }
  def abandon(): Unit = if opened.nonEmpty then {
    into.clear(): Unit
    NioHandles.close(opened.get)
    opened = Handle.Opt.empty
  }
}

/** A stream socket bound and listening on `address` (see `SocketOps.listen`). */
final class Listen(address: SocketAddress, reuseAddr: Boolean, into: Handle.Slot) extends OpensAtOnce(into) {
  protected def open(): Handle = address match {
    case ip: IpAddress     => NioHandles.listen(ip, reuseAddr)
    case unix: UnixAddress => NioHandles.listen(unix)
  }
}

/** A datagram socket, bound and connected where given (see `SocketOps.datagram`). */
final class OpenDatagram(local: IpAddress | Null, remote: IpAddress | Null, into: Handle.Slot)
    extends OpensAtOnce(into) {
  protected def open(): Handle = NioHandles.datagram(local, remote)
}

/** A pipe, its read end written into `read` and its write end into `write` (see `PipeOps.pipe`). */
final class OpenPipe(read: Handle.Slot, write: Handle.Slot) extends NioAtOnce with Opens {
  private var ends: (Handle, Handle) | Null = null
  def perform(): Unit = {
    val (in, out) = NioHandles.pipe()
    ends = (in, out)
    read.set(in)
    write.set(out)
  }
  def abandon(): Unit = if ends != null then {
    val (in, out) = ends.nn
    read.clear(): Unit
    write.clear(): Unit
    NioHandles.close(in)
    NioHandles.close(out)
    ends = null
  }
}

/** Closes a handle (see `Ops.close`): at once, as `NioHandles.close` does. */
final class Close(val handle: Handle) extends NioAtOnce {
  def perform(): Unit = NioHandles.close(handle)
}

/** What to do with a handle once it is ready. `perform` runs a non-blocking operation on the handle's entry, which is
  * looked up when the command is submitted, or performed on the caller (`NioOnCaller`); it returns whether the op is
  * done, and an exception it throws is the op's failure. A reactor performs a command as soon as it is submitted,
  * before waiting for readiness. Commands are `Repeatable`.
  */
abstract class Command extends NioOp with Repeatable {
  def handle: Handle
  def interest: Interest
  def perform(): Boolean

  /** The interest ops to wait for on `channel`: reading a listener is accepting. */
  def selectOps(channel: SelectableChannel): Int =
    if interest == Interest.Write then SelectionKey.OP_WRITE
    else if channel.isInstanceOf[ServerSocketChannel] then SelectionKey.OP_ACCEPT
    else SelectionKey.OP_READ

  /** The entry of the current submission, looked up by the reactor, or by `NioOnCaller`. */
  private[nio] var entry: Entry | Null = null

  /** Submissions in a row that `NioOnCaller` finished at once, so that a run of them yields now and then. Caller's
    * thread only.
    */
  private[nio] var finishedHere = 0

  /** The key of the selector this command last waited on, kept so that waiting there again needs no lookup. */
  private[nio] var key: SelectionKey | Null = null

  protected final def selectable[C <: SelectableChannel](kind: Class[C], op: String): C = entry match {
    case s: Selectable => s.as(kind, op)
    case other         => throw new IOException(s"$op needs a socket, not $other")
  }
}

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream. */
final class Read(val handle: Handle, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = entry.nn.read(buf) != 0
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `PooledReadOps.read`), as `Read` fills the caller's. */
final class PooledRead(val handle: Handle, pool: DirectPool, into: Slot[Received]) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val slot = pool.take()
    if slot == null then throw new IOException("The pool has no buffer free")
    val buffer = slot.nn.buffer
    val read =
      try entry.nn.read(buffer)
      catch {
        case failure: Throwable =>
          slot.nn.release() // a failed read holds nothing
          throw failure
      }
    if read == 0 then {
      slot.nn.release()
      false
    } else {
      buffer.flip()
      into.set(slot.nn)
      true
    }
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written. */
final class Write(val handle: Handle, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Boolean = entry.nn.write(buf) >= 0 || !buf.hasRemaining()
}

/** Accepts one connection on a listener, writing the new connection's handle into `into`. */
final class Accept(val handle: Handle, into: Handle.Slot) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val client = selectable(classOf[ServerSocketChannel], "Accepting").accept()
    if client != null then {
      try client.configureBlocking(false)
      catch {
        case t: Throwable =>
          client.close()
          throw t
      }
      into.set(NioHandles.add(Selectable(client, null)))
    }
    client != null
  }
}

/** A stream socket connected to `address` (see `SocketOps.connect`): the socket is opened, and its non-blocking
  * connect started, when the op is submitted or performed on the caller (`open`), and the op then waits for the connect
  * to finish. Done once connected, after writing the socket into `into`, which a connection that finished at once, as
  * a Unix socket's usually does, is as soon as it is submitted; if the connect fails, or the op is cancelled or times
  * out first, the socket is closed and `into` left empty.
  */
final class ConnectTo(address: SocketAddress, into: Handle.Slot) extends Command with Opens {
  private var socket: Handle.Opt = Handle.Opt.empty
  private var connected = false
  def handle: Handle = socket.get
  def interest: Interest = Interest.Write
  override def selectOps(channel: SelectableChannel): Int = SelectionKey.OP_CONNECT

  /** Opens the socket and starts connecting it, as the op is submitted or performed. */
  private[nio] def open(): Unit = {
    socket = Handle.Opt(NioHandles.connectStream(address))
    connected = false
  }

  def perform(): Boolean = {
    val done =
      try {
        val channel = selectable(classOf[SocketChannel], "Connecting")
        try channel.isConnected || channel.finishConnect()
        catch case e: java.net.ConnectException => throw NioHandles.connectFailed(e)
      } catch {
        case t: Throwable =>
          abandon()
          throw t
      }
    if done then {
      connected = true
      into.set(socket.get)
    }
    done
  }

  def abandon(): Unit = if socket.nonEmpty then {
    if connected then into.clear(): Unit
    NioHandles.close(socket.get)
    socket = Handle.Opt.empty
    connected = false
  }
}

/** Receives one packet into `buf`, left flipped for reading, and writes its sender into `from`. */
final class Receive(val handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    buf.clear()
    val sender = selectable(classOf[DatagramChannel], "Receiving").receive(buf)
    if sender != null then {
      buf.flip()
      from.set(NioHandles.ipAddress(sender))
    }
    sender != null
  }
}

/** Sends `buf` as one packet, to `to` or to the connected peer when `to` is null, consuming it. */
final class Send(val handle: Handle, buf: ByteBuffer, to: IpAddress | Null) extends Command {
  def interest: Interest = Interest.Write
  private val target = if to == null then null else NioHandles.socketAddress(to)
  def perform(): Boolean = {
    val channel = selectable(classOf[DatagramChannel], "Sending")
    val size = buf.remaining()
    // A connected channel sends nothing for an empty buffer, through `write` or `send` alike, and says it sent it.
    if target == null && size == 0 then
      throw new java.io.IOException("The JDK cannot send an empty datagram on a connected socket")
    val sent = if target != null then channel.send(buf, target) else channel.write(buf)
    // A datagram goes whole or not at all; an empty one reports 0 bytes either way.
    sent > 0 || size == 0
  }
}

/** A one-shot timer, kept in its reactor's heap while pending (`TimerHeap.Entry`). */
final class NioTimer(val milliseconds: Int) extends NioOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  /** Whether the pending submission's time limit is shorter than the timer, so it fails when due. Loop thread only. */
  private[nio] var expires = false
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `NioBlocking`, the only kind, completes itself from the blocker pool.
  */
sealed abstract class NioPromise extends NioOp {
  // `Unsettled` until the first completion or failure wins the compare-and-set; later ones are refused.
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](NioPromise.Unsettled)
  @volatile private[nio] var reactor: NioReactor | Null = null

  private[nio] def failure: Throwable | Null = outcome.get() match {
    case NioPromise.Failed(failure) => failure
    case _                          => null
  }

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = settle(NioPromise.Failed(failure))

  /** Forgets the previous submission's outcome, for a `Repeatable` promise submitted again. Loop thread only, before
    * the submission can complete.
    */
  private[nio] def reset(): Unit = outcome.set(NioPromise.Unsettled)

  /** Tells the reactor this promise has finished, without a value. */
  private[nio] def signal(): Unit = settle(())

  /** Records the outcome, if this is the first, and tells the reactor this promise has finished. */
  protected final def settle(value: Any): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    if !outcome.compareAndSet(NioPromise.Unsettled, value) then
      throw new IllegalStateException("A promise can only be completed once")
    owner.resolved(this)
  }
}

object NioPromise {
  private[nio] object Unsettled
  private[nio] final case class Failed(failure: Throwable)
}

/** A promise the reactor completes by running `block` on the blocker pool (`Blockers`), with `BlockingRun`'s
  * cancellation: skipped if not started, interrupted if running, and `onCancel` waits for `block` to return. An
  * exception thrown by `block` is the op's failure. As with the kqueue backend's blocking ops, a subclass writes its
  * result into its slot through `publish`, which skips the write once cancelled. One marked `Repeatable`, such as a
  * file op, is restarted on each submission.
  */
abstract class NioBlocking extends NioPromise with BlockingRun {

  /** The cancelled submission whose `onCancel` waits for `block` to return, or whose `onFailure` does, with
    * `cancelledBy`, for one whose time limit ran out. Loop thread only.
    */
  private[nio] var cancelling: Completion[NioOp] | Null = null
  private[nio] var cancelledBy: Throwable | Null = null

  /** Runs `block` on the blocker pool and completes the promise with the outcome, unless it is cancelled first. */
  private[nio] def start(): Unit = Blockers.run(this)

  protected def settled(failure: Throwable | Null): Unit =
    if failure != null then fail(failure) else signal()

  /** Readies this op to run on the calling thread instead of the blocker pool (see `NioOnCaller`): as a submission
    * would, once for a one-shot op.
    */
  private[nio] def here(): BlockingHere = {
    this match {
      case _: Repeatable => ()
      case _             =>
        if submitted then throw new IllegalStateException(s"$this is not Repeatable and was already submitted")
    }
    submitted = true
    reset()
    restart()
    new BlockingHere(this)
  }
}

/** A blocking task built from a function, for `BlockingOps.blocking`. */
final class BlockingTask(body: () => Unit) extends NioBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `BlockingOps.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends NioBlocking {
  def block(): Unit = publish(into, body())
}

/** Resolves a host name with `InetAddress`, which blocks, on the blocker pool. */
final class Resolve(val host: String, into: Slot[List[ResolvedAddress]]) extends NioBlocking {
  def block(): Unit = publish(into, Resolve.addresses(host))
}

private[nio] object Resolve {

  /** `host`'s addresses, in numeric form, looked up with `InetAddress`, which blocks. */
  def addresses(host: String): List[ResolvedAddress] = {
    val addresses =
      try InetAddress.getAllByName(host).toList
      catch case e: IOException => throw new IOException(s"Failed to resolve $host: ${e.getMessage}", e)
    addresses.map {
      case v4: Inet4Address => ResolvedAddress(AddressFamily.IPv4, v4.getHostAddress)
      case v6               =>
        val text = v6.getHostAddress
        val scope = text.indexOf('%')
        ResolvedAddress(AddressFamily.IPv6, if scope < 0 then text else text.substring(0, scope))
    }
  }
}
