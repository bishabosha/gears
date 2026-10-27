package gears.async.io
package nio

import gears.async.io.AddressFamily
import gears.async.io.BlockingOps
import gears.async.io.BufferPool
import gears.async.io.Completion
import gears.async.io.FileOps
import gears.async.io.IpAddress
import gears.async.io.IpDatagramOps
import gears.async.io.IpStreamOps
import gears.async.io.PipeOps
import gears.async.io.Received
import gears.async.io.Repeatable
import gears.async.io.ResolveOps
import gears.async.io.ResolvedAddress
import gears.async.io.Slot
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamOps
import gears.async.io.internal.Blockers
import gears.async.io.internal.BlockingRun
import gears.async.io.internal.Interest

import java.io.IOException
import java.net.Inet4Address
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

import NioCapabilities.Handle

/** Everything `NioReactor` can run: its `Op` member type. NIO-specific code may construct the classes below directly;
  * code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait NioOp extends gears.async.io.internal.TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

  // Whether this op has been submitted before, so a one-shot op is run once though submitted on two threads at once.
  private val submissions = new java.util.concurrent.atomic.AtomicBoolean()

  /** Records a submission, refusing a one-shot op submitted before with `IllegalStateException`. */
  private[nio] final def markSubmitted(): Unit =
    if submissions.getAndSet(true) && oneShot then
      throw new IllegalStateException(s"$this is not Repeatable and was already submitted")

  /** While the op is pending in the reactor: its completion, so the op is its own record. Taken off just before the
    * completion runs, which may then submit the op again. Loop thread only.
    */
  private[nio] var completion: Completion[NioOp] | Null = null

  /** For a command that failed as soon as it was submitted: its failure, until its completion runs. */
  private[nio] var atOnceFailure: Throwable | Null = null

  /** The failure of an op submitted with a slot still holding a value (`Ops.notEmpty`), which fails it at once, before
    * it does anything; null if its slots are empty.
    */
  private[nio] def slotRefusal: Throwable | Null = null

  /** The claim this op takes on its handle while pending (`Handle.reader` or `writer`); null for one that takes none.
    */
  private[nio] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null = null

  /** Whether it may be submitted only once: an op that is not `Repeatable`, or a command that opens or closes. */
  private[nio] def oneShot: Boolean = !this.isInstanceOf[Repeatable]

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[nio] var attachment: AnyRef | Null = null
}

/** Builds the ops `NioReactor` understands. */
object NioOps
    extends IpStreamOps[NioOp, Handle]
    with IpDatagramOps[NioOp, Handle]
    with UnixStreamOps[NioOp, Handle]
    with PipeOps[NioOp, Handle]
    with BlockingOps[NioOp, Handle]
    with FileOps[NioOp, Handle]
    with ResolveOps[NioOp, Handle] {
  type Readable = NioKinds.Readable
  type Writable = NioKinds.Writable
  type Connection = NioKinds.Connection
  type Listener = NioKinds.Listener
  type BoundDatagram = NioKinds.BoundDatagram
  type ConnectedDatagram = NioKinds.ConnectedDatagram
  type UnixBoundDatagram = NioKinds.UnixBoundDatagram
  type UnixConnectedDatagram = NioKinds.UnixConnectedDatagram
  type File = NioKinds.File

  def read(handle: Readable, buf: ByteBuffer): Read = new Read(handle, buf)
  def write(handle: Writable, buf: ByteBuffer): Write = new Write(handle, buf)
  def read(handle: Readable, bufs: Array[ByteBuffer]): ReadMany = {
    gears.async.io.Ops.checkBuffers(bufs)
    new ReadMany(handle, bufs)
  }
  def write(handle: Writable, bufs: Array[ByteBuffer]): WriteMany = {
    gears.async.io.Ops.checkBuffers(bufs)
    new WriteMany(handle, bufs)
  }
  def accept(handle: Listener, into: Slot[Connection]): Accept = new Accept(handle, into)
  def acceptAll(handle: Listener, into: Slot[Array[Connection]], max: Int): AcceptAll = {
    require(max > 0, s"max must be positive, not $max")
    new AcceptAll(handle, into, max)
  }
  def connect(address: IpAddress, options: SocketOptions, into: Slot[Connection]): ConnectTo = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.Connection)
    new ConnectTo(gears.async.io.Ops.ip(address), into, options)
  }
  def connect(address: UnixAddress, into: Slot[Connection]): ConnectTo =
    new ConnectTo(gears.async.io.Ops.unix(address), into)
  def listen(address: IpAddress, options: SocketOptions, into: Slot[Listener]): NioOp = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.Listener)
    new Listen(gears.async.io.Ops.ip(address), into, options)
  }
  def listen(address: UnixAddress, into: Slot[Listener]): NioOp =
    new Listen(gears.async.io.Ops.unix(address), into)
  def bindDatagram(local: IpAddress, options: SocketOptions, into: Slot[BoundDatagram]): NioOp = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.BoundDatagram)
    new OpenDatagram(gears.async.io.Ops.ip(local), null, into, NioKinds.boundDatagram, options)
  }
  def connectDatagram(
      local: IpAddress | Null,
      remote: IpAddress,
      options: SocketOptions,
      into: Slot[ConnectedDatagram]
  ): NioOp = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.ConnectedDatagram)
    val (l, r) = gears.async.io.Ops.ipPair(local, remote)
    new OpenDatagram(l, r, into, NioKinds.connectedDatagram, options)
  }

  // Socket options: each where the JDK has it here, changed at any time but `ReusePort`, set before binding.
  def supports(option: SocketOption[?]): Boolean = option match {
    case SocketOption.KeepAlive    => NioHandles.keepAliveTimings
    case SocketOption.DontFragment => NioHandles.dontFragment
    case SocketOption.ReusePort    => NioHandles.reusePort
    case _                         => true
  }
  def canChange(option: SocketOption[?]): Boolean = supports(option) && option != SocketOption.ReusePort
  def option[V](handle: Handle, option: SocketOption[V]): V =
    NioHandles.optionOf(networkChannel(handle, option), option).asInstanceOf[V]
  def setOption[V](handle: Handle, option: SocketOption[V], value: V): Unit = {
    option.check(value)
    val channel = networkChannel(handle, option)
    if !canChange(option) then throw new IllegalArgumentException(s"$option is given as a socket opens, not changed")
    NioHandles.setOption(channel, option, value)
  }

  /** `handle`'s channel, an IP socket's `option` applies to; else an `IllegalArgumentException`. */
  private def networkChannel(handle: Handle, option: SocketOption[?]): java.nio.channels.NetworkChannel = {
    if !supports(option) then throw new IllegalArgumentException(s"This backend has no socket option $option here")
    NioHandles.entry(handle) match {
      case s: Selectable =>
        s.channel match {
          case c: java.nio.channels.NetworkChannel if c.getLocalAddress.isInstanceOf[java.net.InetSocketAddress] =>
            val kind = c match {
              case _: ServerSocketChannel => SocketOption.Kind.Listener
              case _: SocketChannel       => SocketOption.Kind.Connection
              case d: DatagramChannel     =>
                if d.isConnected then SocketOption.Kind.ConnectedDatagram else SocketOption.Kind.BoundDatagram
              case _ => throw new IllegalArgumentException(s"$option applies to IP sockets, not $handle")
            }
            if !SocketOption.onSocket(option, kind) then throw gears.async.io.Ops.notFor(option, kind)
            c
          case _ => throw new IllegalArgumentException(s"$option applies to IP sockets, not $handle")
        }
      case _ => throw new IllegalArgumentException(s"$option applies to IP sockets, not $handle")
    }
  }
  def pipe(read: Slot[Readable], write: Slot[Writable]): NioOp = new OpenPipe(read, write)
  def receive(handle: BoundDatagram, buf: ByteBuffer, from: Slot[IpAddress]): Receive = new Receive(handle, buf, from)
  def receive(handle: ConnectedDatagram, buf: ByteBuffer): Receive = new Receive(handle, buf, null)
  def send(handle: BoundDatagram, buf: ByteBuffer, to: IpAddress): Send =
    new Send(handle, buf, gears.async.io.Ops.ip(to))
  def send(handle: ConnectedDatagram, buf: ByteBuffer): Send = new Send(handle, buf, null)
  def send(handle: BoundDatagram, bufs: Array[ByteBuffer], to: IpAddress): Send = {
    gears.async.io.Ops.checkBuffers(bufs)
    new Send(handle, bufs, gears.async.io.Ops.ip(to))
  }
  def send(handle: ConnectedDatagram, bufs: Array[ByteBuffer]): Send = {
    gears.async.io.Ops.checkBuffers(bufs)
    new Send(handle, bufs, null)
  }
  def timer(duration: scala.concurrent.duration.FiniteDuration): NioTimer = {
    require(duration.length >= 0, s"A timer's time is not negative: $duration")
    new NioTimer(duration.toNanos)
  }
  def deadlineTimer(nanoTime: Long): NioDeadlineTimer = new NioDeadlineTimer(nanoTime)
  def blocking(body: () => Unit): NioBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NioBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Readable, pool: BufferPool, into: Slot[Received]): PooledRead =
    new PooledRead(handle, NioOps.direct(pool), into)
  def close(handle: Handle): NioOp = new Close(handle)
  def shutdownOutput(handle: Connection): NioOp = new ShutdownOutput(handle)
  def localAddress(handle: Listener | Connection): IpAddress | UnixAddress = NioHandles.addressOf(handle, peer = false)
  def peerAddress(handle: Connection): IpAddress | UnixAddress = NioHandles.addressOf(handle, peer = true)
  @annotation.targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagram | ConnectedDatagram): IpAddress = ip(handle, peer = false)
  @annotation.targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagram): IpAddress = ip(handle, peer = true)

  private def ip(handle: Handle, peer: Boolean): IpAddress = NioHandles.addressOf(handle, peer) match {
    case ip: IpAddress => ip
    case other         => throw new java.io.IOException(s"$handle is not an IP socket: $other")
  }
  def open(path: String, write: Boolean, into: Slot[File]): NioOp = new OpenFile(path, write, into)
  def read(handle: File, buf: ByteBuffer, offset: Long): PositionalRead = new PositionalRead(handle, buf, offset)
  def write(handle: File, buf: ByteBuffer, offset: Long): PositionalWrite = new PositionalWrite(handle, buf, offset)
  def sync(handle: File): FileSync = new FileSync(handle)

  /** `bufs`' remaining bytes, `size` of them, in one buffer, for a send that takes one; `bufs` left as they were. */
  private[nio] def joined(bufs: Array[ByteBuffer], size: Long): ByteBuffer = {
    val packet = ByteBuffer.allocateDirect(size.toInt)
    bufs.foreach(b => packet.put(b.duplicate()))
    packet.flip()
  }

  private def direct(pool: BufferPool): DirectPool = pool match {
    case direct: DirectPool => direct
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op the reactor performs as soon as it is submitted, with no handle to wait for: its completion runs on the next
  * turn, with the failure `perform` threw, if any. One-shot.
  */
sealed abstract class NioAtOnce extends NioOp {
  def perform(): Unit
}

/** An op that opens a handle and writes it into a slot (see `Ops`): if its submission is cancelled, by `cancel`, a time
  * limit or the reactor closing, before its completion has run, `abandon` closes what it opened and empties its slot,
  * so that nothing is left open that the caller never got.
  */
sealed trait Opens extends NioOp {
  def abandon(): Unit
}

/** An op that opens its handle at once, when submitted (see `NioAtOnce`), as NIO's listeners, datagram sockets and
  * pipes are.
  */
sealed abstract class OpensAtOnce[K <: Handle](into: Slot[K]) extends NioAtOnce with Opens {
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  private var opened: K | Null = null
  protected def open(): K
  def perform(): Unit = {
    val handle = open()
    opened = (handle)
    into.set(handle)
  }
  def abandon(): Unit = if opened != null then {
    into.clear(): Unit
    NioHandles.close(opened.nn)
    opened = null
  }
}

/** A stream socket bound and listening on `address` (see `IpStreamOps.listen`). */
final class Listen(
    address: SocketAddress,
    into: Slot[NioKinds.Listener],
    options: SocketOptions = SocketOptions.default
) extends OpensAtOnce(into) {
  protected def open(): NioKinds.Listener = address match {
    case ip: IpAddress     => NioHandles.listen(ip, options)
    case unix: UnixAddress => NioHandles.listen(unix)
  }
}

/** A datagram socket, bound to `local` and connected to `remote` where given (see `IpDatagramOps.bindDatagram` and
  * `connectDatagram`), written into `into` as the kind `kind` makes.
  */
final class OpenDatagram[K <: Handle](
    local: IpAddress | Null,
    remote: IpAddress | Null,
    into: Slot[K],
    kind: Handle => K,
    options: SocketOptions = SocketOptions.default
) extends OpensAtOnce(into) {
  protected def open(): K = kind(NioHandles.datagram(local, remote, options))
}

/** A pipe, its read end written into `read` and its write end into `write` (see `PipeOps.pipe`). */
final class OpenPipe(read: Slot[NioKinds.Readable], write: Slot[NioKinds.Writable]) extends NioAtOnce with Opens {
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(read, write)
  private var ends: (NioKinds.Readable, NioKinds.Writable) | Null = null
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

/** Shuts a socket down for writing (see `StreamSocketOps.shutdownOutput`): at once, as `SocketChannel.shutdownOutput`
  * does.
  */
final class ShutdownOutput(val handle: Handle) extends NioAtOnce {
  def perform(): Unit = NioHandles.shutdownOutput(handle)
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

  override private[nio] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null =
    if interest == Interest.Read then handle.reader else handle.writer

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

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream; fails if
  * `buf` has no room, where a channel's read returns 0, which would otherwise mean wait, for ever.
  */
final class Read(val handle: Handle, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    if !buf.hasRemaining() then throw gears.async.io.Ops.bufferFull(buf)
    entry.nn.read(buf) != 0
  }
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read`), as `Read` fills the caller's. */
final class PooledRead(val handle: Handle, pool: DirectPool, into: Slot[Received]) extends Command {
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
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

/** Drains `buf` between position and limit. Done once at least one byte has been written, or at once, with no system
  * call, if `buf` has none.
  */
final class Write(val handle: Handle, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Boolean = !buf.hasRemaining() || entry.nn.write(buf) >= 0
}

/** Fills `bufs` in order, as `Read` fills one, through the channel's scattering read. */
final class ReadMany(val handle: Handle, bufs: Array[ByteBuffer]) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    if !gears.async.io.Ops.anyRemaining(bufs) then throw gears.async.io.Ops.buffersFull(bufs)
    entry.nn.read(bufs) != 0
  }
}

/** Drains `bufs` in order, as `Write` drains one, through the channel's gathering write. */
final class WriteMany(val handle: Handle, bufs: Array[ByteBuffer]) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Boolean = !gears.async.io.Ops.anyRemaining(bufs) || entry.nn.write(bufs) >= 0
}

/** Accepts one connection on a listener, writing the new connection's handle into `into`. */
final class Accept(val handle: Handle, into: Slot[NioKinds.Connection]) extends Command {
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
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
      NioHandles.accepted(handle, client)
      into.set(NioKinds.connection(NioHandles.add(Selectable(client, null))))
    }
    client != null
  }
}

/** Accepts the connections pending on a listener, at most `max`, writing their handles into `into` (see
  * `StreamSocketOps.acceptAll`). Done once at least one has been accepted.
  */
final class AcceptAll(val handle: Handle, into: Slot[Array[NioKinds.Connection]], max: Int) extends Command {
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val listener = selectable(classOf[ServerSocketChannel], "Accepting")
    val clients = scala.collection.mutable.ArrayBuffer.empty[NioKinds.Connection]
    var more = true
    try
      while more && clients.length < max do {
        val client = listener.accept()
        if client == null then more = false
        else {
          try client.configureBlocking(false)
          catch {
            case t: Throwable =>
              client.close()
              throw t
          }
          NioHandles.accepted(handle, client)
          clients += NioKinds.connection(NioHandles.add(Selectable(client, null)))
        }
      }
    catch {
      case failure: Throwable =>
        if clients.isEmpty then
          throw failure // with some accepted, they are delivered, and the next accept fails instead
    }
    if clients.nonEmpty then into.set(clients.toArray)
    clients.nonEmpty
  }
}

/** A stream socket connected to `address` (see `IpStreamOps.connect`): the socket is opened, and its non-blocking
  * connect started, when the op is submitted or performed on the caller (`open`), and the op then waits for the connect
  * to finish. Done once connected, after writing the socket into `into`, which a connection that finished at once, as a
  * Unix socket's usually does, is as soon as it is submitted; if the connect fails, or the op is cancelled or times out
  * first, the socket is closed and `into` left empty.
  */
final class ConnectTo(
    address: SocketAddress,
    into: Slot[NioKinds.Connection],
    options: SocketOptions = SocketOptions.default
) extends Command
    with Opens {
  override private[nio] def oneShot: Boolean = true // it opens its socket
  override private[nio] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null =
    null // on a socket of its own
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  private var socket: Handle | Null = null
  private var connected = false
  def handle: Handle = socket.nn
  def interest: Interest = Interest.Write
  override def selectOps(channel: SelectableChannel): Int = SelectionKey.OP_CONNECT

  /** Opens the socket and starts connecting it, as the op is submitted or performed. */
  private[nio] def open(): Unit = {
    socket = (NioHandles.connectStream(address, options))
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
      into.set(NioKinds.connection(socket.nn))
    }
    done
  }

  def abandon(): Unit = if socket != null then {
    if connected then into.clear(): Unit
    NioHandles.close(socket.nn)
    socket = null
    connected = false
  }
}

/** Receives one packet into `buf`, left flipped for reading, and writes its sender into `from`; on a connected socket,
  * whose sender is its peer, `from` is null.
  */
final class Receive(val handle: Handle, buf: ByteBuffer, from: Slot[IpAddress] | Null) extends Command {
  override private[nio] def slotRefusal: Throwable | Null =
    if from == null then null else gears.async.io.Ops.notEmpty(from.nn)
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    if buf.capacity() == 0 then throw gears.async.io.Ops.noCapacity(buf) // the JDK would drop the whole packet
    buf.clear()
    val sender = selectable(classOf[DatagramChannel], "Receiving").receive(buf)
    if sender != null then {
      buf.flip()
      if from != null then from.nn.set(NioHandles.ipAddress(sender))
    }
    sender != null
  }
}

/** Sends `buf` as one packet, consuming it: from a bound socket to `to`, an empty one too; from a connected one, `to`
  * null, to its peer, never an empty one.
  */
final class Send(val handle: Handle, bufs: Array[ByteBuffer], to: IpAddress | Null) extends Command {
  def this(handle: Handle, buf: ByteBuffer, to: IpAddress | Null) = this(handle, Array(buf), to)
  def interest: Interest = Interest.Write
  private val target = if to == null then null else NioHandles.socketAddress(to)
  def perform(): Boolean = {
    val channel = selectable(classOf[DatagramChannel], "Sending")
    val size = gears.async.io.Ops.remaining(bufs)
    if target == null then {
      // A connected channel sends nothing for an empty buffer, through `write` or `send` alike, and says it sent it;
      // so no backend sends an empty packet on a connected socket (see `IpDatagramOps.send`).
      if size == 0 then throw gears.async.io.Ops.emptyPacket(bufs(0))
      // A connected channel's gathering write sends one datagram of all the buffers.
      (if bufs.length == 1 then channel.write(bufs(0)).toLong else channel.write(bufs)) > 0
    } else {
      // A send to an address takes one buffer: several are joined, a copy.
      val packet = if bufs.length == 1 then bufs(0) else NioOps.joined(bufs, size)
      val sent = channel.send(packet, target) > 0 || size == 0 // an empty one goes, and reports 0 bytes
      if sent && bufs.length > 1 then bufs.foreach(b => b.position(b.limit()))
      sent
    }
  }
}

/** A one-shot timer, kept in its reactor's heap while pending (`TimerHeap.Entry`). */
sealed class NioTimer private[nio] (protected val nanos: Long)
    extends NioOp
    with Repeatable
    with gears.async.io.internal.TimerState

/** A timer at a deadline that may move while it is pending (see `Ops.deadlineTimer`). */
final class NioDeadlineTimer private[nio] (initially: Long) extends NioTimer(-1L) with Deadline {
  at = initially
  def deadline: Long = at
  def moveTo(nanoTime: Long): Unit = move(nanoTime)
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

  /** Readies this op to run on the calling thread instead of the blocker pool (see `NioOnCaller`, which has recorded
    * the submission).
    */
  private[nio] def here(): BlockingHere = {
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
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def block(): Unit = publish(into, body())
}

/** Resolves a host name with `InetAddress`, which blocks, on the blocker pool. */
final class Resolve(val host: String, into: Slot[List[ResolvedAddress]]) extends NioBlocking {
  gears.async.io.Ops.checkHost(host)
  override private[nio] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  override protected def detachable: Boolean = true // its cancel need not wait for the lookup
  def block(): Unit = publish(into, Resolve.addresses(host))
}

private[nio] object Resolve {

  /** `host`'s addresses, in numeric form, looked up with `InetAddress`, which blocks: an IPv6 one as `inet_ntop` writes
    * it (`IpText`), where the JDK writes every group in full, and without a scope, as the Native backends give it.
    */
  def addresses(host: String): List[ResolvedAddress] = {
    val addresses =
      try InetAddress.getAllByName(host).toList
      catch case e: IOException => throw new IOException(s"Failed to resolve $host: ${e.getMessage}", e)
    gears.async.io.Ops.resolved(
      host,
      addresses.map {
        case v4: Inet4Address => ResolvedAddress(AddressFamily.IPv4, v4.getHostAddress)
        case v6               => ResolvedAddress(AddressFamily.IPv6, gears.async.io.IpText.ipv6(v6.getAddress))
      }
    )
  }
}
