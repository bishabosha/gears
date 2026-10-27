package gears.async.io
package nwframework

import gears.async.io.BlockingOps
import gears.async.io.BufferPool
import gears.async.io.FifoOps
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
import gears.async.io.UnixDatagramOps
import gears.async.io.UnixStreamOps
import gears.async.io.internal.NativeBuffer
import gears.async.io.internal.NativePool
import gears.async.io.kqueue.KqueueBackend.Handle
import gears.async.io.kqueue.KqueueKinds
import gears.async.io.kqueue.KqueueOp
import gears.async.io.kqueue.KqueueOps
import gears.async.io.kqueue.KqueueReactor
import gears.async.io.kqueue.Notified

import java.nio.ByteBuffer

import NWFrameworkHandles.owns

/** Builds the backend's ops, each Network.framework's or kqueue's, so that both run on the same kqueue reactor. An op
  * on a handle is that handle's: a Network.framework connection's, listener's or connected datagram socket's
  * (`NWFrameworkCommand`), or else kqueue's, on a descriptor. An opener chooses by what it opens, as Apple advises
  * (TN3151, "Choosing the right networking API"): Network.framework for TCP connections and listeners, and for UDP
  * between two peers, a connected datagram socket; kqueue's own code for what Network.framework does not do, or does no
  * better than the kernel: Unix domain sockets, a bound datagram socket that sends to and hears from anyone, pipes,
  * FIFOs and files; and, with `loopbackOnKqueue`, sockets on loopback, a listener there or a socket whose peer is on
  * this host, which Network.framework carries over the kernel's own loopback at several times kqueue's cost (see
  * docs/io/nwframework.md).
  *
  * Its kinds are opaque types of its own over kqueue's handle, so that a program cannot pass one to kqueue's ops, nor
  * kqueue's to these; underneath, kqueue's openers write the same handles into its slots.
  */
final class NWFrameworkOps private[nwframework] (loopbackOnKqueue: Boolean)
    extends IpStreamOps[KqueueOp, Handle]
    with IpDatagramOps[KqueueOp, Handle]
    with UnixStreamOps[KqueueOp, Handle]
    with UnixDatagramOps[KqueueOp, Handle]
    with PipeOps[KqueueOp, Handle]
    with FifoOps[KqueueOp, Handle]
    with BlockingOps[KqueueOp, Handle]
    with FileOps[KqueueOp, Handle]
    with ResolveOps[KqueueOp, Handle] {
  type Readable = NWFrameworkKinds.Readable
  type Writable = NWFrameworkKinds.Writable
  type Connection = NWFrameworkKinds.Connection
  type Listener = NWFrameworkKinds.Listener
  type BoundDatagram = NWFrameworkKinds.BoundDatagram
  type ConnectedDatagram = NWFrameworkKinds.ConnectedDatagram
  type UnixBoundDatagram = NWFrameworkKinds.UnixBoundDatagram
  type UnixConnectedDatagram = NWFrameworkKinds.UnixConnectedDatagram
  type File = NWFrameworkKinds.File

  /** Whether a listener on `address` goes to kqueue rather than Network.framework: one on loopback. One on another
    * address of this host is Network.framework's, since other hosts reach it.
    */
  private def listensOnKqueue(address: IpAddress): Boolean =
    loopbackOnKqueue && gears.async.io.IpText.isLoopback(address)

  /** Whether a socket connected to `peer` goes to kqueue rather than Network.framework: one whose peer is on this host,
    * on loopback or at another of its addresses (`nwf_is_host_address`, from `getifaddrs`), whose traffic the kernel
    * carries over loopback whichever, and which Network.framework, given a local address on some interfaces, such as
    * `feth`'s, never makes ready (probed on macOS 27.0).
    */
  private def connectsOnKqueue(peer: IpAddress): Boolean =
    loopbackOnKqueue && (gears.async.io.IpText.isLoopback(peer) || NWFrameworkOps.isHostAddress(peer))

  /** `into`, for kqueue's opener: the same handles, under kqueue's kind of the same name. */
  private def forKqueue[K <: AnyRef](into: Slot[?]): Slot[K] = into.asInstanceOf[Slot[K]]

  def read(handle: Readable, buf: ByteBuffer): KqueueOp & Repeatable =
    if owns(handle) then new NWFrameworkRead(handle, buf) else KqueueOps.read(KqueueKinds.readable(handle), buf)
  def write(handle: Writable, buf: ByteBuffer): KqueueOp & Repeatable =
    if owns(handle) then new NWFrameworkWrite(handle, buf) else KqueueOps.write(KqueueKinds.writable(handle), buf)
  def read(handle: Readable, pool: BufferPool, into: Slot[Received]): KqueueOp & Repeatable =
    if owns(handle) then new NWFrameworkPooledRead(handle, native(pool), into)
    else KqueueOps.read(KqueueKinds.readable(handle), pool, into)
  def accept(handle: Listener, into: Slot[Connection]): KqueueOp & Repeatable =
    if owns(handle) then new NWFrameworkAccept(handle, into)
    else KqueueOps.accept(KqueueKinds.listener(handle), forKqueue(into))
  def acceptAll(handle: Listener, into: Slot[Array[Connection]], max: Int): KqueueOp & Repeatable = {
    require(max > 0, s"max must be positive, not $max")
    if owns(handle) then new NWFrameworkAcceptAll(handle, into, max)
    else KqueueOps.acceptAll(KqueueKinds.listener(handle), forKqueue(into), max)
  }
  def shutdownOutput(handle: Connection): KqueueOp =
    if owns(handle) then new NWFrameworkShutdownOutput(handle)
    else KqueueOps.shutdownOutput(KqueueKinds.connection(handle))
  def localAddress(handle: Listener | Connection): IpAddress | UnixAddress =
    if owns(handle) then NWFrameworkHandles.get(handle).address(peer = false)
    else KqueueOps.localAddress(KqueueKinds.connection(handle))
  def peerAddress(handle: Connection): IpAddress | UnixAddress =
    if owns(handle) then NWFrameworkHandles.get(handle).address(peer = true)
    else KqueueOps.peerAddress(KqueueKinds.connection(handle))
  @annotation.targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagram | ConnectedDatagram): IpAddress =
    if owns(handle) then NWFrameworkHandles.get(handle).address(peer = false)
    else KqueueOps.localAddress(KqueueKinds.connectedDatagram(handle))
  @annotation.targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagram): IpAddress =
    if owns(handle) then NWFrameworkHandles.get(handle).address(peer = true)
    else KqueueOps.peerAddress(KqueueKinds.connectedDatagram(handle))
  @annotation.targetName("unixDatagramLocalAddress")
  def localAddress(handle: UnixBoundDatagram | UnixConnectedDatagram): UnixAddress =
    KqueueOps.localAddress(KqueueKinds.unixConnectedDatagram(handle))
  @annotation.targetName("unixDatagramPeerAddress")
  def peerAddress(handle: UnixConnectedDatagram): UnixAddress =
    KqueueOps.peerAddress(KqueueKinds.unixConnectedDatagram(handle))
  def close(handle: Handle): KqueueOp =
    if owns(handle) then new NWFrameworkClose(handle) else KqueueOps.close(handle)

  def connect(address: IpAddress, into: Slot[Connection]): KqueueOp = {
    val ip = gears.async.io.Ops.ip(address)
    if connectsOnKqueue(ip) then KqueueOps.connect(ip, forKqueue(into)) else new NWFrameworkConnectTo(ip, into)
  }
  def listen(address: IpAddress, into: Slot[Listener]): KqueueOp = {
    val ip = gears.async.io.Ops.ip(address)
    if listensOnKqueue(ip) then KqueueOps.listen(ip, forKqueue(into)) else new NWFrameworkListen(ip, into)
  }

  /** A connected datagram socket is Network.framework's, unless its peer is on this host (see `connectsOnKqueue`). */
  def connectDatagram(local: IpAddress | Null, remote: IpAddress, into: Slot[ConnectedDatagram]): KqueueOp = {
    val (l, r) = gears.async.io.Ops.ipPair(local, remote)
    if connectsOnKqueue(r.nn) then KqueueOps.connectDatagram(l, r.nn, forKqueue(into))
    else new NWFrameworkOpenDatagram(l, r.nn, into)
  }
  def receive(handle: ConnectedDatagram, buf: ByteBuffer): KqueueOp & Repeatable =
    if owns(handle) then new NWFrameworkReceive(handle, buf)
    else KqueueOps.receive(KqueueKinds.connectedDatagram(handle), buf)
  def send(handle: ConnectedDatagram, buf: ByteBuffer): KqueueOp & Repeatable =
    if owns(handle) then new NWFrameworkSend(handle, buf)
    else KqueueOps.send(KqueueKinds.connectedDatagram(handle), buf)

  /** A bound datagram socket, which sends to and hears from anyone, is kqueue's: Network.framework has no such socket
    * (see `NWFrameworkOps`).
    */
  def bindDatagram(local: IpAddress, into: Slot[BoundDatagram]): KqueueOp =
    KqueueOps.bindDatagram(local, forKqueue(into))
  def receive(handle: BoundDatagram, buf: ByteBuffer, from: Slot[IpAddress]): KqueueOp & Repeatable =
    KqueueOps.receive(KqueueKinds.boundDatagram(handle), buf, from)
  def send(handle: BoundDatagram, buf: ByteBuffer, to: IpAddress): KqueueOp & Repeatable =
    KqueueOps.send(KqueueKinds.boundDatagram(handle), buf, to)

  // Kqueue's alone.
  def connect(address: UnixAddress, into: Slot[Connection]): KqueueOp = KqueueOps.connect(address, forKqueue(into))
  def listen(address: UnixAddress, into: Slot[Listener]): KqueueOp = KqueueOps.listen(address, forKqueue(into))
  def bindDatagram(local: UnixAddress, into: Slot[UnixBoundDatagram]): KqueueOp =
    KqueueOps.bindDatagram(local, forKqueue(into))
  def connectDatagram(local: UnixAddress | Null, remote: UnixAddress, into: Slot[UnixConnectedDatagram]): KqueueOp =
    KqueueOps.connectDatagram(local, remote, forKqueue(into))
  @annotation.targetName("receiveUnix")
  def receive(handle: UnixBoundDatagram, buf: ByteBuffer, from: Slot[UnixAddress]): KqueueOp & Repeatable =
    KqueueOps.receive(KqueueKinds.unixBoundDatagram(handle), buf, from)
  @annotation.targetName("receiveUnixConnected")
  def receive(handle: UnixConnectedDatagram, buf: ByteBuffer): KqueueOp & Repeatable =
    KqueueOps.receive(KqueueKinds.unixConnectedDatagram(handle), buf)
  def send(handle: UnixBoundDatagram, buf: ByteBuffer, to: UnixAddress): KqueueOp & Repeatable =
    KqueueOps.send(KqueueKinds.unixBoundDatagram(handle), buf, to)
  @annotation.targetName("sendUnixConnected")
  def send(handle: UnixConnectedDatagram, buf: ByteBuffer): KqueueOp & Repeatable =
    KqueueOps.send(KqueueKinds.unixConnectedDatagram(handle), buf)
  def pipe(read: Slot[Readable], write: Slot[Writable]): KqueueOp = KqueueOps.pipe(forKqueue(read), forKqueue(write))
  def openFifoForReading(path: String, into: Slot[Readable]): KqueueOp =
    KqueueOps.openFifoForReading(path, forKqueue(into))
  def openFifoForWriting(path: String, into: Slot[Writable]): KqueueOp =
    KqueueOps.openFifoForWriting(path, forKqueue(into))
  def timer(milliseconds: Int): KqueueOp & Repeatable = KqueueOps.timer(milliseconds)
  def blocking(body: () => Unit): KqueueOp = KqueueOps.blocking(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueOp = KqueueOps.blocking(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): KqueueOp = KqueueOps.resolve(host, into)
  def open(path: String, write: Boolean, into: Slot[File]): KqueueOp = KqueueOps.open(path, write, forKqueue(into))
  def read(handle: File, buf: ByteBuffer, offset: Long): KqueueOp & Repeatable =
    KqueueOps.read(KqueueKinds.file(handle), buf, offset)
  def write(handle: File, buf: ByteBuffer, offset: Long): KqueueOp & Repeatable =
    KqueueOps.write(KqueueKinds.file(handle), buf, offset)
  def sync(handle: File): KqueueOp & Repeatable = KqueueOps.sync(KqueueKinds.file(handle))

  private def native(pool: BufferPool): NativePool = pool match {
    case native: NativePool => native
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op on a Network.framework handle, run by a kqueue reactor (see `Notified`): `attemptOn` tries to finish it on the
  * reactor's thread, after the handle has taken in what the framework delivered: true once done, with its results in
  * the caller's buffers and slots; false when it has to wait, registered on the handle with the reactor and the
  * submission's token, so that the handle notifies the reactor to attempt again; a failure is thrown.
  */
sealed abstract class NWFrameworkCommand extends Notified {
  def handle: Handle

  /** What the command waits for (`NWFrameworkHandle.Read`, `Write`, `Connect` or `Close`). */
  def kind: Int

  override private[io] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null =
    if kind == NWFrameworkHandle.Read then handle.reader
    else if kind == NWFrameworkHandle.Write then handle.writer
    else null

  /** The handle the current submission resolved to, kept for its attempts since the handle may be closed meanwhile.
    * Loop thread only.
    */
  private[nwframework] var target: NWFrameworkHandle | Null = null

  override private[io] def begin(): Unit = target = NWFrameworkHandles.get(handle)

  /** On submission the handle takes in its events first, so that an op finishes at once when it can; attempted again
    * after a notification, they were taken in by the drain that led to it.
    */
  private[io] def attempt(reactor: KqueueReactor, first: Boolean): Boolean = {
    val h = target.nn
    if first then h.drain(NWFrameworkExtension.of(reactor).event)
    attemptOn(h, reactor)
  }

  private[io] def withdraw(): Unit = {
    target.nn.unwait(kind, token)
    reset()
  }

  /** Forgets what an attempt began, for a submission withdrawn. */
  protected def reset(): Unit = ()

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean
}

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream; fails if
  * `buf` has no room, since a read of nothing would look like the end of the stream.
  */
final class NWFrameworkRead(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean =
    if !buf.hasRemaining() then throw gears.async.io.Ops.bufferFull(buf)
    else {
      val n = h.read(NativeBuffer.atPosition(buf), buf.remaining(), reactor, token)
      if n > 0 then NativeBuffer.advance(buf, n)
      n >= 0
    }
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read`). */
final class NWFrameworkPooledRead(val handle: Handle, pool: NativePool, into: Slot[Received])
    extends NWFrameworkCommand {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    val received = h.readPooled(pool, reactor, token)
    if received == null then false
    else {
      into.set(received.nn)
      true
    }
  }
}

/** Accepts the connections waiting on a listener, at most `max`, writing their handles into `into`. */
final class NWFrameworkAcceptAll(val handle: Handle, into: Slot[Array[NWFrameworkKinds.Connection]], max: Int)
    extends NWFrameworkCommand {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    val accepted = h.acceptAll(max, reactor, token)
    if accepted == null then false
    else {
      into.set(accepted.map(a => NWFrameworkKinds.connection(NWFrameworkHandles.accepted(a))))
      true
    }
  }
}

/** Accepts one connection on a listener, writing its handle into `into`. */
final class NWFrameworkAccept(val handle: Handle, into: Slot[NWFrameworkKinds.Connection]) extends NWFrameworkCommand {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    val accepted = h.accept(reactor, token)
    if accepted == null then false
    else {
      into.set(NWFrameworkKinds.connection(NWFrameworkHandles.accepted(accepted)))
      true
    }
  }
}

/** A command that opens the handle it waits on, and writes it into a slot (see `Ops`): `make` makes the handle as the
  * command is submitted, starting what Network.framework does asynchronously, a connection or a listener, and the
  * command then waits for it to be ready. Done once it is, after writing the handle into `into`; if it fails, or the
  * submission is cancelled first, by `cancel`, a time limit or the reactor closing, `abandon` closes the handle and
  * leaves `into` empty.
  */
sealed abstract class NWFrameworkOpening[K <: Handle](into: Slot[K], kindOf: Handle => K) extends NWFrameworkCommand {
  override private[io] def oneShot: Boolean = true // it opens its handle
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  private var opened: Handle | Null = null
  private var delivered = false
  def kind: Int = NWFrameworkHandle.Connect
  def handle: Handle = opened.nn

  /** Makes the handle. */
  protected def make(): Handle

  override private[io] def begin(): Unit = {
    opened = make()
    delivered = false
    super.begin()
  }

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    val done =
      try h.connected(reactor, token)
      catch {
        case t: Throwable =>
          abandon()
          throw t
      }
    if done then {
      into.set(kindOf(opened.nn))
      delivered = true
    }
    done
  }

  override private[io] def abandon(): Unit = if opened != null then {
    if delivered then into.clear(): Unit
    NWFrameworkHandles.close(opened.nn)
    opened = null
    delivered = false
  }
}

/** A connection to `address` (see `IpStreamOps.connect`): ready once Network.framework has it ready, failing with its
  * error if it failed.
  */
final class NWFrameworkConnectTo(address: IpAddress, into: Slot[NWFrameworkKinds.Connection])
    extends NWFrameworkOpening(into, NWFrameworkKinds.connection) {
  protected def make(): Handle = NWFrameworkHandles.connect(address)
}

/** A listener on `address` (see `IpStreamOps.listen`): ready once it is listening, failing if it cannot. */
final class NWFrameworkListen(address: IpAddress, into: Slot[NWFrameworkKinds.Listener])
    extends NWFrameworkOpening(into, NWFrameworkKinds.listener) {
  protected def make(): Handle = NWFrameworkHandles.listener(address)
}

/** A datagram socket connected to `remote` (see `IpDatagramOps.connectDatagram`), bound to `local` if given: ready once
  * Network.framework has its connection ready, failing if it cannot be.
  */
final class NWFrameworkOpenDatagram(
    local: IpAddress | Null,
    remote: IpAddress,
    into: Slot[NWFrameworkKinds.ConnectedDatagram]
) extends NWFrameworkOpening(into, NWFrameworkKinds.connectedDatagram) {
  protected def make(): Handle = NWFrameworkHandles.datagram(local, remote)
}

/** Closes a handle, completing once it is closed (see `Ops.close`): a listener's cancellation is waited for, and its
  * number taken back once the op is done, so that the reactor still finds the handle for the cancelled state meanwhile;
  * ops submitted on it meanwhile fail as closed.
  */
final class NWFrameworkClose(val handle: Handle) extends NWFrameworkCommand {
  override private[io] def oneShot: Boolean = true
  override private[io] def closes: Boolean = true
  def kind: Int = NWFrameworkHandle.Close
  private var begun = false

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    val done =
      if begun then h.closeDone
      else {
        begun = true
        h.closeAsync(reactor, token) // throws if already closed, so the op fails
      }
    if done then NWFrameworkHandles.remove(handle): Unit
    done
  }
}

/** Ends a connected stream's sending side (see `StreamSocketOps.shutdownOutput`): at once, the end queued after the
  * sends before it.
  */
final class NWFrameworkShutdownOutput(val handle: Handle) extends NWFrameworkCommand {
  override private[io] def oneShot: Boolean = true
  def kind: Int = NWFrameworkHandle.Write

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    h.shutdownOutput()
    true
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written: once as much as the handle's
  * send window has room for has been copied and sent, at once if it has room, else once a send has been processed by
  * the stack, having sent nothing until then (see `NWFrameworkHandle.write`). With nothing remaining, done at once.
  */
final class NWFrameworkWrite(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Write
  private var sent = false // with no window: the bytes were handed over, and the handle tells once one is processed

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean =
    if sent then {
      sent = false
      h.sent()
    } else if !buf.hasRemaining() then true // nothing to write: done
    else {
      val n = h.write(NativeBuffer.atPosition(buf), buf.remaining(), reactor, token)
      if n > 0 then {
        NativeBuffer.advance(buf, n)
        true
      } else if n == 0 then false // the window is full: told once a send is processed, then tried again
      else {
        NativeBuffer.advance(buf, -n)
        sent = true
        false
      }
    }

  override protected def reset(): Unit = sent = false
}

/** Receives one datagram, from the peer, into `buf`, which is left flipped for reading. */
final class NWFrameworkReceive(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean = {
    if buf.capacity() == 0 then throw gears.async.io.Ops.noCapacity(buf) // the packet would be dropped
    buf.clear()
    val packet = h.receivePacket(reactor, token)
    if packet == null then false
    else {
      val n = math.min(packet.length, buf.remaining()) // truncated beyond the buffer, as a datagram is
      buf.put(packet, 0, n)
      buf.flip()
      true
    }
  }
}

/** Sends `buf` as one datagram to the socket's peer, consuming it; never an empty one, as on every backend (see
  * `IpDatagramOps.send`).
  */
final class NWFrameworkSend(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Write
  private var sent = false

  protected def attemptOn(h: NWFrameworkHandle, reactor: KqueueReactor): Boolean =
    if sent then {
      sent = false
      h.sent()
    } else {
      val size = buf.remaining()
      if size == 0 then throw gears.async.io.Ops.emptyPacket(buf)
      val done = h.send(NativeBuffer.atPosition(buf), size, reactor, token)
      buf.position(buf.limit()) // a datagram goes whole or not at all
      sent = !done
      done
    }

  override protected def reset(): Unit = sent = false
}

private[nwframework] object NWFrameworkOps {

  /** Whether `address` is one of this host's own, asked of `getifaddrs` each time, since interfaces come and go. */
  def isHostAddress(address: IpAddress): Boolean = gears.async.io.IpText.unmapped(address) match {
    case IpAddress.IPv4(host, _) => scala.scalanative.unsafe.Zone.acquire(z => ask(host)(using z))
    case IpAddress.IPv6(host, _) => scala.scalanative.unsafe.Zone.acquire(z => ask(host)(using z))
  }

  private def ask(host: String)(using scala.scalanative.unsafe.Zone): Boolean =
    NWFrameworkIo.nwf_is_host_address(scala.scalanative.unsafe.toCString(host)) != 0
}
