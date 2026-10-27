package gears.async.io
package kqueue

import gears.async.io.BlockingOps
import gears.async.io.BufferPool
import gears.async.io.Completion
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
import gears.async.io.internal.BlockingRun
import gears.async.io.internal.Interest
import gears.async.io.internal.NativePool
import gears.async.io.internal.NonBlocking

import java.nio.ByteBuffer

import KqueueBackend.Handle

/** Everything `KqueueReactor` can run: its `Op` member type. Kqueue-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait KqueueOp extends gears.async.io.internal.TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

  // Whether this op has been submitted before, so a one-shot op is run once though submitted on two threads at once.
  private val submissions = new java.util.concurrent.atomic.AtomicBoolean()

  /** Records a submission, refusing a one-shot op submitted before with `IllegalStateException`. */
  private[kqueue] final def markSubmitted(): Unit =
    if submissions.getAndSet(true) && oneShot then
      throw new IllegalStateException(s"$this is not Repeatable and was already submitted")

  /** While the op is pending in the reactor: its completion, so the op is its own record and submitting it allocates
    * nothing. Taken off just before the completion runs, which may then submit the op again. Loop thread only.
    */
  private[kqueue] var completion: Completion[KqueueOp] | Null = null

  /** For an op that failed as soon as it was submitted, or a command whose descriptor was closed while it waited: its
    * failure, until its completion runs.
    */
  private[kqueue] var atOnceFailure: Throwable | Null = null

  /** The failure of an op submitted with a slot still holding a value (`Ops.notEmpty`), which fails it at once, before
    * it does anything; null if its slots are empty.
    */
  private[io] def slotRefusal: Throwable | Null = null

  /** The claim this op takes on its handle while pending (`Handle.reader` or `writer`); null for one that takes none.
    */
  private[io] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null = null

  /** Whether it may be submitted only once: an op that is not `Repeatable`, or a command that opens or closes. */
  private[io] def oneShot: Boolean = !this.isInstanceOf[Repeatable]

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[kqueue] var attachment: AnyRef | Null = null
}

/** Builds the ops `KqueueReactor` understands. */
object KqueueOps
    extends IpStreamOps[KqueueOp, Handle]
    with IpDatagramOps[KqueueOp, Handle]
    with UnixStreamOps[KqueueOp, Handle]
    with UnixDatagramOps[KqueueOp, Handle]
    with PipeOps[KqueueOp, Handle]
    with FifoOps[KqueueOp, Handle]
    with BlockingOps[KqueueOp, Handle]
    with FileOps[KqueueOp, Handle]
    with ResolveOps[KqueueOp, Handle] {
  type Readable = KqueueKinds.Readable
  type Writable = KqueueKinds.Writable
  type Connection = KqueueKinds.Connection
  type Listener = KqueueKinds.Listener
  type BoundDatagram = KqueueKinds.BoundDatagram
  type ConnectedDatagram = KqueueKinds.ConnectedDatagram
  type UnixBoundDatagram = KqueueKinds.UnixBoundDatagram
  type UnixConnectedDatagram = KqueueKinds.UnixConnectedDatagram
  type File = KqueueKinds.File

  def read(handle: Readable, buf: ByteBuffer): ReadIntoBuffer = on(handle)(ReadIntoBuffer(fd(handle), buf))
  def write(handle: Writable, buf: ByteBuffer): WriteFromBuffer = on(handle)(WriteFromBuffer(fd(handle), buf))
  def accept(handle: Listener, into: Slot[Connection]): Accept = on(handle)(Accept(fd(handle), into))
  def acceptAll(handle: Listener, into: Slot[Array[Connection]], max: Int): AcceptAll = {
    require(max > 0, s"max must be positive, not $max")
    on(handle)(new AcceptAll(fd(handle), into, max))
  }
  def connect(address: IpAddress, options: SocketOptions, into: Slot[Connection]): ConnectTo = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.Connection)
    new ConnectTo(gears.async.io.Ops.ip(address), into, options)
  }
  def connect(address: UnixAddress, into: Slot[Connection]): ConnectTo =
    new ConnectTo(gears.async.io.Ops.unix(address), into)
  def listen(address: IpAddress, options: SocketOptions, into: Slot[Listener]): KqueueOp = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.Listener)
    new Listen(gears.async.io.Ops.ip(address), into, options)
  }
  def listen(address: UnixAddress, into: Slot[Listener]): KqueueOp =
    new Listen(gears.async.io.Ops.unix(address), into)
  def bindDatagram(local: IpAddress, options: SocketOptions, into: Slot[BoundDatagram]): KqueueOp = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.BoundDatagram)
    new OpenDatagram(gears.async.io.Ops.ip(local), null, into, KqueueKinds.boundDatagram, options)
  }
  def connectDatagram(
      local: IpAddress | Null,
      remote: IpAddress,
      options: SocketOptions,
      into: Slot[ConnectedDatagram]
  ): KqueueOp = {
    gears.async.io.Ops.checkOptions(options, supports, SocketOption.Kind.ConnectedDatagram)
    val (l, r) = gears.async.io.Ops.ipPair(local, remote)
    new OpenDatagram(l, r, into, KqueueKinds.connectedDatagram, options)
  }
  def bindDatagram(local: UnixAddress, into: Slot[UnixBoundDatagram]): KqueueOp =
    new OpenDatagram(gears.async.io.Ops.unix(local), null, into, KqueueKinds.unixBoundDatagram)
  def connectDatagram(local: UnixAddress | Null, remote: UnixAddress, into: Slot[UnixConnectedDatagram]): KqueueOp =
    new OpenDatagram(
      if local == null then null else gears.async.io.Ops.unix(local),
      gears.async.io.Ops.unix(remote),
      into,
      KqueueKinds.unixConnectedDatagram
    )
  def pipe(read: Slot[Readable], write: Slot[Writable]): KqueueOp = new OpenPipe(read, write)
  def openFifoForReading(path: String, into: Slot[Readable]): KqueueOp =
    new OpenFifo(path, write = false, into, KqueueKinds.readable)
  def openFifoForWriting(path: String, into: Slot[Writable]): KqueueOp =
    new OpenFifo(path, write = true, into, KqueueKinds.writable)
  def receive(handle: BoundDatagram, buf: ByteBuffer, from: Slot[IpAddress]): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, from, unix = false))
  def receive(handle: ConnectedDatagram, buf: ByteBuffer): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, null, unix = false))
  def send(handle: BoundDatagram, buf: ByteBuffer, to: IpAddress): SendTo =
    on(handle)(new SendTo(fd(handle), buf, gears.async.io.Ops.ip(to)))
  def send(handle: ConnectedDatagram, buf: ByteBuffer): SendTo = on(handle)(new SendTo(fd(handle), buf, null))
  @annotation.targetName("receiveUnix")
  def receive(handle: UnixBoundDatagram, buf: ByteBuffer, from: Slot[UnixAddress]): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, from, unix = true))
  @annotation.targetName("receiveUnixConnected")
  def receive(handle: UnixConnectedDatagram, buf: ByteBuffer): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, null, unix = true))
  def send(handle: UnixBoundDatagram, buf: ByteBuffer, to: UnixAddress): SendTo =
    on(handle)(new SendTo(fd(handle), buf, gears.async.io.Ops.unix(to)))
  @annotation.targetName("sendUnixConnected")
  def send(handle: UnixConnectedDatagram, buf: ByteBuffer): SendTo = on(handle)(new SendTo(fd(handle), buf, null))
  def timer(milliseconds: Int): KqueueTimer = new KqueueTimer(milliseconds)
  def blocking(body: () => Unit): KqueueBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Readable, pool: BufferPool, into: Slot[Received]): PooledRead =
    on(handle)(PooledRead(fd(handle), KqueueOps.native(pool), into))
  def close(handle: Handle): KqueueOp = {
    KqueueHandles.checkOwn(handle)
    new Close(handle)
  }
  def shutdownOutput(handle: Connection): KqueueOp = new ShutdownOutput(handle)

  // Socket options: every one, on macOS and Linux alike, changed at any time but `ReusePort`, set before binding.
  def supports(option: SocketOption[?]): Boolean = true
  def canChange(option: SocketOption[?]): Boolean = option != SocketOption.ReusePort
  def option[V](handle: Handle, option: SocketOption[V]): V =
    onSocket(handle, option)(KqueueHandles.optionOf(_, option).asInstanceOf[V])
  def setOption[V](handle: Handle, option: SocketOption[V], value: V): Unit = {
    option.check(value)
    if !canChange(option) then throw new IllegalArgumentException(s"$option is given as a socket opens, not changed")
    onSocket(handle, option)(KqueueHandles.setOption(_, option, value))
  }

  /** Runs `body` on `handle`'s descriptor, an IP socket `option` applies to; else an `IllegalArgumentException`. */
  private def onSocket[A](handle: Handle, option: SocketOption[?])(body: Int => A): A = {
    KqueueHandles.checkOwn(handle)
    KqueueHandles.using(handle) {
      val kind = KqueueHandles.socketKind(handle.fd)
      if kind == null then throw new IllegalArgumentException(s"$option applies to IP sockets, not $handle")
      if !SocketOption.onSocket(option, kind.nn) then throw gears.async.io.Ops.notFor(option, kind.nn)
      body(handle.fd)
    }
  }
  def localAddress(handle: Listener | Connection): IpAddress | UnixAddress =
    KqueueHandles.addressOf(handle, peer = false)
  def peerAddress(handle: Connection): IpAddress | UnixAddress = KqueueHandles.addressOf(handle, peer = true)
  @annotation.targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagram | ConnectedDatagram): IpAddress = ip(handle, peer = false)
  @annotation.targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagram): IpAddress = ip(handle, peer = true)
  @annotation.targetName("unixDatagramLocalAddress")
  def localAddress(handle: UnixBoundDatagram | UnixConnectedDatagram): UnixAddress = unix(handle, peer = false)
  @annotation.targetName("unixDatagramPeerAddress")
  def peerAddress(handle: UnixConnectedDatagram): UnixAddress = unix(handle, peer = true)

  private def ip(handle: Handle, peer: Boolean): IpAddress = KqueueHandles.addressOf(handle, peer) match {
    case ip: IpAddress => ip
    case other         => throw new java.io.IOException(s"$handle is not an IP socket: $other")
  }
  private def unix(handle: Handle, peer: Boolean): UnixAddress = KqueueHandles.addressOf(handle, peer) match {
    case unix: UnixAddress => unix
    case other             => throw new java.io.IOException(s"$handle is not a Unix socket: $other")
  }
  def open(path: String, write: Boolean, into: Slot[File]): KqueueOp = new OpenFile(path, write, into)
  def read(handle: File, buf: ByteBuffer, offset: Long): PositionalRead = new PositionalRead(handle, buf, offset)
  def write(handle: File, buf: ByteBuffer, offset: Long): PositionalWrite = new PositionalWrite(handle, buf, offset)
  def sync(handle: File): FileSync = new FileSync(handle)

  // Kqueue's own readiness, outside the interface, whose ops say what is done rather than what is waited for: for code
  // written against kqueue that does its own operations once a handle is ready, as its demos do.

  /** Completes once `handle` is ready for `interest`. The caller then uses the handle without waiting (`readNow`,
    * `writeNow`) until it would block, and submits the op again to wait for the next readiness. Level-triggered:
    * submitted again while the handle is still ready, it completes at once.
    */
  def ready(handle: Handle, interest: Interest): Ready = on(handle)(new Ready(fd(handle), interest))

  /** Reads what is available now into `buf`, without waiting: the bytes read, 0 if nothing is, or -1 at end of stream.
    * Throws `IllegalArgumentException` if `buf` has no room, as `read` fails. Not an op: it runs on the calling thread.
    */
  def readNow(handle: Readable, buf: ByteBuffer): Int = KqueueHandles.readNow(handle, buf)

  /** Writes what fits now from `buf`, without waiting: the bytes written, or -1 if nothing fits; 0 if `buf` has nothing
    * remaining, writing nothing. Not an op.
    */
  def writeNow(handle: Writable, buf: ByteBuffer): Int = KqueueHandles.writeNow(handle, buf)

  private def fd(handle: Handle): Int = KqueueHandles.fd(handle)

  /** `command`, carrying `handle`, which the reactor takes for each use of the descriptor (see `Command.handle`). */
  private def on[C <: Command](handle: Handle)(command: C): C = {
    KqueueHandles.checkOwn(handle)
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

/** An op that opens a handle and writes it into a slot (see `Ops`): if its submission is cancelled, by `cancel`, a time
  * limit or the reactor closing, before its completion has run, `abandon` closes what it opened and empties its slot,
  * so that nothing is left open that the caller never got.
  */
sealed trait Opens extends KqueueOp {
  def abandon(): Unit
}

/** An op that opens its handle at once, when submitted (see `KqueueAtOnce`), as kqueue's listeners, datagram sockets,
  * pipes and FIFOs are: `opened` is what it opened, written into `into` by the time the op completes.
  */
sealed abstract class OpensAtOnce[K <: Handle](into: Slot[K], kind: Handle => K) extends KqueueAtOnce with Opens {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  private var opened = -1
  protected def open(): Int
  def perform(): Unit = {
    opened = open()
    into.set(kind(KqueueHandles.handle(opened)))
  }
  def abandon(): Unit = if opened >= 0 then {
    into.clear(): Unit
    KqueueHandles.closeDescriptor(opened)
    opened = -1
  }
}

/** A stream socket bound and listening on `address` (see `IpStreamOps.listen`). */
final class Listen(
    address: SocketAddress,
    into: Slot[KqueueKinds.Listener],
    options: SocketOptions = SocketOptions.default
) extends OpensAtOnce(into, KqueueKinds.listener) {
  protected def open(): Int = KqueueHandles.listenStream(address, options)
}

/** A datagram socket, bound and connected where given (see `IpDatagramOps.connectDatagram`). */
final class OpenDatagram[K <: Handle](
    local: SocketAddress | Null,
    remote: SocketAddress | Null,
    into: Slot[K],
    kind: Handle => K,
    options: SocketOptions = SocketOptions.default
) extends OpensAtOnce(into, kind) {
  protected def open(): Int = KqueueHandles.datagramSocket(local, remote, options)
}

/** A FIFO, opened without waiting for its other end (see `FifoOps.openFifoForReading` and `openFifoForWriting`). */
final class OpenFifo[K <: Handle](path: String, write: Boolean, into: Slot[K], kind: Handle => K)
    extends OpensAtOnce(into, kind) {
  gears.async.io.Ops.checkPath(path)
  protected def open(): Int = KqueueHandles.fifoDescriptor(path, write)
}

/** A pipe, its read end written into `read` and its write end into `write` (see `PipeOps.pipe`). */
final class OpenPipe(read: Slot[KqueueKinds.Readable], write: Slot[KqueueKinds.Writable])
    extends KqueueAtOnce
    with Opens {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(read, write)
  private var ends: (Int, Int) | Null = null
  def perform(): Unit = {
    val (in, out) = KqueueHandles.pipeDescriptors()
    ends = (in, out)
    read.set(KqueueKinds.readable(KqueueHandles.handle(in)))
    write.set(KqueueKinds.writable(KqueueHandles.handle(out)))
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

/** A stream socket connected to `address` (see `IpStreamOps.connect`): the socket is opened, and its non-blocking
  * connect started, when the op is submitted (`open`), and the op then waits as `Connect` does. Done once connected,
  * after writing the socket into `into`; if the connect fails, or the submission is cancelled first, the socket is
  * closed and `into` left empty.
  */
final class ConnectTo(
    address: SocketAddress,
    into: Slot[KqueueKinds.Connection],
    options: SocketOptions = SocketOptions.default
) extends Command
    with Opens {
  override private[io] def oneShot: Boolean = true // it opens its socket
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  private var socket = -1
  private var connected = false
  def fd: Int = socket
  def interest: Interest = Interest.Write
  override def tryFirst: Boolean = false // the check passes while the connection is still in progress
  override def registersReads: Boolean = true

  /** Opens the socket and starts connecting it, for the reactor, as the op is submitted. */
  private[kqueue] def open(): Unit = {
    socket = KqueueHandles.connectStream(address, options)
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
    into.set(KqueueKinds.connection(KqueueHandles.handle(socket)))
    true
  }

  def abandon(): Unit = if socket >= 0 then {
    if connected then into.clear(): Unit
    KqueueHandles.closeDescriptor(socket)
    socket = -1
    connected = false
  }
}

/** Closes a handle (see `Ops.close` and `KqueueBackend.Handle`): at once in meaning, and the descriptor itself at once
  * too if no use of it is in progress, as nearly always, completing as an at-once op does; otherwise once the last use
  * ends, on whose thread the descriptor is closed and this completed, as a promise is from another thread.
  */
final class Close(val handle: Handle) extends KqueuePromise {

  /** Starts closing: true if that finished here, the descriptor closed or the close failed, its failure in
    * `atOnceFailure`; false if the last use of the descriptor closes it, completing this.
    */
  private[kqueue] def start(): Boolean =
    try KqueueHandles.closeHandle(handle, failure => if failure == null then signal() else fail(failure.nn))
    catch {
      case t: Throwable =>
        atOnceFailure = t
        true
    }
}

/** Shuts a socket down for writing (see `StreamSocketOps.shutdownOutput`): at once, since the kernel sends what is
  * queued, then the end of the stream, after the call returns.
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

  override private[io] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null = {
    val h = handle
    if h == null then null else if interest == Interest.Read then h.reader else h.writer
  }

  /** The handle the command was built on, through `KqueueOps`, which the reactor takes around each use of the
    * descriptor, the command's system call and its registration, failing the command if it is closed; null for a
    * command built on a descriptor directly, as kqueue's own code does.
    */
  private[kqueue] var handle: KqueueBackend.Handle | Null = null

  /** Whether the reactor may perform this command as soon as it is submitted, before waiting for readiness. True for
    * operations that simply report would-block when not ready, so trying first only saves the wait when they are.
    */
  def tryFirst: Boolean = true

  /** Why the command cannot be performed as it stands, such as a read into a buffer with no room, or null: then it
    * fails as it is submitted, before readiness is looked at, and is never performed.
    */
  def refusal: Throwable | Null = null

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

/** Readiness itself, for `KqueueOps.ready`: done as soon as kqueue reports the descriptor ready, which it does at once
  * while the descriptor stays so, since the command waits on a one-shot, level-triggered filter each time.
  */
final class Ready(val fd: Int, val interest: Interest) extends Command {
  def perform(): Boolean = true
  override def tryFirst: Boolean = false // the wait is the point: only kqueue says the descriptor is ready
}

/** An op for another I/O facility than kqueue's, driven by a kqueue reactor (see `ReactorExtension`), such as
  * Network.framework's, whose outcome comes as a notification rather than as a descriptor's readiness: attempted on the
  * loop thread as it is submitted, and again each time the facility notifies the reactor for its submission's token
  * (`KqueueReactor.notified`), until it finishes. A backend built on kqueue's reactors subclasses it, so that its ops
  * and kqueue's run on the same reactor. Repeatable, unless `oneShot` says otherwise.
  */
abstract class Notified extends KqueueOp with Repeatable {

  /** The current submission's token, which a notification names. Loop thread only. */
  private[io] var token = 0L

  /** Readies a submission before its first attempt, such as by opening what it waits on; a throw fails the op. */
  private[io] def begin(): Unit = ()

  /** Tries to finish, on `reactor`'s thread, `first` as it is submitted: true once done, its results in the caller's
    * buffers and slots; false having arranged for the facility to notify `reactor` with `token`. A failure is thrown.
    */
  private[io] def attempt(reactor: KqueueReactor, first: Boolean): Boolean

  /** Withdraws the arrangement of a submission cancelled, timed out, or dropped as its reactor closes. */
  private[io] def withdraw(): Unit

  /** After a submission withdrawn: closes what it opened, which never reaches the caller. */
  private[io] def abandon(): Unit = ()

  /** Whether it closes a handle, so that its completion runs after the failures that causes (see `Ops.close`). */
  private[io] def closes: Boolean = false
}

/** Another I/O facility a kqueue reactor drives, besides kqueue: one whose events come on threads of its own, such as
  * Network.framework's on Dispatch's, which queue them and wake the reactor with its extension event
  * (`KqueueReactor.ExtensionEvent`, a user event on its kqueue, triggered from anywhere). The reactor takes them in on
  * its thread each turn, and the ops they finish (`Notified`) are notified there.
  */
trait ReactorExtension {

  /** On the loop thread, each turn: takes in what the facility delivered, notifying the ops it finishes. Returns
    * whether anything came.
    */
  def deliver(): Boolean

  /** On the loop thread as the reactor closes, once none of the facility's ops is pending there, before its kqueue
    * closes: nothing may trigger the extension event after this returns.
    */
  def close(): Unit
}

/** A one-shot timer, kept in the reactor's heap while pending (`TimerHeap.Entry`). */
final class KqueueTimer(val milliseconds: Int) extends KqueueOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  /** Whether the pending submission's time limit is shorter than the timer, so it fails when due. Loop thread only. */
  private[kqueue] var expires = false
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `KqueueBlocking` completes itself from the blocker pool, and `Close`, when it cannot close at once, from the
  * thread of the descriptor's last use.
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
  * if not started, interrupted if running, and `onCancel` waits for `block` to return. One marked `Repeatable`, such as
  * a file op, is restarted on each submission.
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
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def block(): Unit = {
    publish(into, body())
  }
}
