package gears.async.io
package node

import gears.async.Async
import gears.async.js.UnsafeJsAsyncFromSync

import java.nio.ByteBuffer
import scala.annotation.targetName
import scala.concurrent.duration.FiniteDuration
import scala.scalajs.js

/** The Node.js backend: the interface on Node's own I/O, for a Scala.js program linked to WebAssembly and run by
  * Node.js, which suspends with JSPI.
  *
  * A program runs on Node's one thread, its event loop, as gears' futures do on Scala.js. An op is performed by the
  * future performing it: begun at once, and if Node has not what completes it, left with Node as a listener or a
  * callback while the future is suspended, which the event loop resumes once it has. Nothing blocks, so that every
  * future, timer and socket goes on while one waits; and since there is one thread, what a program does between two
  * suspensions no other future sees half done.
  *
  * A run suspends its caller, so `Io.run` over this backend must be called where JSPI can suspend: within Scala.js's
  * `js.async`, as a gears program is on Scala.js. `start` does both, for a program started from JavaScript's side, a
  * main method or a callback.
  */
object NodeBackend extends Backend {
  type Interface = NodeOps.type

  def pool(bufferSize: Int, buffers: Int): BufferPool = new NodePool(bufferSize, buffers)

  private val io: Io[Interface] = new Io[Interface] {
    val backend: NodeBackend.type = NodeBackend
    val ops: NodeOps.type = NodeOps
    private[io] def performWithin(op: NodeOp, timeoutMillis: Long)(using Async): Unit = op.perform(timeoutMillis)
  }

  /** Runs `program` on the event loop, where its caller is, which is suspended until it returns. */
  private[io] def run[T](program: Io[Interface] ?=> Async.Spawn ?=> T): T =
    Async.fromSync(using UnsafeJsAsyncFromSync)(program(using io))

  /** Starts `program` as `Io.run` runs it, from where nothing can be suspended, and returns at once: the promise is
    * resolved with what the program returns, or rejected with its failure.
    */
  def start[T](program: Io[Interface] ?=> Async.Spawn ?=> T): js.Promise[T] = js.async(run(program))
}

/** The Node.js backend's ops, each checked as it is built. */
object NodeOps extends Pipes with IpStreams with IpDatagrams {
  import SocketOption.Kind

  type Op = NodeOp
  type Handle = NodeHandle
  type Readable = ReadEnd
  type Writable = WriteEnd
  type Connection = ConnectionHandle
  type Listener = ListenerHandle
  type BoundDatagram = BoundDatagramHandle
  type ConnectedDatagram = ConnectedDatagramHandle

  private def fillable(buf: ByteBuffer): ByteBuffer = {
    if buf.isReadOnly() then throw new IllegalArgumentException(s"A read-only buffer cannot be read into: $buf")
    buf
  }

  private def several(bufs: Array[ByteBuffer]): Array[ByteBuffer] = {
    if bufs.isEmpty then throw new IllegalArgumentException("A vectored op takes at least one buffer")
    bufs
  }

  // ---- Every backend's

  def timer(duration: FiniteDuration): NodeOp & Repeatable = {
    if duration.length < 0 then throw new IllegalArgumentException(s"A timer's time is not negative: $duration")
    new TimerOp(duration.toNanos)
  }

  def deadlineTimer(nanoTime: Long): NodeOp & Repeatable & Deadline = new DeadlineOp(nanoTime)

  // ---- Streams

  def read(handle: ReadEnd, buf: ByteBuffer): NodeOp & Repeatable = new ReadOp(handle, Array(fillable(buf)))

  def write(handle: WriteEnd, buf: ByteBuffer): NodeOp & Repeatable = new WriteOp(handle, Array(buf))

  def read(handle: ReadEnd, bufs: Array[ByteBuffer]): NodeOp & Repeatable = {
    several(bufs).foreach(fillable)
    new ReadOp(handle, bufs)
  }

  def write(handle: WriteEnd, bufs: Array[ByteBuffer]): NodeOp & Repeatable = new WriteOp(handle, several(bufs))

  def read(handle: ReadEnd, pool: BufferPool, into: Slot[Received]): NodeOp & Repeatable = pool match {
    case ours: NodePool => new PooledReadOp(handle, ours, into)
    case other          => throw new IllegalArgumentException(s"Not a pool of this backend's: $other")
  }

  def close(handle: NodeHandle): NodeOp = new CloseOp(handle)

  // ---- Pipes

  def pipe(read: Slot[ReadEnd], write: Slot[WriteEnd]): NodeOp = new PipeOp(read, write)

  // ---- Stream sockets

  def accept(handle: ListenerHandle, into: Slot[ConnectionHandle]): NodeOp & Repeatable =
    new AcceptOp(handle, 1, into, accepted => into.set(accepted(0)))

  def acceptAll(handle: ListenerHandle, into: Slot[Array[ConnectionHandle]], max: Int): NodeOp & Repeatable = {
    if max <= 0 then throw new IllegalArgumentException(s"acceptAll takes at least one connection, not $max")
    new AcceptOp(handle, max, into, accepted => into.set(accepted))
  }

  def shutdownOutput(handle: ConnectionHandle): NodeOp = new ShutdownOutputOp(handle)

  def localAddress(handle: ListenerHandle | ConnectionHandle): IpSocketAddress | UnixAddress = {
    handle.checkOpen()
    handle match {
      case listener: ListenerHandle     => listener.local
      case connection: ConnectionHandle => connection.local
    }
  }

  def peerAddress(handle: ConnectionHandle): IpSocketAddress | UnixAddress = {
    handle.checkOpen()
    handle.peer
  }

  def connect(address: IpSocketAddress, options: SocketOptions, into: Slot[ConnectionHandle]): NodeOp = {
    Options.check(options, Kind.Connection)
    new ConnectOp(address, options, into)
  }

  def listen(address: IpSocketAddress, options: SocketOptions, into: Slot[ListenerHandle]): NodeOp = {
    Options.check(options, Kind.Listener)
    new ListenOp(address, options, into)
  }

  // ---- IP datagram sockets

  def bindDatagram(local: IpSocketAddress, options: SocketOptions, into: Slot[BoundDatagramHandle]): NodeOp = {
    Options.check(options, Kind.BoundDatagram)
    new OpenDatagramOp(local, null, options, into)
  }

  def connectDatagram(
      local: IpSocketAddress | Null,
      remote: IpSocketAddress,
      options: SocketOptions,
      into: Slot[ConnectedDatagramHandle]
  ): NodeOp = {
    val (from, to) = Addresses.pair(local, remote)
    Options.check(options, Kind.ConnectedDatagram)
    new OpenDatagramOp(from, to, options, into)
  }

  def receive(handle: BoundDatagramHandle, buf: ByteBuffer, from: Slot[IpSocketAddress]): NodeOp & Repeatable =
    new ReceiveOp(handle, fillable(buf), from)

  def receive(handle: ConnectedDatagramHandle, buf: ByteBuffer): NodeOp & Repeatable =
    new ReceiveOp(handle, fillable(buf), null)

  def send(handle: BoundDatagramHandle, buf: ByteBuffer, to: IpSocketAddress): NodeOp & Repeatable =
    new SendOp(handle, Array(buf), to)

  def send(handle: ConnectedDatagramHandle, buf: ByteBuffer): NodeOp & Repeatable = new SendOp(handle, Array(buf), null)

  def send(handle: BoundDatagramHandle, bufs: Array[ByteBuffer], to: IpSocketAddress): NodeOp & Repeatable =
    new SendOp(handle, several(bufs), to)

  def send(handle: ConnectedDatagramHandle, bufs: Array[ByteBuffer]): NodeOp & Repeatable =
    new SendOp(handle, several(bufs), null)

  @targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagramHandle | ConnectedDatagramHandle): IpSocketAddress = {
    handle.checkOpen()
    handle.local
  }

  @targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagramHandle): IpSocketAddress = {
    handle.checkOpen()
    handle.peer
  }

  // ---- Socket options

  def supports(option: SocketOption[?], kind: Kind): Boolean = Options.supports(option, kind)

  def canChange(option: SocketOption[?], kind: Kind): Boolean = Options.canChange(option, kind)

  /** The kind of socket `handle` names. */
  private def kindOf(handle: NodeHandle): Kind = handle match {
    case connection: ConnectionHandle => connection.kind
    case listener: ListenerHandle     => listener.kind
    case datagram: DatagramHandle     => datagram.kind
    case other                        => throw new IllegalArgumentException(s"$other is not a socket")
  }

  def option[V](handle: NodeHandle, option: SocketOption[V]): V = {
    val kind = kindOf(handle)
    if !Options.onOpen(option, kind) then
      throw new IllegalArgumentException(s"$option is not read on a ${kind.description} by this backend")
    handle.checkOpen()
    (handle match {
      case connection: ConnectionHandle => connection.option(option)
      case datagram: DatagramHandle     => datagram.option(option)
      case other                        => throw new IllegalArgumentException(s"$option is not read on $other")
    }).asInstanceOf[V]
  }

  def setOption[V](handle: NodeHandle, option: SocketOption[V], value: V): Unit = {
    val kind = kindOf(handle)
    if !Options.canChange(option, kind) then
      throw new IllegalArgumentException(s"$option is not changed on an open ${kind.description} by this backend")
    option.check(value)
    handle.checkOpen()
    handle match {
      case connection: ConnectionHandle => connection.setOption(option, value)
      case datagram: DatagramHandle     => datagram.setOption(option, value)
      case other                        => throw new IllegalArgumentException(s"$option is not changed on $other")
    }
  }
}
