package gears.async.io
package node

import java.io.IOException
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.Uint8Array

/** A stream of Node's, a `net.Socket`, as the backend reads and writes it: a source and a sink.
  *
  * It is read as Node's paused streams are: Node takes bytes from the system up to its high-water mark and keeps them
  * until a read asks, so a peer that sends to a program that does not read is held back as the system would hold it. It
  * is written one chunk at a time, of at most `ChunkSize` bytes, the next not taken until Node has handed the last to
  * the system: Node itself takes whatever it is given, without bound, and a write it has taken cannot be taken back, so
  * a write here waits before Node has anything of it, and one cancelled or out of time has written nothing. What Node
  * holds, then, is one chunk at most, as a socket's send buffer a little larger.
  */
private[node] final class SocketStream(socket: js.Dynamic, readable: Boolean, writable: Boolean)
    extends Source
    with Sink {
  private var endSeen = false
  private var error: IOException | Null = null
  private var closed = false
  private var unsent = 0
  private var shutDown = !writable

  private val onReadable: js.Function0[Unit] = () => {
    // With nothing to read, this is the end coming: a read that finds none has Node say so, with `end`.
    if socket.readableLength.asInstanceOf[Int] == 0 then socket.read(): Unit
    wakeReader()
  }
  private val onEnd: js.Function0[Unit] = () => {
    endSeen = true
    wakeReader()
  }
  private val onError: js.Function1[js.Dynamic, Unit] = failure => {
    if error == null then error = Errors.io(failure)
    wakeReader()
    wakeWriter()
  }
  private val onClose: js.Function0[Unit] = () => {
    closed = true
    wakeReader()
    wakeWriter()
  }

  socket.on("error", onError)
  socket.on("close", onClose)
  if readable then {
    socket.on("end", onEnd)
    socket.on("readable", onReadable)
  }

  def available: Int = if readable && !closed then socket.readableLength.asInstanceOf[Int] else 0

  def take(max: Int): Int8Array = {
    val chunk = socket.read(math.min(max, available))
    if chunk == null then throw new IOException("Node had no bytes of those it said were ready")
    // A read of exactly what was left does not have Node say that the end, if it has come already, is next: only a
    // read that finds nothing does, so one is made, which takes nothing.
    if socket.readableLength.asInstanceOf[Int] == 0 then socket.read(0): Unit
    Buffers.signed(chunk.asInstanceOf[Uint8Array])
  }

  def ended: Boolean = endSeen || closed

  def failure: IOException | Null = error

  def room: Int = if unsent > 0 then 0 else SocketStream.ChunkSize

  def put(chunk: Int8Array): Unit = {
    val size = chunk.length
    unsent += size
    val sent: js.Function1[js.Dynamic, Unit] = failure => {
      unsent -= size
      if failure != null && !js.isUndefined(failure) && error == null then error = Errors.io(failure)
      wakeWriter()
    }
    try socket.write(new Uint8Array(chunk.buffer, chunk.byteOffset, size), sent): Unit
    catch
      case js.JavaScriptException(failure) => // refused outright: its callback will not come
        unsent -= size
        if error == null then error = Errors.io(failure)
        throw error.nn
  }

  def refusal: IOException | Null =
    if shutDown then new IOException("The stream is shut down for writing")
    else if error != null then error
    else if closed then new IOException("Broken pipe: the stream has closed")
    else null

  /** Ends the stream for writing: its end goes after what was written. */
  def shutdown(): Unit =
    if !shutDown then {
      shutDown = true
      if !closed then socket.end(): Unit
    }

  /** Closes the stream. What Node still holds of the bytes written is sent first, by Node, after this returns, as the
    * system sends what a closed socket's buffer holds.
    */
  def close(): Unit =
    if !closed then
      if unsent > 0 && error == null then socket.destroySoon(): Unit
      else socket.destroy(): Unit
}

private[node] object SocketStream {

  /** The most a write hands Node at once. */
  inline val ChunkSize = 65536
}

/** A connected stream socket, IP or Unix, with its addresses as they were when it connected: Node forgets them once the
  * socket has ended.
  */
final class ConnectionHandle private[node] (
    socket: js.Dynamic,
    private[node] val unix: Boolean,
    private[node] val local: IpSocketAddress | UnixAddress,
    private[node] val peer: IpSocketAddress | UnixAddress,
    private var noDelay: Boolean
) extends NodeHandle(if unix then "Unix connection" else "connection")
    with SourceEnd
    with SinkEnd {
  private val stream = new SocketStream(socket, readable = true, writable = true)
  if !unix then socket.setNoDelay(noDelay)

  private[node] def source: Source = stream
  private[node] def sink: Sink = stream

  private[node] def shutdownOutput(): Unit = stream.shutdown()

  private[node] def kind: SocketOption.Kind =
    if unix then SocketOption.Kind.UnixConnection else SocketOption.Kind.Connection

  private[node] def option(option: SocketOption[?]): Any = option match {
    case SocketOption.NoDelay => noDelay
    case other                => throw new IllegalArgumentException(s"This backend has no socket option $other")
  }

  private[node] def setOption(option: SocketOption[?], value: Any): Unit = option match {
    case SocketOption.NoDelay =>
      Errors.guard(socket.setNoDelay(value.asInstanceOf[Boolean]))
      noDelay = value.asInstanceOf[Boolean]
    case other => throw new IllegalArgumentException(s"This backend has no socket option $other")
  }

  protected def shut(done: () => Unit): Unit = {
    stream.close()
    done()
  }
}

/** A listening stream socket, IP or Unix. Node accepts the connections that come as they come, so those no accept has
  * taken yet wait here, as they would in the system's queue, at most `Backlog` of them, the rest refused; the accepts
  * waiting take them in the order they came.
  */
final class ListenerHandle private[node] (
    server: js.Dynamic,
    private[node] val unix: Boolean,
    private[node] val local: IpSocketAddress | UnixAddress,
    options: SocketOptions
) extends NodeHandle(if unix then "Unix listener" else "listener") {
  private val arrived = scala.collection.mutable.ArrayDeque.empty[ConnectionHandle]
  private val accepts = scala.collection.mutable.ArrayDeque.empty[ListenerHandle.Accept]
  private val noDelay = options.get(SocketOption.NoDelay) != false

  private var shutDown = false

  private val onConnection: js.Function1[js.Dynamic, Unit] = socket =>
    if shutDown || arrived.length >= ListenerHandle.Backlog then socket.destroy(): Unit
    else
      try {
        val connection =
          if unix then new ConnectionHandle(socket, true, local, UnixAddress(""), noDelay = false)
          else
            new ConnectionHandle(
              socket,
              false,
              Addresses.reported(socket.localAddress, socket.localPort),
              Addresses.reported(socket.remoteAddress, socket.remotePort),
              noDelay
            )
        arrived.append(connection)
        serve()
      } catch
        case _: Exception => // gone before it was accepted: Node no longer has its addresses
          socket.on("error", ListenerHandle.ignore)
          socket.destroy(): Unit

  private val onError: js.Function1[js.Dynamic, Unit] = failure => {
    val failed = Errors.io(failure)
    for accept <- accepts.toList do accept.run.fail(failed)
  }

  server.on("connection", onConnection)
  server.on("error", onError)

  private[node] def kind: SocketOption.Kind =
    if unix then SocketOption.Kind.UnixListener else SocketOption.Kind.Listener

  /** Gives the accepts waiting the connections that have arrived, in order. */
  private def serve(): Unit =
    while accepts.nonEmpty && arrived.nonEmpty do {
      val accept = accepts.head
      val taken = new Array[ConnectionHandle](math.min(accept.max, arrived.length))
      for i <- taken.indices do taken(i) = arrived.removeHead()
      try {
        accept.deliver(taken)
        accept.run.succeed() // which takes it out of the queue
      } catch
        case failure: Throwable => // its slot filled while it waited: the connections stay for the next accept
          arrived.prependAll(taken)
          accept.run.fail(failure)
    }

  /** Accepts at least one connection and at most `max` for `run`, behind the accepts already waiting. */
  private[node] def accept(run: Run, max: Int, deliver: Array[ConnectionHandle] => Unit): Unit = {
    val accept = new ListenerHandle.Accept(run, max, deliver)
    accepts.append(accept)
    run.release = () => accepts.removeFirst(_ eq accept): Unit
    serve()
  }

  /** What closing does to the listener's path, for a Unix one: set by its opener. */
  private[node] var closing: (() => Unit) => Unit = close => close()

  protected def shut(done: () => Unit): Unit = {
    shutDown = true // a connection Node hands over after this is closed as it comes
    for connection <- arrived do connection.shutNow()
    arrived.clear()
    closing(() => server.close(): Unit)
    done()
  }
}

private[node] object ListenerHandle {
  inline val Backlog = 511

  final class Accept(val run: Run, val max: Int, val deliver: Array[ConnectionHandle] => Unit)

  val ignore: js.Function1[js.Dynamic, Unit] = _ => ()
}

/** Opens a stream socket connected to an address. */
private[node] final class ConnectOp(
    address: IpSocketAddress | UnixAddress,
    options: SocketOptions,
    into: Slot[ConnectionHandle]
) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(into)

  protected def start(run: Run): Unit = {
    val to = address match {
      case IpSocketAddress(ip, port) =>
        js.Dynamic.literal(host = Addresses.host(ip), port = port, allowHalfOpen = true)
      case UnixAddress(path) => js.Dynamic.literal(path = path, allowHalfOpen = true)
    }
    val socket = Errors.guard(Net.connect(to))
    var connected = false
    val onError: js.Function1[js.Dynamic, Unit] = failure => run.fail(Errors.io(failure))
    lazy val onConnect: js.Function0[Unit] = () =>
      try {
        connected = true
        socket.removeListener("error", onError)
        into.set(address match {
          case unix: UnixAddress  => new ConnectionHandle(socket, true, UnixAddress(""), unix, noDelay = false)
          case _: IpSocketAddress =>
            new ConnectionHandle(
              socket,
              false,
              Addresses.reported(socket.localAddress, socket.localPort),
              Addresses.reported(socket.remoteAddress, socket.remotePort),
              options.get(SocketOption.NoDelay) != false
            )
        })
        run.succeed()
      } catch
        case failure: Throwable =>
          connected = false
          run.fail(failure)
    socket.once("error", onError)
    socket.once("connect", onConnect)
    run.release = () =>
      if !connected then { // failed, cancelled or out of time: nothing is left open
        socket.removeListener("connect", onConnect)
        socket.on("error", ListenerHandle.ignore)
        socket.destroy(): Unit
      }
  }
}

/** Opens a stream socket listening on an address: an IP one, or a Unix path, whose socket file it replaces if one is
  * there, and which it removes as it closes if the file there is still its own.
  */
private[node] final class ListenOp(
    address: IpSocketAddress | UnixAddress,
    options: SocketOptions,
    into: Slot[ListenerHandle]
) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(into)

  protected def start(run: Run): Unit = {
    val on = address match {
      case IpSocketAddress(ip, port) =>
        js.Dynamic.literal(host = Addresses.host(ip), port = port, backlog = ListenerHandle.Backlog)
      case UnixAddress(path) =>
        UnixPaths.clear(path)
        js.Dynamic.literal(path = path, backlog = ListenerHandle.Backlog)
    }
    val server = Errors.guard(Net.createServer(js.Dynamic.literal(allowHalfOpen = true)))
    var listening = false
    val onError: js.Function1[js.Dynamic, Unit] = failure => run.fail(Errors.io(failure))
    lazy val onListening: js.Function0[Unit] = () =>
      try {
        val listener = address match {
          case unix: UnixAddress =>
            val listener = new ListenerHandle(server, true, unix, options)
            val own = UnixPaths.identity(unix.path)
            listener.closing = close => UnixPaths.closing(unix.path, own)(close)
            listener
          case _: IpSocketAddress =>
            val bound = server.address()
            new ListenerHandle(server, false, Addresses.reported(bound.address, bound.port), options)
        }
        server.removeListener("error", onError)
        into.set(listener)
        listening = true
        run.succeed()
      } catch case failure: Throwable => run.fail(failure)
    server.once("error", onError)
    server.once("listening", onListening)
    run.release = () =>
      if !listening then {
        server.removeListener("listening", onListening)
        server.on("error", ListenerHandle.ignore)
        server.close(): Unit
      }
    Errors.guard(server.listen(on)): Unit
  }
}

/** The socket files of Unix listeners. */
private[node] object UnixPaths {
  private val asBigInts = js.Dynamic.literal(throwIfNoEntry = false, bigint = true)
  private var renames = 0L

  /** What tells the file at `path` from any other, its device and inode, or null if there is none. */
  def identity(path: String): String | Null =
    try
      Fs.lstatSync(path, asBigInts).toOption match {
        case Some(stat) => s"${stat.dev}:${stat.ino}"
        case None       => null
      }
    catch case _: js.JavaScriptException => null

  /** Makes way for a listener at `path`: removes a socket file there, a server's that ran before or runs still, and
    * fails for any other kind of file, which is left.
    */
  def clear(path: String): Unit = Errors.guard {
    Fs.lstatSync(path, asBigInts).toOption match {
      case Some(stat) =>
        if !stat.isSocket().asInstanceOf[Boolean] then throw new IOException(s"$path exists and is not a socket")
        Fs.unlinkSync(path)
      case None => ()
    }
  }

  /** Closes a listener that bound `path` as the file `own`, with `close`. Node removes the path as it closes the
    * listener, whatever is there, and at once, within the call; so if the file there is no longer the listener's own,
    * but one a later listener bound in its place, it is moved aside for the close and moved back after, within this
    * turn of the event loop.
    */
  def closing(path: String, own: String | Null)(close: () => Unit): Unit = {
    val there = identity(path)
    if there == null || there == own then close()
    else {
      renames += 1
      val aside = s"$path.$renames.kept"
      val moved =
        try {
          Fs.renameSync(path, aside)
          true
        } catch case _: js.JavaScriptException => false
      try close()
      finally
        if moved then
          try Fs.renameSync(aside, path)
          catch case _: js.JavaScriptException => ()
    }
  }
}

/** Accepts connections on a listener, one or several. */
private[node] final class AcceptOp(
    on: ListenerHandle,
    max: Int,
    slot: Slot[?],
    deliver: Array[ConnectionHandle] => Unit
) extends NodeOp
    with Repeatable {
  override protected def slots: List[Slot[?]] = List(slot)
  override protected def handle: NodeHandle = on
  protected def start(run: Run): Unit = on.accept(run, max, deliver)
}

/** Shuts a connection down for writing. */
private[node] final class ShutdownOutputOp(on: ConnectionHandle) extends NodeOp {
  override protected def handle: NodeHandle = on
  protected def start(run: Run): Unit = {
    on.shutdownOutput()
    run.succeed()
  }
}
