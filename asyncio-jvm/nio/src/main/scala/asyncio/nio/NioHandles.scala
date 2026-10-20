package asyncio.nio

import java.io.IOException
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.StandardProtocolFamily
import java.net.StandardSocketOptions
import java.net.UnixDomainSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.FileChannel
import java.nio.channels.Pipe
import java.nio.channels.ReadableByteChannel
import java.nio.channels.SelectableChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicReferenceArray

import asyncio.IpAddress
import asyncio.ReadinessHandles
import asyncio.UnixAddress
import asyncio.UnixStreamHandles

/** What a handle names: an I/O object, and how to read and write it without waiting. `read` returns the bytes read, 0
  * if none are available, or -1 at end of stream; `write` returns the bytes written, or -1 if nothing fits.
  */
private[nio] trait Entry {
  def read(buf: ByteBuffer): Int
  def write(buf: ByteBuffer): Int
  def close(): Unit
}

/** A channel a `Selector` watches: a socket, a listener, a datagram socket, or an end of a pipe, in non-blocking mode.
  * A Unix path it bound is removed when it is closed.
  */
private[nio] final class Selectable(val channel: SelectableChannel, boundPath: Path | Null) extends Entry {
  def read(buf: ByteBuffer): Int = channel match {
    case readable: ReadableByteChannel => readable.read(buf)
    case _                             => throw new IOException(s"$channel cannot be read")
  }

  def write(buf: ByteBuffer): Int = channel match {
    case writable: WritableByteChannel =>
      val written = writable.write(buf)
      if written == 0 && buf.hasRemaining then -1 else written
    case _ => throw new IOException(s"$channel cannot be written")
  }

  // The reactors whose selectors this channel is registered with, which `close` wakes.
  private val registrations = new java.util.concurrent.ConcurrentLinkedQueue[NioReactor]()

  /** Records that `reactor` registered the channel with its selector. */
  def registered(reactor: NioReactor): Unit = registrations.add(reactor): Unit

  /** Closes the channel, and wakes the reactors it is registered with: a registered channel's descriptor is released
    * only once each selector has seen its key cancelled, which one blocked with nothing else to do would never get to,
    * so the other end of a pipe or socket would never see it close.
    */
  def close(): Unit =
    try {
      channel.close()
      registrations.forEach(_.wakeup())
    } finally if boundPath != null then Files.deleteIfExists(boundPath): Unit

  /** The channel, as the kind an op needs. */
  def as[C <: SelectableChannel](kind: Class[C], op: String): C =
    if kind.isInstance(channel) then kind.cast(channel)
    else throw new IOException(s"$op needs a ${kind.getSimpleName}, not $channel")
}

/** A regular file. Like POSIX, which reports a regular file always ready, it never waits: reads and writes go straight
  * to the file.
  */
private[nio] final class RegularFile(val channel: FileChannel) extends Entry {
  def read(buf: ByteBuffer): Int = channel.read(buf)
  def write(buf: ByteBuffer): Int = channel.write(buf)
  def close(): Unit = channel.close()
}

/** NIO's handles: a process-wide table of the channels its ops name, indexed by handle, so that, like descriptors, a
  * handle means the same on every reactor and thread. Numbers freed by `close` are reused.
  *
  * Sockets, listeners, datagram sockets and pipes are NIO's selectable channels in non-blocking mode, Unix stream
  * sockets among them. A regular file is a `FileChannel`, which never waits. The JDK has neither Unix datagram channels
  * nor a way to wait on a FIFO, so NIO has neither capability. Writing to a pipe or socket whose reader has gone fails
  * with an `IOException`: the JVM ignores `SIGPIPE`.
  */
/** Either kind of socket address. */
private[nio] type SocketAddress = IpAddress | UnixAddress

object NioHandles extends UnixStreamHandles with ReadinessHandles {

  /** Connections a listener queues before it accepts them, as `SOMAXCONN` on macOS. */
  private inline val Backlog = 128

  private val lock = new Object
  @volatile private var table = new AtomicReferenceArray[Entry | Null](64)
  private val free = new java.util.ArrayDeque[Integer]() // guarded by `lock`
  private var next = 0 // guarded by `lock`

  /** Gives `entry` a handle. */
  private[nio] def add(entry: Entry): Int = lock.synchronized {
    val handle = if free.isEmpty then { next += 1; next - 1 }
    else free.pop().intValue
    var current = table
    if handle >= current.length() then {
      val grown = new AtomicReferenceArray[Entry | Null](current.length() * 2)
      var i = 0
      while i < current.length() do {
        grown.set(i, current.get(i))
        i += 1
      }
      table = grown
      current = grown
    }
    current.set(handle, entry)
    handle
  }

  /** What `handle` names; throws if it is not open. */
  private[nio] def entry(handle: Int): Entry = {
    val current = table
    val found = if handle >= 0 && handle < current.length() then current.get(handle) else null
    if found == null then throw new IOException(s"Not an open handle: $handle")
    found
  }

  def openFile(path: String, write: Boolean): Int = {
    val file = Path.of(path)
    if !Files.exists(file) then throw new NoSuchFileException(path)
    if !Files.isRegularFile(file) then throw new IOException(s"Not a regular file: $path")
    add(RegularFile(FileChannel.open(file, if write then StandardOpenOption.WRITE else StandardOpenOption.READ)))
  }

  def connect(address: IpAddress): Int = connectStream(address)
  def connect(address: UnixAddress): Int = connectStream(address)

  private def connectStream(address: SocketAddress): Int = {
    val channel = SocketChannel.open(familyOf(address))
    selectable(channel, null) {
      channel.configureBlocking(false)
      // A Unix socket usually connects at once; `ops.connect` checks.
      try channel.connect(socketAddress(address))
      catch case e: java.net.ConnectException => throw NioHandles.connectFailed(e)
    }
  }

  def listen(address: IpAddress, reuseAddr: Boolean = true): Int = {
    val channel = ServerSocketChannel.open(familyOf(address))
    selectable(channel, null) {
      channel.configureBlocking(false)
      if reuseAddr then channel.setOption(StandardSocketOptions.SO_REUSEADDR, true)
      channel.bind(socketAddress(address), Backlog)
    }
  }

  def listen(address: UnixAddress): Int = {
    val channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    val path = Path.of(address.path)
    selectable(channel, path) {
      channel.configureBlocking(false)
      Files.deleteIfExists(path) // as the kqueue backend, which replaces a socket file already there
      channel.bind(socketAddress(address), Backlog)
    }
  }

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Int = {
    val either = if local != null then local else remote
    require(either != null, "A datagram socket needs a local or a remote address")
    require(
      local == null || remote == null || familyOf(local) == familyOf(remote),
      "Local and remote addresses must be of the same family"
    )
    val channel = DatagramChannel.open(familyOf(either))
    selectable(channel, null) {
      channel.configureBlocking(false)
      if local != null then channel.bind(socketAddress(local))
      if remote != null then channel.connect(socketAddress(remote)) // binds an ephemeral local port
    }
  }

  def pipe(): (Int, Int) = {
    val pipe = Pipe.open()
    val source = selectable(pipe.source(), null)(pipe.source().configureBlocking(false))
    val sink =
      try selectable(pipe.sink(), null)(pipe.sink().configureBlocking(false))
      catch {
        case t: Throwable =>
          close(source)
          throw t
      }
    (source, sink)
  }

  def readNow(handle: Int, buf: ByteBuffer): Int = entry(handle).read(buf)
  def writeNow(handle: Int, buf: ByteBuffer): Int = entry(handle).write(buf)

  def close(handle: Int): Unit = {
    val closing = lock.synchronized {
      val current = table
      val found = if handle >= 0 && handle < current.length() then current.get(handle) else null
      if found != null then {
        current.set(handle, null)
        free.push(handle)
      }
      found
    }
    if closing == null then throw new IOException(s"Not an open handle: $handle")
    closing.close()
  }

  /** Runs `setup` on `channel` and gives it a handle, closing it again if `setup` fails. */
  private def selectable(channel: SelectableChannel, boundPath: Path | Null)(setup: => Any): Int =
    try {
      setup
      add(Selectable(channel, boundPath))
    } catch {
      case t: Throwable =>
        channel.close()
        if boundPath != null then Files.deleteIfExists(boundPath)
        throw t
    }

  private def familyOf(address: SocketAddress): StandardProtocolFamily = address match {
    case UnixAddress(_)       => StandardProtocolFamily.UNIX
    case IpAddress.IPv4(_, _) => StandardProtocolFamily.INET
    case IpAddress.IPv6(_, _) => StandardProtocolFamily.INET6
  }

  /** The JDK's form of `address`. Hosts must be numeric, as `IpAddress` requires, so building one never resolves a
    * name.
    */
  private[nio] def socketAddress(address: SocketAddress): java.net.SocketAddress = address match {
    case UnixAddress(path)          => UnixDomainSocketAddress.of(path)
    case IpAddress.IPv4(host, p) => new InetSocketAddress(numeric(host, ipv6 = false), port(p))
    case IpAddress.IPv6(host, p) => new InetSocketAddress(numeric(host, ipv6 = true), port(p))
  }

  /** A numeric host as an address. `InetAddress.getByName` only checks the form of a literal, but would look up a
    * name, so anything that is not one is refused first, with the kqueue backend's message.
    */
  private def numeric(host: String, ipv6: Boolean): InetAddress = {
    def invalid = new IOException(s"Invalid numeric IP address: $host")
    val literal =
      if ipv6 then host.contains(':') else host.nonEmpty && host.forall(c => c == '.' || (c >= '0' && c <= '9'))
    if !literal then throw invalid
    try InetAddress.getByName(host)
    catch case _: java.net.UnknownHostException => throw invalid
  }

  /** Checks a port as the kqueue backend does, with its message. */
  private def port(port: Int): Int = {
    require(port >= 0 && port <= 65535, s"Port must be between 0 and 65535: $port")
    port
  }

  /** A failed connection, with the kqueue backend's message. */
  private[nio] def connectFailed(e: IOException): IOException =
    new IOException(s"Failed to connect socket: ${e.getMessage}", e)

  /** The `IpAddress` of a socket address the JDK reports for an IP socket. */
  private[nio] def ipAddress(socket: java.net.SocketAddress): IpAddress = socket match {
    case ip: InetSocketAddress =>
      ip.getAddress match {
        case v4: Inet4Address => IpAddress.IPv4(v4.getHostAddress, ip.getPort)
        case v6               =>
          val text = v6.getHostAddress
          val scope = text.indexOf('%')
          IpAddress.IPv6(if scope < 0 then text else text.substring(0, scope), ip.getPort)
      }
    case other => throw new IOException(s"Not an IP address: $other")
  }
}
