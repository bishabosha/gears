package asyncio.nio

import java.io.IOException
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.StandardProtocolFamily
import java.net.StandardSocketOptions
import java.net.UnixDomainSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.DatagramChannel
import java.nio.channels.FileChannel
import java.nio.channels.Pipe
import java.nio.channels.ReadableByteChannel
import java.nio.channels.SelectableChannel
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import asyncio.Handle
import asyncio.IpAddress
import asyncio.PipeHandles
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
  // For a listener from `listener`: where it is to listen, until its `listening` op binds it.
  @volatile var toListen: (SocketAddress, Boolean) | Null = null

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

  // The channel's own selectors, one for each direction, on which callers off the pool wait (`SelectorWait`),
  // taken from the pool on first use; the ops waiting on them; and whether the channel has been closed, after which none is taken, and the
  // selectors go back to the pool once no op waits on them. Guarded by `this`.
  private var reading: Selector | Null = null
  private var writing: Selector | Null = null
  private var waiters = 0
  private var closed = false

  /** The channel's own selector for writing (connecting, too) or for reading (accepting, too), with the channel
    * registered on it, for an op to wait on until it calls `exitWait`. Throws once the handle is closed.
    */
  def enterWait(write: Boolean): Selector = synchronized {
    if closed then throw new ClosedChannelException()
    val existing = if write then writing else reading
    val selector =
      if existing != null then existing
      else {
        val taken = SelectorPool.take()
        try channel.register(taken, 0)
        catch {
          case t: Throwable =>
            SelectorPool.give(taken)
            throw t
        }
        if write then writing = taken else reading = taken
        taken
      }
    waiters += 1
    selector
  }

  /** An op has stopped waiting; the last one to leave a closed handle gives its selectors back to the pool. */
  def exitWait(): Unit = {
    val released = synchronized {
      waiters -= 1
      if closed && waiters == 0 then releaseSelectors() else Nil
    }
    released.foreach(SelectorPool.give)
  }

  /** Takes the selectors off this handle, for the pool. Holding `this`. */
  private def releaseSelectors(): List[Selector] = {
    val all = List(reading, writing).collect { case s: Selector => s }
    reading = null
    writing = null
    all
  }

  /** Closes the channel, and tells the reactors it is registered with, which wakes them: a registered channel's
    * descriptor is released only once each selector has seen its key cancelled, which one blocked with nothing else to
    * do would never get to, so the other end of a pipe or socket would never see it close; and the commands pending on
    * it there fail, as `Handles.close` says. An op waiting on one of its own selectors is woken, to find the channel
    * closed, and the selectors go back to the pool once none waits.
    */
  def close(): Unit = {
    val (waking, released) = synchronized {
      closed = true
      val waking = List(reading, writing).collect { case s: Selector => s }
      (waking, if waiters == 0 then releaseSelectors() else Nil)
    }
    try {
      channel.close()
      registrations.forEach(_.closed(this))
    } finally {
      try {
        waking.foreach(_.wakeup())
        released.foreach(SelectorPool.give)
      } finally if boundPath != null then Files.deleteIfExists(boundPath): Unit
    }
  }

  /** The channel, as the kind an op needs. */
  def as[C <: SelectableChannel](kind: Class[C], op: String): C =
    if kind.isInstance(channel) then kind.cast(channel)
    else throw new IOException(s"$op needs a ${kind.getSimpleName}, not $channel")
}

/** Selectors for `Selectable.selector` to reuse: on macOS, opening one makes a kqueue and a wake-up pipe, and closing
  * it closes them, which a connection's ops would otherwise pay for on every connection.
  */
private[nio] object SelectorPool {

  /** Selectors kept at most, beyond which a returned one is closed. */
  private inline val Kept = 256

  private val idle = new java.util.concurrent.ConcurrentLinkedQueue[Selector]()
  private val count = new java.util.concurrent.atomic.AtomicInteger()

  def take(): Selector = {
    val kept = idle.poll()
    if kept == null then Selector.open()
    else {
      count.decrementAndGet()
      kept
    }
  }

  /** Takes back a selector no op waits on, whose channel has been closed. A `selectNow` drops the cancelled key, which
    * also lets the JDK release the channel's descriptor, so the other end sees it close at once, and clears a wake-up
    * left from a cancel or the close.
    */
  def give(selector: Selector): Unit = {
    try {
      selector.selectNow(): Unit
      if count.incrementAndGet() <= Kept then idle.add(selector): Unit
      else {
        count.decrementAndGet()
        selector.close()
      }
    } catch case _: java.io.IOException | _: java.nio.channels.ClosedSelectorException => selector.close()
  }
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

object NioHandles extends UnixStreamHandles with ReadinessHandles with PipeHandles {

  /** Connections a listener queues before it accepts them, as `SOMAXCONN` on macOS. */
  private inline val Backlog = 128

  private val table = new Handle.Table[Entry]

  /** Gives `entry` a handle. */
  private[nio] def add(entry: Entry): Handle = table.add(entry)

  /** What `handle` names; throws if it is not open. */
  private[nio] def entry(handle: Handle): Entry = table(handle)

  /** Opens a regular file, blocking for the disk: the `open` op's body, run on the blocker pool, and for code with no
    * reactor.
    */
  def openFile(path: String, write: Boolean): Handle = {
    val file = Path.of(path)
    if !Files.exists(file) then throw new NoSuchFileException(path)
    if !Files.isRegularFile(file) then throw new IOException(s"Not a regular file: $path")
    add(RegularFile(FileChannel.open(file, if write then StandardOpenOption.WRITE else StandardOpenOption.READ)))
  }

  def connect(address: IpAddress): Handle = connectStream(address)
  def connect(address: UnixAddress): Handle = connectStream(address)

  private def connectStream(address: SocketAddress): Handle = {
    val channel = SocketChannel.open(familyOf(address))
    selectable(channel, null) {
      channel.configureBlocking(false)
      // A Unix socket usually connects at once; `ops.connect` checks.
      try channel.connect(socketAddress(address))
      catch case e: java.net.ConnectException => throw NioHandles.connectFailed(e)
    }
  }

  def listener(address: IpAddress, reuseAddr: Boolean = true): Handle = {
    val channel = ServerSocketChannel.open(familyOf(address))
    val handle = selectable(channel, null)(channel.configureBlocking(false))
    entry(handle).asInstanceOf[Selectable].toListen = (address, reuseAddr)
    handle
  }

  def listener(address: UnixAddress): Handle = {
    val channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    val handle = selectable(channel, Path.of(address.path))(channel.configureBlocking(false))
    entry(handle).asInstanceOf[Selectable].toListen = (address, false)
    handle
  }

  /** Binds a listener from `listener`, for its `listening` op, or does nothing for a datagram socket, bound when it was
    * made; throws if it is neither, or the bind fails.
    */
  private[nio] def bind(handle: Handle): Unit = {
    entry(handle) match {
      case s: Selectable if s.channel.isInstanceOf[DatagramChannel] => ()
      case s: Selectable =>
        val pending = s.toListen
        if pending == null then throw new IOException(s"Handle $handle is not a listener waiting to listen")
        s.toListen = null
        val (address, reuseAddr) = pending
        val channel = s.as(classOf[ServerSocketChannel], "Listening")
        if reuseAddr then channel.setOption(StandardSocketOptions.SO_REUSEADDR, true)
        address match {
          case UnixAddress(path) => Files.deleteIfExists(Path.of(path)) // as kqueue, replacing a socket file there
          case _                 => ()
        }
        channel.bind(socketAddress(address), Backlog)
      case other => throw new IOException(s"Listening needs a socket, not $other")
    }
  }

  /** A listening socket, bound and listening at once: for code with no reactor to perform `listening` on; the
    * interface's way is `listener` and that op.
    */
  def listen(address: IpAddress, reuseAddr: Boolean = true): Handle = {
    val channel = ServerSocketChannel.open(familyOf(address))
    selectable(channel, null) {
      channel.configureBlocking(false)
      if reuseAddr then channel.setOption(StandardSocketOptions.SO_REUSEADDR, true)
      channel.bind(socketAddress(address), Backlog)
    }
  }

  def listen(address: UnixAddress): Handle = {
    val channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    val path = Path.of(address.path)
    selectable(channel, path) {
      channel.configureBlocking(false)
      Files.deleteIfExists(path) // as the kqueue backend, which replaces a socket file already there
      channel.bind(socketAddress(address), Backlog)
    }
  }

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Handle = {
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

  def pipe(): (Handle, Handle) = {
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

  def readNow(handle: Handle, buf: ByteBuffer): Int = entry(handle).read(buf)
  def writeNow(handle: Handle, buf: ByteBuffer): Int = entry(handle).write(buf)

  def close(handle: Handle): Unit = table.remove(handle).close()

  /** Runs `setup` on `channel` and gives it a handle, closing it again if `setup` fails. */
  private def selectable(channel: SelectableChannel, boundPath: Path | Null)(setup: => Any): Handle =
    try {
      setup
      add(Selectable(channel, boundPath))
    } catch {
      case t: Throwable =>
        channel.close()
        if boundPath != null then Files.deleteIfExists(boundPath)
        throw t
    }

  private[nio] def familyOf(address: SocketAddress): StandardProtocolFamily = address match {
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
  private[nio] def port(port: Int): Int = {
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
