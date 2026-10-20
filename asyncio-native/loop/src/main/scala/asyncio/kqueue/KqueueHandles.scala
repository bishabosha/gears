package asyncio.kqueue

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.sys.statOps.*
import scala.scalanative.posix.sys.un
import scala.scalanative.posix.sys.unOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.FifoHandles
import asyncio.IpAddress
import asyncio.ReadinessHandles
import asyncio.UnixAddress
import asyncio.UnixDatagramHandles
import asyncio.UnixStreamHandles
import asyncio.unsafe.DescriptorTable
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixFileOps
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport

/** Kqueue's handles are POSIX file descriptors, opened non-blocking. Descriptor numbers are process-wide, so the Unix
  * paths bound by `listen` and `datagram` are remembered per descriptor and removed when it is closed. Writing to a
  * handle whose reader has gone fails with `EPIPE` rather than raising `SIGPIPE`, which would end the process: sockets
  * get `SO_NOSIGPIPE`, and on macOS pipes and FIFOs opened for writing get `F_SETNOSIGPIPE`.
  */
/** Either kind of socket address, as the system calls take them. */
private[kqueue] type SocketAddress = IpAddress | UnixAddress

object KqueueHandles
    extends UnixStreamHandles
    with UnixDatagramHandles
    with FifoHandles
    with ReadinessHandles {
  private val boundPaths = new ConcurrentHashMap[Integer, String]()

  /** Opens a regular file. Its descriptor is marked in `DescriptorTable`, so that the reactor never takes a short read
    * of it to mean it is drained: kqueue reports a regular file readable only while its offset is short of the end, so
    * a read waiting for that would never see the end.
    */
  def openFile(path: String, write: Boolean): Int = {
    val fd = openNonBlocking(path, write)
    try if !isKind(fd, stat.S_ISREG) then throw new IOException(s"Not a regular file: $path")
    catch {
      case e: IOException =>
        PosixSockets.close(fd)
        throw e
    }
    DescriptorTable.markAlwaysTry(fd)
    fd
  }

  /** Opens a FIFO. Its descriptor is marked as a regular file's is: on macOS, a FIFO whose writer closed before the
    * reactor's read filter was added never reports the end either.
    */
  def openFifo(path: String, write: Boolean): Int = {
    val fd = openNonBlocking(path, write)
    try {
      if !isKind(fd, stat.S_ISFIFO) then throw new IOException(s"Not a FIFO: $path")
      if write then noSigPipe(fd)
    } catch {
      case e: IOException =>
        PosixSockets.close(fd)
        throw e
    }
    DescriptorTable.markAlwaysTry(fd)
    fd
  }

  private def openNonBlocking(path: String, write: Boolean): Int = {
    val fd = Zone.acquire { implicit z =>
      fcntl.open(toCString(path), (if write then fcntl.O_WRONLY else fcntl.O_RDONLY) | fcntl.O_NONBLOCK)
    }
    if fd < 0 then throw new IOException(s"Failed to open $path: ${cError()}")
    fd
  }

  private def isKind(fd: Int, kind: stat.mode_t => CInt): Boolean = {
    val info = stackalloc[stat.stat]()
    if stat.fstat(fd, info) < 0 then throw new IOException(s"Failed to inspect file: ${cError()}")
    kind(info.st_mode) != 0
  }

  /** macOS's `fcntl` command for a pipe or FIFO's write end that makes a write after the reader has closed fail with
    * `EPIPE` instead of raising `SIGPIPE`. posixlib does not define it.
    */
  private inline val F_SETNOSIGPIPE = 73

  /** Stops writes to `fd`, a pipe or FIFO's write end, from raising `SIGPIPE`. On macOS only: FreeBSD has no
    * per-descriptor equivalent, so there such a write still raises the signal unless the application ignores it.
    */
  private def noSigPipe(fd: Int): Unit =
    if LinktimeInfo.isMac then // a link-time condition, kept apart from the runtime one
      if fcntl.fcntl(fd, F_SETNOSIGPIPE, 1) < 0 then throw new IOException(s"Failed to set F_SETNOSIGPIPE: ${cError()}")

  def connect(address: IpAddress): Int = connectStream(address)
  def connect(address: UnixAddress): Int = connectStream(address)

  private def connectStream(address: SocketAddress): Int =
    socketFor(address, Transport.Stream)(fd => connectTo(fd, address))

  def listen(address: IpAddress, reuseAddr: Boolean = true): Int = listenStream(address, reuseAddr)
  def listen(address: UnixAddress): Int = listenStream(address, reuseAddr = false)

  private def listenStream(address: SocketAddress, reuseAddr: Boolean): Int =
    socketFor(address, Transport.Stream) { fd =>
      if reuseAddr then PosixSockets.setReuseAddress(fd)
      bindTo(fd, address)
      PosixSockets.listen(fd, PosixSockets.maxConnections)
    }

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Int = {
    require(
      local == null || remote == null || flavorOf(local) == flavorOf(remote),
      "Local and remote addresses must be of the same family"
    )
    datagramSocket(local, remote)
  }

  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null): Int = datagramSocket(local, remote)

  private def datagramSocket(local: SocketAddress | Null, remote: SocketAddress | Null): Int = {
    val either = if local != null then local else remote
    require(either != null, "A datagram socket needs a local or a remote address")
    socketFor(either, Transport.Datagram) { fd =>
      if local != null then bindTo(fd, local)
      if remote != null then connectTo(fd, remote) // assigns an IP socket an ephemeral local port
    }
  }

  def pipe(): (Int, Int) = {
    val ends = stackalloc[CInt](2)
    if unistd.pipe(ends) < 0 then throw new IOException(s"Failed to create pipe: ${cError()}")
    try {
      PosixSockets.setNonBlocking(ends(0))
      PosixSockets.setNonBlocking(ends(1))
      noSigPipe(ends(1))
    } catch {
      case e: IOException =>
        PosixSockets.close(ends(0))
        PosixSockets.close(ends(1))
        throw e
    }
    (ends(0), ends(1))
  }

  def readNow(handle: Int, buf: ByteBuffer): Int = NonBlocking.read(handle, buf)
  def writeNow(handle: Int, buf: ByteBuffer): Int = NonBlocking.write(handle, buf)

  def close(handle: Int): Unit = {
    val path = boundPaths.remove(handle) // before closing, while the number is still ours
    PosixSockets.close(handle)
    if path != null then Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }
  }

  private def flavorOf(address: SocketAddress): Flavor = address match {
    case UnixAddress(_)       => Flavor.Unix
    case IpAddress.IPv4(_, _) => Flavor.IPv4
    case IpAddress.IPv6(_, _) => Flavor.IPv6
  }

  /** Opens a non-blocking socket and runs `setup` on it, closing it again if `setup` fails. */
  private def socketFor(address: SocketAddress, transport: Transport)(setup: Int => Unit): Int = {
    val fd = PosixSockets.open(flavorOf(address), transport)
    try {
      PosixSockets.setNonBlocking(fd)
      setup(fd)
      fd
    } catch {
      case t: Throwable =>
        close(fd)
        throw t
    }
  }

  private def bindTo(fd: Int, address: SocketAddress): Unit = address match {
    case UnixAddress(path) =>
      PosixSockets.bindUnixAddr(fd, path, removeExisting = true)
      boundPaths.put(fd, path)
    case IpAddress.IPv4(host, port) => PosixSockets.bindIPv4Addr(fd, host, port)
    case IpAddress.IPv6(host, port) => PosixSockets.bindIPv6Addr(fd, host, port)
  }

  private def connectTo(fd: Int, address: SocketAddress): Unit = address match {
    case UnixAddress(path)          => PosixSockets.connectUnixAddr(fd, path)
    case IpAddress.IPv4(host, port) => PosixSockets.connectIPv4Addr(fd, host, port)
    case IpAddress.IPv6(host, port) => PosixSockets.connectIPv6Addr(fd, host, port)
  }

  /** Builds the socket address for `address` and passes it to `use`, valid only during the call. */
  private[kqueue] def withSocketAddr[A](address: SocketAddress)(use: (Ptr[socket.sockaddr], socket.socklen_t) => A): A =
    address match {
      case UnixAddress(path)          => PosixSockets.withUnixSocketAddr(path)(use)
      case IpAddress.IPv4(host, port) => PosixSockets.withIPv4SocketAddr(host, port)(use)
      case IpAddress.IPv6(host, port) => PosixSockets.withIPv6SocketAddr(host, port)(use)
    }

  /** Decodes a socket address filled in by the kernel. An unnamed Unix socket has the empty path. */
  private[kqueue] def decode(addr: Ptr[socket.sockaddr], length: socket.socklen_t): SocketAddress = {
    val family = addr.sa_family.toInt
    if family == socket.AF_UNIX then {
      val unix = addr.asInstanceOf[Ptr[un.sockaddr_un]]
      val headerLength = sizeOf[un.sockaddr_un] - sizeOf[CArray[CChar, un._108]]
      if length.toInt <= headerLength.toInt then UnixAddress("")
      else UnixAddress(fromCString(unix.sun_path.at(0), StandardCharsets.UTF_8))
    } else {
      val text = stackalloc[Byte](64)
      if family == socket.AF_INET then {
        val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
        inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        IpAddress.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
      } else if family == socket.AF_INET6 then {
        val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
        inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        IpAddress.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt)
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}
