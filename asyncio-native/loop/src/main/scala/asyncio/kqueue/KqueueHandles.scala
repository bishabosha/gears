package asyncio.kqueue

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
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
import asyncio.unsafe.Descriptors
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixFileOps
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport

/** Kqueue's handles are POSIX file descriptors, opened non-blocking (see `Descriptors`). Descriptor numbers are
  * process-wide, so the Unix paths bound by `listen` and `datagram` are remembered per descriptor and removed when it
  * is closed. Writing to a handle whose reader has gone fails with `EPIPE` rather than raising `SIGPIPE`, which would
  * end the process: sockets get `SO_NOSIGPIPE`, and on macOS pipes and FIFOs opened for writing get `F_SETNOSIGPIPE`.
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
    val fd = Descriptors.openFile(path, write)
    DescriptorTable.markAlwaysTry(fd)
    fd
  }

  /** Opens a FIFO. Its descriptor is marked as a regular file's is: on macOS, a FIFO whose writer closed before the
    * reactor's read filter was added never reports the end either.
    */
  def openFifo(path: String, write: Boolean): Int = {
    val fd = Descriptors.openFifo(path, write)
    DescriptorTable.markAlwaysTry(fd)
    fd
  }

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

  def pipe(): (Int, Int) = Descriptors.pipe()

  def readNow(handle: Int, buf: ByteBuffer): Int = NonBlocking.read(handle, buf)
  def writeNow(handle: Int, buf: ByteBuffer): Int = NonBlocking.write(handle, buf)

  /** Closes a descriptor, from any thread: its generation in `DescriptorTable` ends first, while the number is still
    * its, and every open reactor is told afterwards, with the generation that ended, so that the commands pending on
    * it fail and its slots are forgotten before the number is reused (see `Handles.close`). A handle must not be used
    * while it is being closed.
    */
  def close(handle: Int): Unit = {
    val path = boundPaths.remove(handle) // before closing, while the number is still ours
    val generation = DescriptorTable.generation(DescriptorTable.get(handle))
    DescriptorTable.closing(handle)
    PosixSockets.close(handle)
    KqueueReactor.descriptorClosed(handle, generation)
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
