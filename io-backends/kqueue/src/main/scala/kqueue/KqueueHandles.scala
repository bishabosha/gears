package gears.async.io
package kqueue

import gears.async.io.IpAddress
import gears.async.io.UnixAddress
import gears.async.io.internal.DescriptorTable
import gears.async.io.internal.Descriptors
import gears.async.io.internal.NonBlocking
import gears.async.io.internal.PosixErr.cError
import gears.async.io.internal.PosixFileOps
import gears.async.io.internal.PosixSockets
import gears.async.io.internal.Sockets.Flavor
import gears.async.io.internal.Sockets.Transport

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
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

import KqueueBackend.Handle

/** Either kind of socket address, as the system calls take them. */
private[kqueue] type SocketAddress = IpAddress | UnixAddress

/** Opening, using and closing kqueue's handles directly, for kqueue-specific code such as its demos, which open them
  * outside any reactor; the interface opens them with ops (`KqueueOps`), which call the openers here.
  *
  * Kqueue's handles are POSIX file descriptors, opened non-blocking (see `Descriptors`). Descriptor numbers are
  * process-wide, so the Unix paths bound by `listen` and `datagram` are remembered per descriptor and removed when it
  * is closed. Writing to a handle whose reader has gone fails with `EPIPE` rather than raising `SIGPIPE`, which would
  * end the process: sockets get `SO_NOSIGPIPE`, and on macOS pipes and FIFOs opened for writing get `F_SETNOSIGPIPE`.
  */
object KqueueHandles {

  /** What is kept about an open socket, as its descriptor's note in `DescriptorTable`: the Unix path it bound, to
    * remove when it closes.
    */
  private final class SocketNote {
    @volatile var boundPath: String | Null = null
    @volatile var boundFile: PosixFileOps.FileId | Null = null // the socket file it bound there
    @volatile var ipv6 = false // an IPv6 socket, dual stack, which reaches an IPv4 peer by its IPv4-mapped address
  }

  /** `fd`'s note, made if it has none. Only the thread opening or binding a socket sets one. */
  private def noteOf(fd: Int): SocketNote = DescriptorTable.note(fd) match {
    case note: SocketNote => note
    case _                =>
      val note = new SocketNote
      DescriptorTable.setNote(fd, note)
      note
  }

  /** The handle of descriptor `fd`: for kqueue-specific code, whose handles are descriptors; the interface's handles
    * come from the openers below.
    */
  def handle(fd: Int): Handle = new Handle(fd)

  /** The handle of a connected stream socket's descriptor `fd`, as a connection, for kqueue-specific code that accepts
    * or connects one itself.
    */
  def connection(fd: Int): KqueueKinds.Connection = KqueueKinds.connection(handle(fd))

  /** The descriptor `handle` is, for kqueue-specific code. */
  def descriptor(handle: Handle): Int = handle.fd

  private def wrap(fd: Int): Handle = handle(fd)
  private[kqueue] def fd(handle: Handle): Int = descriptor(handle)

  /** Opens a regular file, blocking for the disk: the `open` op's body, run on the blocker pool, and for code with no
    * reactor. Its descriptor is marked in `DescriptorTable`, so that the reactor never takes a short read of it to mean
    * it is drained: kqueue reports a regular file readable only while its offset is short of the end, so a read waiting
    * for that would never see the end.
    */
  def openFile(path: String, write: Boolean): KqueueKinds.File = {
    val fd = Descriptors.openFile(path, write)
    DescriptorTable.markAlwaysTry(fd)
    KqueueKinds.file(wrap(fd))
  }

  /** Opens a FIFO. Its descriptor is marked as a regular file's is: on macOS, a FIFO whose writer closed before the
    * reactor's read filter was added never reports the end either.
    */
  def openFifoForReading(path: String): KqueueKinds.Readable = KqueueKinds.readable(wrap(fifoDescriptor(path, false)))
  def openFifoForWriting(path: String): KqueueKinds.Writable = KqueueKinds.writable(wrap(fifoDescriptor(path, true)))

  private[kqueue] def fifoDescriptor(path: String, write: Boolean): Int = {
    val fd = Descriptors.openFifo(path, write)
    DescriptorTable.markAlwaysTry(fd)
    fd
  }

  def connect(address: IpAddress): KqueueKinds.Connection = KqueueKinds.connection(wrap(connectStream(address)))
  def connect(address: UnixAddress): KqueueKinds.Connection = KqueueKinds.connection(wrap(connectStream(address)))

  private[kqueue] def connectStream(address: SocketAddress): Int =
    socketFor(address, Transport.Stream)(fd => connectTo(fd, address))

  /** A listening socket, bound and listening at once. */
  def listen(address: IpAddress, reuseAddr: Boolean = true): KqueueKinds.Listener =
    KqueueKinds.listener(wrap(listenStream(address, reuseAddr)))
  def listen(address: UnixAddress): KqueueKinds.Listener = KqueueKinds.listener(wrap(listenStream(address, false)))

  private[kqueue] def listenStream(address: SocketAddress, reuseAddr: Boolean): Int =
    socketFor(address, Transport.Stream) { fd =>
      if reuseAddr then PosixSockets.setReuseAddress(fd)
      bindTo(fd, address)
      PosixSockets.listen(fd, PosixSockets.maxConnections)
    }

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): KqueueKinds.Datagram = {
    require(
      local == null || remote == null || flavorOf(local) == flavorOf(remote),
      "Local and remote addresses must be of the same family"
    )
    KqueueKinds.datagram(wrap(datagramSocket(local, remote)))
  }

  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null): KqueueKinds.UnixDatagram =
    KqueueKinds.unixDatagram(wrap(datagramSocket(local, remote)))

  private[kqueue] def datagramSocket(local: SocketAddress | Null, remote: SocketAddress | Null): Int = {
    val either = if local != null then local else remote
    require(either != null, "A datagram socket needs a local or a remote address")
    socketFor(either, Transport.Datagram) { fd =>
      if local != null then bindTo(fd, local)
      if remote != null then connectTo(fd, remote) // assigns an IP socket an ephemeral local port
    }
  }

  def pipe(): (KqueueKinds.Readable, KqueueKinds.Writable) = {
    val (in, out) = pipeDescriptors()
    (KqueueKinds.readable(wrap(in)), KqueueKinds.writable(wrap(out)))
  }

  private[kqueue] def pipeDescriptors(): (Int, Int) = Descriptors.pipe()

  def readNow(handle: Handle, buf: ByteBuffer): Int = using(handle) {
    if !buf.hasRemaining() then throw gears.async.io.Ops.bufferFull(buf) // reading nothing would say end of stream
    NonBlocking.read(handle.fd, buf)
  }
  def writeNow(handle: Handle, buf: ByteBuffer): Int =
    using(handle)(if buf.hasRemaining() then NonBlocking.write(handle.fd, buf) else 0)

  /** Closes `handle`, from any thread, as `closeHandle` does, waiting, if a use of its descriptor is in progress, for
    * the last use to close it, and throwing what that close threw. Closing the descriptor ends its generation in
    * `DescriptorTable` first, while the number is still its, and tells every open reactor afterwards, with the
    * generation that ended, so that the commands pending on it fail and its slots are forgotten before the number is
    * reused (see `Ops.close` and `closeDescriptor`).
    */
  def close(handle: Handle): Unit = {
    val closed = new java.util.concurrent.CountDownLatch(1)
    var failure: Throwable | Null = null
    val here = closeHandle(handle, f => { failure = f; closed.countDown() })
    if !here then {
      closed.await() // for the last use of the descriptor to end, and close it
      if failure != null then throw failure.nn
    }
  }

  /** Closes `handle` the way `KqueueBackend.Handle` says: at once in meaning, a Unix path it bound removed at once too,
    * so that a listener bound to the path right after is not removed later; the descriptor now if no use of it is in
    * progress, returning true, and otherwise once the last ends, returning false, the descriptor then closed on that
    * use's thread, which calls `later` with the close's failure, or null. Throws if the handle was already closed, or
    * if closing the descriptor here fails.
    */
  private[kqueue] def closeHandle(handle: Handle, later: (Throwable | Null) => Unit): Boolean = {
    val fd = handle.fd
    val finish: Runnable = () =>
      later(
        try {
          closeDescriptor(fd)
          null
        } catch case t: Throwable => t
      )
    handle.close(finish) match {
      case KqueueBackend.Handle.AlreadyClosed => throw notOpen(handle)
      case KqueueBackend.Handle.Idle          =>
        closeDescriptor(fd)
        true
      case _ =>
        removeBoundPath(fd) // now, not when the descriptor closes
        false
    }
  }

  /** Runs `body` as a use of `handle`'s descriptor (see `KqueueBackend.Handle`); throws if the handle is closed. */
  private[kqueue] inline def using[A](handle: Handle)(inline body: A): A = {
    if !handle.acquire() then throw notOpen(handle)
    try body
    finally handle.release()
  }

  private[kqueue] def notOpen(handle: Handle): IOException = new IOException(s"Not an open handle: $handle")

  /** `close` on a descriptor number, for the backend's own code. */
  private[kqueue] def closeDescriptor(handle: Int): Unit = {
    removeBoundPath(handle) // before closing, while the number is still ours
    gears.async.io.internal.FifoWatcher.forget(handle) // so the number is not watched once reused
    val generation = DescriptorTable.generation(DescriptorTable.get(handle))
    DescriptorTable.closing(handle)
    PosixSockets.close(handle)
    KqueueReactor.descriptorClosed(handle, generation)
  }

  /** Removes the socket file the socket `fd` bound at a Unix path, if any, and if it is still that file: a listener
    * that took the path since replaced it with its own, which must stay. Forgotten as it is taken.
    */
  private def removeBoundPath(fd: Int): Unit = {
    DescriptorTable.note(fd) match {
      case note: SocketNote if note.boundPath != null && note.boundFile != null =>
        val path = note.boundPath.nn
        Zone.acquire { implicit z =>
          PosixFileOps.unlinkIfStill(toCString(path, StandardCharsets.UTF_8), note.boundFile.nn)
        }
      case _ => ()
    }
    DescriptorTable.setNote(fd, null)
  }

  private def flavorOf(address: SocketAddress): Flavor = address match {
    case UnixAddress(_)       => Flavor.Unix
    case IpAddress.IPv4(_, _) => Flavor.IPv4
    case IpAddress.IPv6(_, _) => Flavor.IPv6
  }

  /** Opens a non-blocking socket and runs `setup` on it, closing it again if `setup` fails. */
  private def socketFor(address: SocketAddress, transport: Transport)(setup: Int => Unit): Int = {
    val flavor = flavorOf(address)
    val fd = PosixSockets.open(flavor, transport)
    try {
      PosixSockets.setNonBlocking(fd)
      if flavor == Flavor.IPv6 then {
        PosixSockets.setDualStack(fd)
        noteOf(fd).ipv6 = true
      }
      setup(fd)
      fd
    } catch {
      case t: Throwable =>
        closeDescriptor(fd)
        throw t
    }
  }

  private def bindTo(fd: Int, address: SocketAddress): Unit = address match {
    case UnixAddress(path) =>
      val bound = PosixSockets.bindUnixAddr(fd, path, removeExisting = true)
      val note = noteOf(fd)
      note.boundFile = bound
      note.boundPath = path
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

  /** `to` as the IPv6 socket `fd` reaches it: an IPv4 address as its IPv4-mapped one, since the socket is dual stack
    * and cannot take an IPv4 socket address; otherwise `to` itself.
    */
  private[kqueue] def targetFor(fd: Int, to: SocketAddress): SocketAddress = to match {
    case IpAddress.IPv4(host, port) if (DescriptorTable.note(fd) match {
          case note: SocketNote => note.ipv6
          case _                => false
        }) =>
      IpAddress.IPv6(s"::ffff:$host", port)
    case other => other
  }

  /** The peer `fd` is connected to, or null if it is not connected. */
  private[kqueue] def peerOf(fd: Int): SocketAddress | Null = {
    val storage = stackalloc[socket.sockaddr_storage]()
    val length = stackalloc[socket.socklen_t]()
    !length = sizeOf[socket.sockaddr_storage].toUInt
    val address = storage.asInstanceOf[Ptr[socket.sockaddr]]
    if socket.getpeername(fd, address, length) == 0 then decode(address, !length)
    else if scala.scalanative.posix.errno.errno == scala.scalanative.posix.errno.ENOTCONN then null
    else throw new IOException(s"Failed to find the peer of descriptor $fd: ${cError()}")
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
        // An IPv4 peer of a dual-stack socket comes IPv4-mapped: reported as the IPv4 address it is, as the JDK does.
        gears.async.io.IpText.unmapped(IpAddress.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt))
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}
