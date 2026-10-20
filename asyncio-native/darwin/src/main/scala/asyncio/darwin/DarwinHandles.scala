package asyncio.darwin

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReferenceArray
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.FifoHandles
import asyncio.IpAddress
import asyncio.unsafe.Descriptors

/** An open I/O object of the Darwin backend: a Network.framework connection, listener or datagram socket, or a
  * descriptor, each with its C side (`DarwinIo`); a regular file has only its descriptor, since its reads and writes
  * never wait. The handle number is its index in `DarwinHandles`' table.
  */
private[darwin] final class DarwinHandle(val kind: Int, val ptr: DarwinIo.Handle, val fd: Int, val remote: IpAddress | Null) {
  def isFile: Boolean = kind == DarwinHandle.File
  def isDescriptor: Boolean = kind == DarwinHandle.Descriptor
}

private[darwin] object DarwinHandle {
  inline val Stream = 1
  inline val Listener = 2
  inline val Datagram = 3
  inline val BoundDatagram = 4
  inline val Descriptor = 5 // a pipe or FIFO, waited on through a Dispatch source
  inline val File = 6
}

/** The Darwin backend's handles: indexes into a table of its I/O objects, since Network.framework's are not
  * descriptors. IP sockets are Network.framework's; pipes, FIFOs and regular files are descriptors, which
  * Network.framework has nothing for. Unix domain sockets it has no public endpoint for, so there are none.
  */
object DarwinHandles extends FifoHandles {

  // The table: a handle number is an index, reused once closed. Grown under the lock, and published through the
  // volatile field so that any thread reads the current table.
  @volatile private var table = new AtomicReferenceArray[DarwinHandle | Null](256)
  private val free = new java.util.ArrayDeque[Integer]()
  private var next = 0

  private def add(handle: DarwinHandle): Int = synchronized {
    val index =
      if !free.isEmpty then free.poll().intValue
      else {
        if next == table.length then {
          val grown = new AtomicReferenceArray[DarwinHandle | Null](table.length * 2)
          for i <- 0 until table.length do grown.set(i, table.get(i))
          table = grown
        }
        next += 1
        next - 1
      }
    table.set(index, handle)
    index
  }

  /** The handle's object, or an `IOException` if it is not open. */
  private[darwin] def get(handle: Int): DarwinHandle = {
    val current = table
    val found = if handle >= 0 && handle < current.length then current.get(handle) else null
    if found == null then throw new IOException(s"Handle $handle is not open")
    found
  }

  /** Registers an accepted connection, as its own handle. */
  private[darwin] def accepted(ptr: DarwinIo.Handle): Int = add(new DarwinHandle(DarwinHandle.Stream, ptr, -1, null))

  def openFile(path: String, write: Boolean): Int =
    add(new DarwinHandle(DarwinHandle.File, null, Descriptors.openFile(path, write), null))

  def openFifo(path: String, write: Boolean): Int = descriptor(Descriptors.openFifo(path, write))

  def pipe(): (Int, Int) = {
    val (in, out) = Descriptors.pipe()
    (descriptor(in), descriptor(out))
  }

  private def descriptor(fd: Int): Int =
    add(new DarwinHandle(DarwinHandle.Descriptor, DarwinIo.dn_fd_open(fd), fd, null))

  private def hostAndPort(address: IpAddress): (String, Int) = address match {
    case IpAddress.IPv4(host, port) => (host, port)
    case IpAddress.IPv6(host, port) => (host, port)
  }

  def connect(address: IpAddress): Int = {
    val (host, port) = hostAndPort(address)
    val ptr = Zone.acquire { implicit z => DarwinIo.dn_connect(toCString(host), port) }
    add(new DarwinHandle(DarwinHandle.Stream, ptr, -1, address))
  }

  def listen(address: IpAddress, reuseAddr: Boolean = true): Int = {
    val (host, port) = hostAndPort(address)
    val error = stackalloc[CInt]()
    val ptr = Zone.acquire { implicit z => DarwinIo.dn_listen(toCString(host), port, if reuseAddr then 1 else 0, error) }
    if ptr == null then throw new IOException(s"Failed to listen on $host:$port: ${errorText(!error)}")
    add(new DarwinHandle(DarwinHandle.Listener, ptr, -1, null))
  }

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Int = {
    require(
      local == null || remote == null || local.getClass == remote.getClass,
      "Local and remote addresses must be of the same family"
    )
    require(local != null || remote != null, "A datagram socket needs a local or a remote address")
    if remote != null then {
      val (host, port) = hostAndPort(remote)
      val ptr = Zone.acquire { implicit z =>
        if local != null then {
          val (localHost, localPort) = hostAndPort(local)
          DarwinIo.dn_datagram_connect(toCString(host), port, toCString(localHost), localPort)
        } else DarwinIo.dn_datagram_connect(toCString(host), port, null, 0)
      }
      add(new DarwinHandle(DarwinHandle.Datagram, ptr, -1, remote))
    } else {
      val (host, port) = hostAndPort(local.nn)
      val error = stackalloc[CInt]()
      val ptr = Zone.acquire { implicit z => DarwinIo.dn_datagram_bind(toCString(host), port, error) }
      if ptr == null then throw new IOException(s"Failed to bind $host:$port: ${errorText(!error)}")
      add(new DarwinHandle(DarwinHandle.BoundDatagram, ptr, -1, null))
    }
  }

  /** Closes a handle, from any thread: the ops pending on it, on whichever reactor, fail there, told by its C side
    * (see `Handles.close`); a regular file's descriptor is simply closed. The number is free for reuse at once.
    */
  def close(handle: Int): Unit = {
    val found = synchronized {
      val current = table
      val found = if handle >= 0 && handle < current.length then current.get(handle) else null
      if found != null then {
        current.set(handle, null)
        free.add(handle)
      }
      found
    }
    if found == null then throw new IOException(s"Handle $handle is not open")
    if found.ptr != null then DarwinIo.dn_close(found.ptr)
    else if unistd.close(found.fd) < 0 then throw new IOException(s"Failed to close: ${errorText(scala.scalanative.posix.errno.errno)}")
  }

  private[darwin] def errorText(errno: Int): String = fromCString(string.strerror(errno))

  /** Decodes an IP socket address filled in by the C side. */
  private[darwin] def decode(addr: Ptr[socket.sockaddr_storage], length: socket.socklen_t): IpAddress = {
    val family = addr.asInstanceOf[Ptr[socket.sockaddr]].sa_family.toInt
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
