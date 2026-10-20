package gearsexample

import java.nio.file.Files
import java.nio.file.Paths

import asyncio.Backend
import asyncio.IpAddress
import asyncio.UnixAddress
import asyncio.UnixStreamBackend

/** Where a stream demo's server listens: an IP address, or a Unix path on a backend with Unix stream sockets. */
type StreamAddress = IpAddress | UnixAddress

/** The stream demos' sockets, for either family of address. */
object Sockets {
  def connect(b: UnixStreamBackend, address: StreamAddress): Int = address match {
    case unix: UnixAddress => b.handles.connect(unix)
    case ip: IpAddress     => b.handles.connect(ip)
  }

  def listen(b: UnixStreamBackend, address: StreamAddress): Int = address match {
    case unix: UnixAddress => b.handles.listen(unix)
    case ip: IpAddress     => b.handles.listen(ip)
  }

  /** For code whose address is chosen when it runs, such as the bench's: on any backend for an IP address, and on one
    * with Unix stream sockets for a Unix one, refused otherwise.
    */
  def connect(b: Backend, address: StreamAddress): Int = address match {
    case ip: IpAddress => b.handles.connect(ip)
    case unix: UnixAddress => connect(unixSockets(b, unix), unix)
  }

  def listen(b: Backend, address: StreamAddress): Int = address match {
    case ip: IpAddress => b.handles.listen(ip)
    case unix: UnixAddress => listen(unixSockets(b, unix), unix)
  }

  private def unixSockets(b: Backend, address: UnixAddress): UnixStreamBackend = b match {
    case unix: UnixStreamBackend => unix
    case _ => throw new IllegalArgumentException(s"$b has no Unix domain sockets, so cannot use $address; use TCP")
  }

  /** Removes a Unix socket file left behind, if `address` names one. */
  def removeFile(address: StreamAddress): Unit = address match {
    case UnixAddress(path) => Files.deleteIfExists(Paths.get(path)): Unit
    case _                 => ()
  }
}
