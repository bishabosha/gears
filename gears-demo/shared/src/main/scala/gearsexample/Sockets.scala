package gearsexample

import java.nio.file.Files
import java.nio.file.Paths

import gears.async.Async

import asyncio.gears.Io
import asyncio.Handle
import asyncio.IpAddress
import asyncio.SocketBackend
import asyncio.UnixAddress
import asyncio.UnixStreamBackend
import asyncio.gears.ReactorAsync

/** Where a stream demo's server listens: an IP address, or a Unix path on a backend with Unix stream sockets. */
type StreamAddress = IpAddress | UnixAddress

/** The stream demos' sockets, for either family of address. */
object Sockets {
  def connect(b: UnixStreamBackend, address: StreamAddress): Handle = address match {
    case unix: UnixAddress => b.handles.connect(unix)
    case ip: IpAddress     => b.handles.connect(ip)
  }

  /** A listener on `address`, listening: the handle, and its `listening` op performed. */
  def listen(b: Io & UnixStreamBackend, address: StreamAddress)(using Async): Handle = {
    val listener = address match {
      case unix: UnixAddress => b.handles.listener(unix)
      case ip: IpAddress     => b.handles.listener(ip)
    }
    b.perform(b.ops.listening(listener))
    listener
  }

  /** For code whose address is chosen when it runs, such as the bench's: on any backend for an IP address, and on one
    * with Unix stream sockets for a Unix one, refused otherwise.
    */
  def connect(b: SocketBackend, address: StreamAddress): Handle = address match {
    case ip: IpAddress     => b.handles.connect(ip)
    case unix: UnixAddress => connect(unixSockets(b, unix), unix)
  }

  def listen(b: Io & SocketBackend, address: StreamAddress)(using Async): Handle = address match {
    case ip: IpAddress =>
      val listener = b.handles.listener(ip)
      b.perform(b.ops.listening(listener))
      listener
    case unix: UnixAddress => listen(unixSockets(b, unix), unix)
  }

  private def unixSockets(b: SocketBackend, address: UnixAddress): UnixStreamBackend & b.type = b match {
    case unix: (UnixStreamBackend & b.type) => unix
    case _ => throw new IllegalArgumentException(s"$b has no Unix domain sockets, so cannot use $address; use TCP")
  }

  /** Removes a Unix socket file left behind, if `address` names one. */
  def removeFile(address: StreamAddress): Unit = address match {
    case UnixAddress(path) => Files.deleteIfExists(Paths.get(path)): Unit
    case _                 => ()
  }
}
