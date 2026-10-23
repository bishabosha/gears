package gearsexample

import java.nio.file.Files
import java.nio.file.Paths

import gears.async.Async

import gears.async.io.Io
import gears.async.io.Handle
import gears.async.io.IpAddress
import gears.async.io.IpSockets
import gears.async.io.UnixAddress
import gears.async.io.UnixStreams

/** Where a stream demo's server listens: an IP address, or a Unix path on a backend with Unix stream sockets. */
type StreamAddress = IpAddress | UnixAddress

/** The stream demos' sockets, for either family of address. */
object Sockets {
  def connect(b: UnixStreams, address: StreamAddress): Handle = address match {
    case unix: UnixAddress => b.handles.connect(unix)
    case ip: IpAddress     => b.handles.connect(ip)
  }

  /** A listener on `address`, listening: the handle, and its `listening` op performed. */
  @annotation.targetName("listenUnix")
  def listen(r: Io[UnixStreams], address: StreamAddress)(using Async): Handle = {
    val listener = address match {
      case unix: UnixAddress => r.backend.handles.listener(unix)
      case ip: IpAddress     => r.backend.handles.listener(ip)
    }
    r.perform(r.backend.ops.listening(listener))
    listener
  }

  /** For code whose address is chosen when it runs, such as the bench's: on any backend for an IP address, and on one
    * with Unix stream sockets for a Unix one, refused otherwise.
    */
  def connect(b: IpSockets, address: StreamAddress): Handle = address match {
    case ip: IpAddress     => b.handles.connect(ip)
    case unix: UnixAddress => connect(unixSockets(b, unix), unix)
  }

  def listen(r: Io[IpSockets], address: StreamAddress)(using Async): Handle = {
    val listener = address match {
      case ip: IpAddress     => r.backend.handles.listener(ip)
      case unix: UnixAddress => unixSockets(r.backend, unix).handles.listener(unix)
    }
    r.perform(r.backend.ops.listening(listener))
    listener
  }

  private def unixSockets(b: IpSockets, address: UnixAddress): UnixStreams & b.type = b match {
    case unix: (UnixStreams & b.type) => unix
    case _ => throw new IllegalArgumentException(s"$b has no Unix domain sockets, so cannot use $address; use TCP")
  }

  /** Removes a Unix socket file left behind, if `address` names one. */
  def removeFile(address: StreamAddress): Unit = address match {
    case UnixAddress(path) => Files.deleteIfExists(Paths.get(path)): Unit
    case _                 => ()
  }
}
