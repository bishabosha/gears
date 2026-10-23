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
import higherlevel.HigherLevel

/** Where a stream demo's server listens: an IP address, or a Unix path on a backend with Unix stream sockets. */
type StreamAddress = IpAddress | UnixAddress

/** The stream demos' sockets, for either family of address. */
object Sockets {
  /** A stream socket connected to `address`. */
  @annotation.targetName("connectUnix")
  def connect(r: Io[UnixStreams], address: StreamAddress)(using Async): Handle = address match {
    case unix: UnixAddress => HigherLevel.connect(r, unix)
    case ip: IpAddress     => HigherLevel.connect(r, ip)
  }

  /** A listener on `address`, listening. */
  @annotation.targetName("listenUnix")
  def listen(r: Io[UnixStreams], address: StreamAddress)(using Async): Handle = address match {
    case unix: UnixAddress => HigherLevel.listen(r, unix)
    case ip: IpAddress     => HigherLevel.listen(r, ip)
  }

  /** For code whose address is chosen when it runs, such as the bench's: on any backend for an IP address, and on one
    * with Unix stream sockets for a Unix one, refused otherwise.
    */
  def connect(r: Io[IpSockets], address: StreamAddress)(using Async): Handle = address match {
    case ip: IpAddress     => HigherLevel.connect(r, ip)
    case unix: UnixAddress =>
      val into = Handle.Slot()
      val sockets: UnixStreams & r.backend.type = unixSockets(r.backend, unix)
      r.perform(sockets.ops.connect(unix, into))
      into.clear()
  }

  def listen(r: Io[IpSockets], address: StreamAddress)(using Async): Handle = address match {
    case ip: IpAddress     => HigherLevel.listen(r, ip)
    case unix: UnixAddress =>
      val into = Handle.Slot()
      val sockets: UnixStreams & r.backend.type = unixSockets(r.backend, unix)
      r.perform(sockets.ops.listen(unix, into))
      into.clear()
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
