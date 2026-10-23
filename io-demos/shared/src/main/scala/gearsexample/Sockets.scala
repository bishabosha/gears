package gearsexample

import java.nio.file.Files
import java.nio.file.Paths

import gears.async.Async
import gears.async.Future

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

  /** Accepts connections on `listener` until the program ends, handing each to `serve` in a future of its own. A
    * failed accept leaves the server running: the connection it was for is gone, and running out of descriptors
    * (`EMFILE`) passes as connections close, so it waits a little and accepts again. One repeatable op serves every
    * connection, taking all that are pending each time (`acceptAll`), so that a burst does not wait an op apiece behind
    * the connections already being served while the listen queue fills.
    */
  def serve(r: Io[IpSockets], listener: Handle)(serve: Handle => Async.Spawn ?=> Unit)(using Async.Spawn): Unit = {
    val accepted = gears.async.io.Slot[Array[Handle]]()
    val accept = r.backend.ops.acceptAll(listener, accepted, max = 64)
    var failing = false
    while true do {
      val clients =
        try {
          r.perform(accept)
          failing = false
          accepted.clear()
        } catch {
          case e: java.io.IOException =>
            if !failing then println(s"Accepting failed, and is tried again: ${e.getMessage}") // once per run of them
            failing = true
            r.sleep(10)
            Array.empty[Handle]
        }
      for client <- clients do Future(serve(client))
    }
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
