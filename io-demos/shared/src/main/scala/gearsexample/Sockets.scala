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
  /** A stream socket connected to `address`: on any backend for an IP address, and on one with Unix stream sockets
    * for a Unix one, refused otherwise.
    */
  def connect(address: StreamAddress)(using io: Io[IpSockets], async: Async): Handle = address match {
    case ip: IpAddress     => HigherLevel.connect(ip)
    case unix: UnixAddress => HigherLevel.connect(unix)(using unixSockets(io, unix))
  }

  /** A listener on `address`, listening, on the backends `connect` connects on. */
  def listen(address: StreamAddress)(using io: Io[IpSockets], async: Async): Handle = address match {
    case ip: IpAddress     => HigherLevel.listen(ip)
    case unix: UnixAddress => HigherLevel.listen(unix)(using unixSockets(io, unix))
  }

  /** Accepts connections on `listener` until the program ends, handing each to `serve` in a future of its own. A
    * failed accept leaves the server running: the connection it was for is gone, and running out of descriptors
    * (`EMFILE`) passes as connections close, so it waits a little and accepts again. One repeatable op serves every
    * connection, taking all that are pending each time (`acceptAll`), so that a burst does not wait an op apiece behind
    * the connections already being served while the listen queue fills.
    */
  def serve(listener: Handle)(serve: Handle => Async.Spawn ?=> Unit)(using
      io: Io[IpSockets],
      spawn: Async.Spawn
  ): Unit = {
    val accepted = gears.async.io.Slot[Array[Handle]]()
    val accept = io.backend.ops.acceptAll(listener, accepted, max = 64)
    var failing = false
    while true do {
      val clients =
        try {
          io.perform(accept)
          failing = false
          accepted.clear()
        } catch {
          case e: java.io.IOException =>
            if !failing then println(s"Accepting failed, and is tried again: ${e.getMessage}") // once per run of them
            failing = true
            io.sleep(10)
            Array.empty[Handle]
        }
      for client <- clients do Future(serve(client))
    }
  }

  /** `io` as an `Io` over Unix stream sockets, if its backend has them. */
  private def unixSockets(io: Io[IpSockets], address: UnixAddress): Io[UnixStreams] = io.backend match {
    case _: UnixStreams => io.asInstanceOf[Io[UnixStreams]] // its backend is one, and `Io` is covariant in it
    case b => throw new IllegalArgumentException(s"$b has no Unix domain sockets, so cannot use $address; use TCP")
  }

  /** Removes a Unix socket file left behind, if `address` names one. */
  def removeFile(address: StreamAddress): Unit = address match {
    case UnixAddress(path) => Files.deleteIfExists(Paths.get(path)): Unit
    case _                 => ()
  }
}
