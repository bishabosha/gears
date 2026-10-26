package gearsexample

import gears.async.Async
import gears.async.Future
import gears.async.io.Io
import gears.async.io.IpAddress
import gears.async.io.IpSockets
import gears.async.io.UnixAddress
import gears.async.io.UnixStreams

import java.nio.file.Files
import java.nio.file.Paths

import higherlevel.HigherLevel

/** Where a stream demo's server listens: an IP address, or a Unix path on a backend with Unix stream sockets. */
type StreamAddress = IpAddress | UnixAddress

/** The stream demos' sockets, for either family of address. */
object Sockets {

  /** A stream socket connected to `address`, of either family, on a backend with Unix stream sockets. */
  def connect(address: StreamAddress)(using io: Io[UnixStreams], async: Async): io.Connection = address match {
    case unix: UnixAddress => HigherLevel.connect(unix)
    case ip: IpAddress     => HigherLevel.connect(ip)
  }

  /** A listener on `address`, of either family, listening, on a backend with Unix stream sockets. */
  def listen(address: StreamAddress)(using io: Io[UnixStreams], async: Async): io.Listener = address match {
    case unix: UnixAddress => HigherLevel.listen(unix)
    case ip: IpAddress     => HigherLevel.listen(ip)
  }

  /** Accepts connections on `listener` until the program ends, handing each to `serve` in a future of its own. A failed
    * accept leaves the server running: the connection it was for is gone, and running out of descriptors (`EMFILE`)
    * passes as connections close, so it waits a little and accepts again. One repeatable op serves every connection,
    * taking all that are pending each time (`acceptAll`), so that a burst does not wait an op apiece behind the
    * connections already being served while the listen queue fills.
    */
  def serve(using
      io: Io[IpSockets]
  )(listener: io.Listener)(serve: io.Connection => Async.Spawn ?=> Unit)(using
      Async.Spawn
  ): Unit = {
    val accepted = gears.async.io.Slot[Array[io.Connection]]()
    val accept = io.backend.ops.acceptAll(listener, accepted, max = 64)
    var failing = false
    while true do {
      val clients: Array[io.Connection] | Null =
        try {
          io.perform(accept)
          failing = false
          accepted.clear()
        } catch {
          case e: java.io.IOException =>
            if !failing then println(s"Accepting failed, and is tried again: ${e.getMessage}") // once per run of them
            failing = true
            io.sleep(10)
            null
        }
      if clients != null then for client <- clients do Future(serve(client))
    }
  }

  /** Removes a Unix socket file left behind, if `address` names one. */
  def removeFile(address: StreamAddress): Unit = address match {
    case UnixAddress(path) => Files.deleteIfExists(Paths.get(path)): Unit
    case _                 => ()
  }
}

/** A stream address with the capability its sockets need, `B`: `IpSockets` for an IP address, `UnixStreams` for a Unix
  * path, so that a program over a backend without Unix stream sockets cannot be given a Unix path. For code whose
  * address is chosen when it runs, such as the bench's.
  */
sealed trait Endpoint[-B <: IpSockets] {
  def address: StreamAddress

  /** A stream socket connected to `address`. */
  def connect()(using io: Io[B], async: Async): io.Connection

  /** A listener on `address`, listening. */
  def listen()(using io: Io[B], async: Async): io.Listener
}

object Endpoint {
  final case class Ip(address: IpAddress) extends Endpoint[IpSockets] {
    def connect()(using io: Io[IpSockets], async: Async): io.Connection = HigherLevel.connect(address)
    def listen()(using io: Io[IpSockets], async: Async): io.Listener = HigherLevel.listen(address)
  }

  final case class Unix(address: UnixAddress) extends Endpoint[UnixStreams] {
    def connect()(using io: Io[UnixStreams], async: Async): io.Connection = HigherLevel.connect(address)
    def listen()(using io: Io[UnixStreams], async: Async): io.Listener = HigherLevel.listen(address)
  }
}
