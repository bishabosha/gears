package gearsexample

import java.nio.file.Files
import java.nio.file.Paths

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

  /** Removes a Unix socket file left behind, if `address` names one. */
  def removeFile(address: StreamAddress): Unit = address match {
    case UnixAddress(path) => Files.deleteIfExists(Paths.get(path)): Unit
    case _                 => ()
  }
}
