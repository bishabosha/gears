package example

import gears.async.io.IpAddress
import gears.async.io.IpSocketAddress
import gears.async.io.Protocol
import gears.async.io.internal.Bracket
import gears.async.io.internal.Bracket.FileOperation
import gears.async.io.internal.PosixFileOps
import gears.async.io.internal.PosixSockets
import gears.async.io.internal.Sockets.Flavor
import gears.async.io.internal.Sockets.Transport
import gears.async.io.kqueue.KqueueHandles

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.*

enum KQueueExampleAddress {
  case Unix(sock: String)
  case IPv4(host: String, port: Int)
  case IPv6(host: String, port: Int)

  def flavor: Flavor = this match {
    case Unix(_)    => Flavor.Unix
    case IPv4(_, _) => Flavor.IPv4
    case IPv6(_, _) => Flavor.IPv6
  }

  def bind(fd: Int): Unit = this match {
    case Unix(sock)       => PosixSockets.bindUnixAddr(fd, sock, removeExisting = true)
    case IPv4(host, port) => PosixSockets.bindIp(fd, KQueueExampleAddress.ip(host, port, Protocol.IPv4))
    case IPv6(host, port) => PosixSockets.bindIp(fd, KQueueExampleAddress.ip(host, port, Protocol.IPv6))
  }

  def connect(fd: Int): Unit = this match {
    case Unix(sock)       => PosixSockets.connectUnixAddr(fd, sock)
    case IPv4(host, port) => PosixSockets.connectIp(fd, KQueueExampleAddress.ip(host, port, Protocol.IPv4))
    case IPv6(host, port) => PosixSockets.connectIp(fd, KQueueExampleAddress.ip(host, port, Protocol.IPv6))
  }

  def withSocket(transport: Transport)(use: FileOperation): Unit = {
    open(transport, unlinkOnClose = false)(use(_))
  }

  /** Binds the address; a bound Unix path is removed again when the socket closes. */
  def withBoundSocket(transport: Transport)(use: FileOperation): Unit = {
    open(transport, unlinkOnClose = true) { fd =>
      // A listening IP socket may need its port while connections closed just before a restart are in TIME_WAIT.
      if transport == Transport.Stream && flavor != Flavor.Unix then PosixSockets.setReuseAddress(fd)
      bind(fd)
      println(s"Bound socket $fd to $this")
      use(fd)
    }
  }

  private inline def open(transport: Transport, unlinkOnClose: Boolean)(inline use: Int => Unit): Unit = {
    Bracket.fileResource(PosixSockets.open(flavor, transport))(close(_, unlinkOnClose))(fd =>
      PosixSockets.setNonBlocking(fd)
      use(fd)
    )
  }

  private def close(fd: Int, unlinkOnClose: Boolean): Unit = {
    KqueueHandles.close(KqueueHandles.handle(fd))
    this match
      case Unix(sock) if unlinkOnClose =>
        Zone.acquire { implicit z =>
          PosixFileOps.safeUnlink(toCString(sock, StandardCharsets.UTF_8))
        }
      case _ => ()
  }
}

object KQueueExampleAddress {

  /** The numeric `host` given on the command line, of `protocol`, as an address: refused if it is not one, never looked
    * up as a name.
    */
  def ip(host: String, port: Int, protocol: Protocol): IpSocketAddress = {
    def invalid = new IllegalArgumentException(s"Not a numeric $protocol address: $host")
    val literal = protocol match {
      case Protocol.IPv4 => host.nonEmpty && host.forall(c => c == '.' || c.isDigit)
      case Protocol.IPv6 => host.contains(':')
    }
    if !literal then throw invalid
    val bytes =
      try java.net.InetAddress.getByName(host).getAddress // a literal, so parsed, not looked up
      catch case _: java.net.UnknownHostException => throw invalid
    val expected = if protocol == Protocol.IPv4 then 4 else 16
    if bytes.length != expected then throw invalid
    IpSocketAddress(IpAddress.fromBytes(protocol, bytes), port)
  }
}
