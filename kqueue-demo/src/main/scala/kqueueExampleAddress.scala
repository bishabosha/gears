package example

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.*

import asyncio.unsafe.Bracket
import asyncio.unsafe.PosixFileOps
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport
import asyncio.unsafe.Bracket.FileOperation

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
    case IPv4(host, port) => PosixSockets.bindIPv4Addr(fd, host, port)
    case IPv6(host, port) => PosixSockets.bindIPv6Addr(fd, host, port)
  }

  def connect(fd: Int): Unit = this match {
    case Unix(sock)       => PosixSockets.connectUnixAddr(fd, sock)
    case IPv4(host, port) => PosixSockets.connectIPv4Addr(fd, host, port)
    case IPv6(host, port) => PosixSockets.connectIPv6Addr(fd, host, port)
  }

  def withSocket(transport: Transport)(use: FileOperation): Unit = {
    open(transport)(use(_))
  }

  def withBoundSocket(transport: Transport)(use: FileOperation): Unit = {
    open(transport) { fd =>
      bind(fd)
      println(s"Bound socket $fd to $this")
      use(fd)
    }
  }

  private inline def open(transport: Transport)(inline use: Int => Unit): Unit = {
    Bracket.fileResource(PosixSockets.open(flavor, transport))(close)(fd =>
      PosixSockets.setNonBlocking(fd)
      use(fd)
    )
  }

  private def close(fd: Int): Unit = {
    PosixSockets.close(fd)
    this match
      case Unix(sock) =>
        Zone.acquire { implicit z =>
          PosixFileOps.safeUnlink(toCString(sock, StandardCharsets.UTF_8))
        }
      case _ => ()
  }
}
