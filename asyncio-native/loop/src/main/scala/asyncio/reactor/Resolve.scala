package asyncio.reactor

import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.netdb
import scala.scalanative.posix.netdbOps.*
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.Sockets.Flavor

/** One address a host name resolved to, in numeric form. */
final case class ResolvedAddress(flavor: Flavor, host: String)

/** Resolves a host name without blocking the reactor. There is no non-blocking `getaddrinfo`, so the lookup runs on the
  * reactor's blocker pool, the way libuv and tokio do it. Completes with 0 and `addresses` filled, or -1 with `error`
  * set.
  */
final class Resolve(val host: String) extends Blocking {
  @volatile private var outcome: Either[String, List[ResolvedAddress]] = Left("not resolved yet")

  def addresses: List[ResolvedAddress] = outcome.getOrElse(Nil)
  def error: String = outcome.left.getOrElse("")

  def block(): Int = {
    outcome = Resolve.lookup(host)
    if outcome.isRight then 0 else -1
  }
}

object Resolve {

  /** The blocking lookup itself: every stream address for `host`, in numeric form, IPv4 and IPv6. */
  private def lookup(host: String): Either[String, List[ResolvedAddress]] = Zone.acquire { implicit z =>
    val hints = alloc[netdb.addrinfo]()
    hints.ai_socktype = socket.SOCK_STREAM // one entry per address rather than one per socket type
    val results = alloc[Ptr[netdb.addrinfo]]()
    val status = netdb.getaddrinfo(toCString(host), null, hints, results)
    if status != 0 then Left(fromCString(netdb.gai_strerror(status)))
    else {
      val text = alloc[Byte](64)
      var found = List.empty[ResolvedAddress]
      var entry = !results
      while entry != null do {
        val family = entry.ai_family
        val address =
          if family == socket.AF_INET then entry.ai_addr.asInstanceOf[Ptr[in.sockaddr_in]].at3.asInstanceOf[Ptr[Byte]]
          else if family == socket.AF_INET6 then
            entry.ai_addr.asInstanceOf[Ptr[in.sockaddr_in6]].at4.asInstanceOf[Ptr[Byte]]
          else null
        if address != null && inet.inet_ntop(family, address, text, 64.toUInt) != null then
          found =
            ResolvedAddress(if family == socket.AF_INET then Flavor.IPv4 else Flavor.IPv6, fromCString(text)) :: found
        entry = entry.ai_next
      }
      netdb.freeaddrinfo(!results)
      Right(found.reverse)
    }
  }
}
