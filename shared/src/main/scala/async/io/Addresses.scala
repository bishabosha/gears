package gears.async.io

/** Where an IP socket lives: a numeric IPv4 or IPv6 address and a port. */
enum IpAddress {
  case IPv4(host: String, port: Int)
  case IPv6(host: String, port: Int)
}

/** Where a Unix domain socket lives: a path, or the empty path for an unnamed socket. */
final case class UnixAddress(path: String)

enum AddressFamily {
  case IPv4, IPv6
}

final case class ResolvedAddress(family: AddressFamily, host: String)

/** Addresses as text, the same on every backend. */
private[io] object IpText {

  /** The 16 bytes of an IPv6 address in RFC 5952's form, as `inet_ntop` writes it: groups in lower-case hex without
    * leading zeros, the first longest run of two or more zero groups written `::`, and an IPv4-mapped or
    * IPv4-compatible address with its IPv4 part dotted (`::ffff:192.0.2.1`, `::192.0.2.1`), as BIND's `inet_ntop`,
    * which macOS's and glibc's are, does.
    */
  def ipv6(bytes: Array[Byte]): String = {
    require(bytes.length == 16, s"An IPv6 address has 16 bytes, not ${bytes.length}")
    val groups = Array.tabulate(8)(i => ((bytes(2 * i) & 0xff) << 8) | (bytes(2 * i + 1) & 0xff))
    def dotted = s"${bytes(12) & 0xff}.${bytes(13) & 0xff}.${bytes(14) & 0xff}.${bytes(15) & 0xff}"
    if groups.take(5).forall(_ == 0) && groups(5) == 0xffff then s"::ffff:$dotted"
    else if groups.take(6).forall(_ == 0) && groups(6) != 0 then s"::$dotted"
    else {
      var bestStart = -1
      var bestLength = 1 // a run of one zero group is written as 0, not ::
      var i = 0
      while i < 8 do {
        if groups(i) == 0 then {
          var j = i
          while j < 8 && groups(j) == 0 do j += 1
          if j - i > bestLength then {
            bestStart = i
            bestLength = j - i
          }
          i = j
        } else i += 1
      }
      def hex(from: Int, until: Int) = (from until until).map(g => Integer.toHexString(groups(g))).mkString(":")
      if bestStart < 0 then hex(0, 8)
      else s"${hex(0, bestStart)}::${hex(bestStart + bestLength, 8)}"
    }
  }
}
