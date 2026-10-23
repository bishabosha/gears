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
