package gears.async.io

/** A socket option: a key whose values are `V`s, given as a socket opens (`SocketOptions`), and on some backends
  * changed after (`SocketOptionOps.setOption`). Not every backend has every option, nor every host one backend runs on,
  * so whether one does is asked of its ops when the program runs (`SocketOptionOps.supports`), and an op built with one
  * it has not fails as it is built, with an `IllegalArgumentException`, never ignoring it.
  */
sealed abstract class SocketOption[V](val name: String) {

  /** Whether it applies to a socket of kind `kind`, given as it opens, or read and changed on it after. */
  def appliesTo(kind: SocketOption.Kind): Boolean

  /** Checks a value as the op is built, with an `IllegalArgumentException` for one out of range. */
  private[io] def check(value: V): Unit = ()

  override def toString: String = name
}

object SocketOption {
  import Kind.*

  /** The kinds of socket an option applies to. */
  enum Kind {
    case Connection, Listener, BoundDatagram, ConnectedDatagram
  }

  /** Sends small writes at once rather than waiting to coalesce them (Nagle's algorithm off, `TCP_NODELAY`): on unless
    * set off, on every backend, as Go has it, since TLS records, HTTP/2 frames and WebSocket messages are small writes
    * that Nagle's algorithm, with delayed acknowledgements, can hold back by tens of milliseconds. Stream sockets only;
    * given to `listen`, it applies to every connection the listener accepts.
    */
  object NoDelay extends SocketOption[Boolean]("NoDelay") {
    def appliesTo(kind: Kind): Boolean = kind == Connection || kind == Listener
  }

  /** Probes an idle connection so that a peer gone without a word is found out (`SO_KEEPALIVE` and its timings): off by
    * default, the system's. Stream sockets only; given to `listen`, it applies to every connection accepted.
    */
  object KeepAlive extends SocketOption[gears.async.io.KeepAlive]("KeepAlive") {
    def appliesTo(kind: Kind): Boolean = kind == Connection || kind == Listener
    override private[io] def check(value: gears.async.io.KeepAlive): Unit = value match {
      case gears.async.io.KeepAlive.On(idle, interval, count) =>
        if idle <= 0 || interval <= 0 || count <= 0 then
          throw new IllegalArgumentException(s"Keepalive's timings are positive: $value")
      case gears.async.io.KeepAlive.Off => ()
    }
  }

  /** The size of the socket's receive buffer, in bytes (`SO_RCVBUF`): a request, which the system may round or double,
    * so `SocketOptionOps.option` tells what it gave. Given to `listen`, before it listens, so that its connections'
    * windows are scaled for it.
    */
  object ReceiveBuffer extends SocketOption[Int]("ReceiveBuffer") {
    def appliesTo(kind: Kind): Boolean = true
    override private[io] def check(value: Int): Unit =
      if value <= 0 then throw new IllegalArgumentException(s"A buffer size is positive, not $value")
  }

  /** The size of the socket's send buffer, in bytes (`SO_SNDBUF`): a request, as `ReceiveBuffer` is. */
  object SendBuffer extends SocketOption[Int]("SendBuffer") {
    def appliesTo(kind: Kind): Boolean = true
    override private[io] def check(value: Int): Unit =
      if value <= 0 then throw new IllegalArgumentException(s"A buffer size is positive, not $value")
  }

  /** Sets the don't-fragment bit on the datagrams sent (`IP_DONTFRAG`, `IPV6_DONTFRAG`, Linux's `IP_MTU_DISCOVER`), as
    * QUIC requires (RFC 9000 §14): a datagram larger than the path's MTU then fails to send, with an `IOException`,
    * where it would be fragmented. Off by default, the system's. Datagram sockets only: TCP finds the path's MTU
    * itself. An IPv6 socket's covers what it sends to IPv4 peers too (probed on macOS 27.0).
    */
  object DontFragment extends SocketOption[Boolean]("DontFragment") {
    def appliesTo(kind: Kind): Boolean = kind == BoundDatagram || kind == ConnectedDatagram
  }

  /** Lets other sockets that set it bind the same address and port (`SO_REUSEPORT`), as a listener or a bound datagram
    * socket per worker does. Which of them a connection or a datagram goes to is the system's: Linux spreads them by
    * the peer's address, macOS gives every one to the socket bound last (probed on macOS 27.0), so it shares a port
    * without promising to share the load. Off by default; given as the socket opens only, since it must be set before
    * the socket binds.
    */
  object ReusePort extends SocketOption[Boolean]("ReusePort") {
    def appliesTo(kind: Kind): Boolean = kind == Listener || kind == BoundDatagram
  }

  /** Every option, for a backend's `supports`. */
  val all: List[SocketOption[?]] = List(NoDelay, KeepAlive, ReceiveBuffer, SendBuffer, DontFragment, ReusePort)

  /** What a socket of kind `kind` opening with `options` sets on itself, as it opens, before it binds: those that apply
    * to it, `NoDelay` on for a connection unless they set it off, and for a listener not those it gives each connection
    * it accepts (`accepted`).
    */
  private[io] def opening(options: SocketOptions, kind: Kind): List[(SocketOption[?], Any)] = {
    val set = options.options.toList.collect {
      case option if option.appliesTo(kind) && !(kind == Listener && forConnections(option)) =>
        (option, options.get(option).nn)
    }
    if kind == Connection && options.get(NoDelay) == null then (NoDelay, true) :: set else set
  }

  /** What a connection a listener opened with `options` accepts sets on itself: `NoDelay`, on unless they set it off,
    * keepalive and the send buffer; its receive buffer it has from the listener, which set it before listening, so that
    * the connection's window scales for it.
    */
  private[io] def accepted(options: SocketOptions): List[(SocketOption[?], Any)] =
    (NoDelay, options.get(NoDelay) != false) ::
      List(KeepAlive, SendBuffer).flatMap(o =>
        options.get(o) match {
          case null  => Nil
          case value => List((o, value))
        }
      )

  /** The options a listener does not set on itself but gives each connection it accepts. */
  private def forConnections(option: SocketOption[?]): Boolean = option == NoDelay || option == KeepAlive

  /** Whether `option` can be read or changed on an open socket of kind `kind`: one that applies to it, and on a
    * listener not one it only gives the connections it accepts.
    */
  private[io] def onSocket(option: SocketOption[?], kind: Kind): Boolean =
    option.appliesTo(kind) && !(kind == Listener && forConnections(option))
}

/** Keepalive's setting (see `SocketOption.KeepAlive`). */
enum KeepAlive {

  /** No probes. */
  case Off

  /** Probes once a connection has been idle `idleSeconds`, every `intervalSeconds` after, giving it up after `count`
    * have gone unanswered.
    */
  case On(idleSeconds: Int, intervalSeconds: Int, count: Int)
}

/** The options a socket opens with, the system's for those not set, except that a stream socket has `NoDelay` on unless
  * it is set off. Immutable: `set` returns a new set.
  */
final class SocketOptions private (private val values: Map[SocketOption[?], Any]) {

  /** `option`'s value here, or null if it is not set. */
  def get[V](option: SocketOption[V]): V | Null = values.get(option).orNull.asInstanceOf[V | Null]

  /** These options with `option` set to `value`, checked as the option checks its values. */
  def set[V](option: SocketOption[V], value: V): SocketOptions = {
    option.check(value)
    new SocketOptions(values.updated(option, value))
  }

  /** The options set. */
  def options: Iterable[SocketOption[?]] = values.keys

  override def equals(other: Any): Boolean = other match {
    case o: SocketOptions => o.values == values
    case _                => false
  }
  override def hashCode: Int = values.hashCode
  override def toString: String = values.map((k, v) => s"$k = $v").mkString("SocketOptions(", ", ", ")")
}

object SocketOptions {

  /** Nothing set: the defaults, `NoDelay` on for a stream socket and the system's for the rest. */
  val default: SocketOptions = new SocketOptions(Map.empty)
}

/** The socket options ops, for a backend with IP sockets: what it supports, and reading and changing them on an open
  * socket. Plain methods, not ops: every backend answers at once.
  */
trait SocketOptionOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Whether this backend, on this host, has `option`: whether an opener given it applies it rather than failing. */
  def supports(option: SocketOption[?]): Boolean

  /** Whether `option` can be changed on an open socket (`setOption`) rather than only given as it opens: never
    * `ReusePort`, which must be set before the socket binds.
    */
  def canChange(option: SocketOption[?]): Boolean

  /** `option`'s value on the open socket `handle`, as the system has it: a buffer's size as the system gave it. Fails
    * with an `IllegalArgumentException` for an option the backend does not support or that does not apply to the
    * socket, and an `IOException` on a closed handle.
    */
  def option[V](handle: H, option: SocketOption[V]): V

  /** Sets `option` to `value` on the open socket `handle`, failing as `option` does, and with an
    * `IllegalArgumentException` for one it cannot change (`canChange`).
    */
  def setOption[V](handle: H, option: SocketOption[V], value: V): Unit
}
