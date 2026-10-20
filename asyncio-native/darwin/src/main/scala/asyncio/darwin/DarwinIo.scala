package asyncio.darwin

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

/** The C side of the backend, `darwin_io.c` beside this project's resources: Network.framework and Dispatch behind an
  * interface that returns at once, except `dn_reactor_wait`, which parks the reactor's thread, and `dn_listen`, which
  * waits for the listener to be ready or to fail. What the codes and kinds mean is documented there.
  */
@extern
private[darwin] object DarwinIo {
  type Reactor = Ptr[Byte]
  type Handle = Ptr[Byte]

  /** A drained notification: the token of the waiting op, what it waited for, and whether its handle was closed. */
  type Done = CStruct3[CLongLong, CInt, CInt]

  def dn_reactor_create(): Reactor = extern
  def dn_reactor_destroy(reactor: Reactor): Unit = extern
  def dn_reactor_wake(reactor: Reactor): Unit = extern
  def dn_reactor_ready(reactor: Reactor): CInt = extern
  @blocking def dn_reactor_wait(reactor: Reactor, timeoutNanos: CLongLong): Unit = extern
  def dn_reactor_drain(reactor: Reactor, out: Ptr[Done], max: CInt): CInt = extern

  def dn_handle_error(handle: Handle): CInt = extern
  def dn_cancel_wait(handle: Handle, kind: CInt, token: CLongLong): CInt = extern
  def dn_close(handle: Handle): Unit = extern

  def dn_connect(host: CString, port: CInt): Handle = extern
  def dn_wait_connected(handle: Handle, reactor: Reactor, token: CLongLong): CInt = extern
  def dn_read(handle: Handle, buf: Ptr[Byte], cap: CInt, reactor: Reactor, token: CLongLong): CInt = extern
  def dn_write(handle: Handle, bytes: Ptr[Byte], len: CInt, reactor: Reactor, token: CLongLong): CInt = extern
  def dn_write_outcome(handle: Handle): CInt = extern

  def dn_listen(host: CString, port: CInt, reuse: CInt, error: Ptr[CInt]): Handle = extern
  def dn_accept(handle: Handle, reactor: Reactor, token: CLongLong, code: Ptr[CInt]): Handle = extern

  def dn_datagram_connect(host: CString, port: CInt, localHost: CString, localPort: CInt): Handle = extern
  def dn_datagram_bind(host: CString, port: CInt, error: Ptr[CInt]): Handle = extern
  def dn_receive_from(
      handle: Handle,
      buf: Ptr[Byte],
      cap: CInt,
      from: Ptr[socket.sockaddr_storage],
      fromLength: Ptr[socket.socklen_t],
      reactor: Reactor,
      token: CLongLong
  ): CInt = extern
  def dn_send_to(
      handle: Handle,
      bytes: Ptr[Byte],
      len: CInt,
      host: CString,
      port: CInt,
      reactor: Reactor,
      token: CLongLong
  ): CInt = extern

  def dn_fd_open(fd: CInt): Handle = extern
  def dn_fd_wait(handle: Handle, read: CInt, reactor: Reactor, token: CLongLong): CInt = extern
}

/** The codes `DarwinIo`'s handle functions return, and the kinds of wait. */
private[darwin] object DarwinCodes {
  inline val Eof = -1
  inline val Closed = -2
  inline val Busy = -3
  inline val Error = -4
  inline val Waiting = -5

  inline val Read = 1
  inline val Write = 2
  inline val Connect = 3
}
