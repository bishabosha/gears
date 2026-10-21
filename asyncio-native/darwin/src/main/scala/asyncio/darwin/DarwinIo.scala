package asyncio.darwin

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

/** The C side of the backend, `darwin_io.c` beside this project's resources: the blocks Network.framework and Dispatch
  * call, each a trampoline that queues an event on its handle and wakes the reactor watching it; the queue a reactor
  * parks on; and the framework calls themselves. Everything returns at once, except `dn_reactor_wait`, which parks the
  * reactor's thread, `dn_listen`, which waits for the listener to be ready or to fail, and `dn_cancel` of a listener,
  * which waits for its cancelled state. What the events mean is `DarwinHandle`'s business.
  */
@extern
private[darwin] object DarwinIo {
  type Reactor = Ptr[Byte]
  type Handle = Ptr[Byte]

  /** An event as the C side copies it out (`dn_event`): next (unused), kind, error, complete, final, len, data, map,
    * bytes, flow, accepted, from_len, from.
    */
  type Event = CStruct13[
    Ptr[Byte],
    CInt,
    CInt,
    CInt,
    CInt,
    CSize,
    Ptr[Byte],
    Ptr[Byte],
    Ptr[Byte],
    Handle,
    Handle,
    socket.socklen_t,
    socket.sockaddr_storage
  ]

  def dn_reactor_create(): Reactor = extern
  def dn_reactor_destroy(reactor: Reactor): Unit = extern
  def dn_reactor_wake(reactor: Reactor): Unit = extern
  def dn_reactor_ready(reactor: Reactor): CInt = extern
  @blocking def dn_reactor_wait(reactor: Reactor, timeoutNanos: CLongLong): Unit = extern
  def dn_reactor_drain(reactor: Reactor, out: Ptr[Handle], max: CInt): CInt = extern

  def dn_handle_release(handle: Handle): Unit = extern
  def dn_handle_watch(handle: Handle, reactor: Reactor, mask: CInt): Unit = extern
  def dn_handle_next(handle: Handle, out: Ptr[Event]): CInt = extern
  def dn_event_done(event: Ptr[Event]): Unit = extern
  def dn_cancel(handle: Handle): Unit = extern

  def dn_connect(host: CString, port: CInt): Handle = extern
  def dn_receive(handle: Handle, max: CUnsignedInt): Unit = extern
  def dn_receive_message(handle: Handle): Unit = extern
  def dn_send(handle: Handle, bytes: Ptr[Byte], len: CInt): Unit = extern

  def dn_listen(host: CString, port: CInt, reuse: CInt, error: Ptr[CInt]): Handle = extern

  def dn_datagram_connect(host: CString, port: CInt, localHost: CString, localPort: CInt): Handle = extern
  def dn_datagram_bind(host: CString, port: CInt, error: Ptr[CInt]): Handle = extern
  def dn_flow_open(bound: Handle, host: CString, port: CInt): Handle = extern
  def dn_flow_remote(flow: Handle, from: Ptr[socket.sockaddr_storage]): CInt = extern

  def dn_fd_open(fd: CInt): Handle = extern
  def dn_fd_arm(handle: Handle, read: CInt): Unit = extern
}

/** The kinds of event the C side queues, and the fields of one. */
private[darwin] object DarwinEvent {
  inline val Received = 1
  inline val Sent = 2
  inline val Ready = 3
  inline val Failed = 4
  inline val Cancelled = 5
  inline val Accepted = 6
  inline val Packet = 7
  inline val Flow = 8
  inline val Readable = 9
  inline val Writable = 10

  /** The kinds that wake a reactor with an op waiting to read, to write, or to connect, as bits for
    * `dn_handle_watch`; the connection ending wakes whichever.
    */
  inline val Ending = (1 << Failed) | (1 << Cancelled)
  inline val ForRead = (1 << Received) | (1 << Packet) | (1 << Accepted) | (1 << Flow) | (1 << Readable) | Ending
  inline val ForWrite = (1 << Sent) | (1 << Writable) | Ending
  inline val ForConnect = (1 << Ready) | Ending

  extension (e: Ptr[DarwinIo.Event]) {
    inline def kind: Int = e._2
    inline def error: Int = e._3
    inline def complete: Boolean = e._4 != 0
    inline def isFinal: Boolean = e._5 != 0
    inline def len: Int = e._6.toInt
    inline def bytes: Ptr[Byte] = e._9
    inline def flow: DarwinIo.Handle = e._10
    inline def accepted: DarwinIo.Handle = e._11
    inline def fromLength: socket.socklen_t = e._12
    inline def from: Ptr[socket.sockaddr_storage] = e.at13
  }
}
