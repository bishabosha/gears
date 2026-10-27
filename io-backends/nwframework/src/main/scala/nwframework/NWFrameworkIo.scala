package gears.async.io
package nwframework

import gears.async.io.internal.Reactor

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

/** The C side of the backend, `nwframework_io.c` beside this project's resources: the blocks Network.framework calls,
  * each a trampoline that queues an event on its handle and wakes the kqueue reactor watching it, by a user event on
  * its kqueue; the list of handles with events a reactor drains; and the framework calls themselves. Everything returns
  * at once, except `nwf_cancel` of a listener, which waits for its cancelled state. What the events mean is
  * `NWFrameworkHandle`'s business.
  */
@extern
private[nwframework] object NWFrameworkIo {
  type Reactor = Ptr[Byte]
  type Handle = Ptr[Byte]

  /** An event as the C side copies it out (`nwf_event`): next (unused), kind, error, complete, final, len, data, bytes,
    * accepted.
    */
  type Event = CStruct9[Ptr[Byte], CInt, CInt, CInt, CInt, CSize, Ptr[Byte], Ptr[Byte], Handle]

  def nwf_reactor_create(kq: CInt): Reactor = extern
  def nwf_reactor_destroy(reactor: Reactor): Unit = extern
  def nwf_reactor_ready(reactor: Reactor): CInt = extern
  def nwf_reactor_drain(reactor: Reactor, out: Ptr[Handle], max: CInt): CInt = extern

  def nwf_handle_release(handle: Handle): Unit = extern
  def nwf_handle_watch(handle: Handle, reactor: Reactor, mask: CInt): Unit = extern
  def nwf_handle_next(handle: Handle, out: Ptr[Event]): CInt = extern
  def nwf_event_done(event: Ptr[Event]): Unit = extern
  def nwf_cancel(handle: Handle): Unit = extern
  def nwf_cancel_async(handle: Handle): Unit = extern

  /** TCP's options as a connection or listener opens (`nwf_tcp_settings`): no delay, keepalive (0 the system's, 1 off,
    * 2 on), its idle time, interval and count.
    */
  type TcpSettings = CStruct5[CInt, CInt, CInt, CInt, CInt]

  def nwf_connect(host: CString, port: CInt, settings: Ptr[TcpSettings]): Handle = extern
  def nwf_receive(handle: Handle, max: CUnsignedInt): Unit = extern
  def nwf_receive_message(handle: Handle): Unit = extern
  def nwf_receive_held(handle: Handle, max: CUnsignedInt): Unit = extern
  def nwf_data_copy(data: Ptr[Byte], into: Ptr[Byte]): Unit = extern
  def nwf_send(handle: Handle, bytes: Ptr[Byte], len: CInt): Unit = extern
  def nwf_send_final(handle: Handle): Unit = extern
  def nwf_send_owned(handle: Handle, bytes: Ptr[Byte], len: CInt): Unit = extern

  def nwf_listen(host: CString, port: CInt, settings: Ptr[TcpSettings]): Handle = extern

  def nwf_datagram_connect(host: CString, port: CInt, localHost: CString, localPort: CInt, dontFragment: CInt): Handle =
    extern
  def nwf_udp_receive_buffer(): CInt = extern

  def nwf_is_host_address(host: CString): CInt = extern
  def nwf_connection_address(handle: Handle, peer: CInt, out: Ptr[socket.sockaddr_storage]): CInt = extern
  def nwf_listener_port(handle: Handle): CInt = extern
}

/** The kinds of event the C side queues, and the fields of one. */
private[nwframework] object NWFrameworkEvent {
  inline val Received = 1
  inline val Sent = 2
  inline val Ready = 3
  inline val Failed = 4
  inline val Cancelled = 5
  inline val Accepted = 6
  inline val Packet = 7

  /** The kinds that wake a reactor with an op waiting to read, to write, or to connect, as bits for `nwf_handle_watch`;
    * the connection ending wakes whichever.
    */
  inline val Ending = (1 << Failed) | (1 << Cancelled)
  inline val ForRead = (1 << Received) | (1 << Packet) | (1 << Accepted) | Ending
  inline val ForWrite = (1 << Sent) | Ending
  inline val ForConnect = (1 << Ready) | Ending

  extension (e: Ptr[NWFrameworkIo.Event]) {
    inline def kind: Int = e._2
    inline def error: Int = e._3
    inline def complete: Boolean = e._4 != 0
    inline def isFinal: Boolean = e._5 != 0
    inline def len: Int = e._6.toInt

    /** Non-null for content held for the reactor to copy (`nwf_receive_held`), `len` bytes of it. */
    inline def held: Ptr[Byte] = e._7
    inline def bytes: Ptr[Byte] = e._8
    inline def accepted: NWFrameworkIo.Handle = e._9
  }
}
