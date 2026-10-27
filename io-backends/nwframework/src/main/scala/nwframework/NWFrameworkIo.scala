package gears.async.io
package nwframework

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

/** The C side of the backend, `nwframework_io.c` beside this project's resources: the blocks Network.framework and
  * Dispatch call, each a trampoline that queues an event on its handle and wakes the reactor watching it; the queue a
  * reactor parks on; and the framework calls themselves. Everything returns at once, except `nwf_reactor_wait`, which
  * parks the reactor's thread, and `nwf_cancel` of a listener, which waits for its cancelled state. What the events
  * mean is `NWFrameworkHandle`'s business.
  */
@extern
private[nwframework] object NWFrameworkIo {
  type Reactor = Ptr[Byte]
  type Handle = Ptr[Byte]

  /** An event as the C side copies it out (`nwf_event`): next (unused), kind, error, complete, final, len, data,
    * flow_id, bytes, flow, accepted, from_len, from.
    */
  type Event = CStruct13[
    Ptr[Byte],
    CInt,
    CInt,
    CInt,
    CInt,
    CSize,
    Ptr[Byte],
    CSize,
    Ptr[Byte],
    Handle,
    Handle,
    socket.socklen_t,
    socket.sockaddr_storage
  ]

  def nwf_reactor_create(): Reactor = extern
  def nwf_reactor_destroy(reactor: Reactor): Unit = extern
  def nwf_reactor_wake(reactor: Reactor): Unit = extern
  def nwf_reactor_ready(reactor: Reactor): CInt = extern
  @blocking def nwf_reactor_wait(reactor: Reactor, timeoutNanos: CLongLong): Unit = extern
  def nwf_reactor_drain(reactor: Reactor, out: Ptr[Handle], max: CInt): CInt = extern

  def nwf_handle_release(handle: Handle): Unit = extern
  def nwf_handle_watch(handle: Handle, reactor: Reactor, mask: CInt): Unit = extern
  def nwf_handle_next(handle: Handle, out: Ptr[Event]): CInt = extern
  def nwf_event_done(event: Ptr[Event]): Unit = extern
  def nwf_cancel(handle: Handle): Unit = extern
  def nwf_cancel_async(handle: Handle): Unit = extern

  def nwf_connect(host: CString, port: CInt): Handle = extern
  def nwf_receive(handle: Handle, max: CUnsignedInt): Unit = extern
  def nwf_receive_message(handle: Handle): Unit = extern
  def nwf_receive_held(handle: Handle, max: CUnsignedInt): Unit = extern
  def nwf_data_copy(data: Ptr[Byte], into: Ptr[Byte]): Unit = extern
  def nwf_send(handle: Handle, bytes: Ptr[Byte], len: CInt): Unit = extern
  def nwf_send_final(handle: Handle): Unit = extern

  def nwf_listen(host: CString, port: CInt): Handle = extern

  def nwf_datagram_connect(host: CString, port: CInt, localHost: CString, localPort: CInt): Handle = extern
  def nwf_datagram_bind(host: CString, port: CInt): Handle = extern
  def nwf_flow_open(bound: Handle, host: CString, port: CInt): Handle = extern
  def nwf_flow_remote(flow: Handle, from: Ptr[socket.sockaddr_storage]): CInt = extern
  def nwf_flow_id(flow: Handle): CUnsignedLongLong = extern
  def nwf_udp_receive_buffer(): CInt = extern

  def nwf_fd_open(fd: CInt): Handle = extern
  def nwf_fd_arm(handle: Handle, read: CInt): Unit = extern
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
  inline val Flow = 8
  inline val Readable = 9
  inline val Writable = 10

  /** The kinds that wake a reactor with an op waiting to read, to write, or to connect, as bits for `nwf_handle_watch`;
    * the connection ending wakes whichever.
    */
  inline val Ending = (1 << Failed) | (1 << Cancelled)
  inline val ForRead = (1 << Received) | (1 << Packet) | (1 << Accepted) | (1 << Flow) | (1 << Readable) | Ending
  inline val ForWrite = (1 << Sent) | (1 << Writable) | Ending
  inline val ForConnect = (1 << Ready) | Ending

  extension (e: Ptr[NWFrameworkIo.Event]) {
    inline def kind: Int = e._2
    inline def error: Int = e._3
    inline def complete: Boolean = e._4 != 0
    inline def isFinal: Boolean = e._5 != 0
    inline def len: Int = e._6.toInt
    inline def bytes: Ptr[Byte] = e._9

    /** Non-null for content held for the reactor to copy (`nwf_receive_held`), `len` bytes of it. */
    inline def held: Ptr[Byte] = e._7
    inline def flow: NWFrameworkIo.Handle = e._10

    /** The flow's number, unique for the process (`nwf_flow`), by which its handle knows it. */
    inline def flowId: Long = e._8.toLong
    inline def accepted: NWFrameworkIo.Handle = e._11
    inline def fromLength: socket.socklen_t = e._12
    inline def from: Ptr[socket.sockaddr_storage] = e.at13
  }
}
