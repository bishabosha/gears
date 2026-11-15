package gears.async.io
package node

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.Uint8Array

/** An IP datagram socket, a `dgram.Socket` of Node's: bound, or connected to one peer.
  *
  * Node takes every packet from the system as it arrives and hands it over at once, to whoever listens, so the packets
  * no receive has taken wait here, as they would in the socket's receive buffer, and no more of them than it holds
  * (`SO_RCVBUF`, as Node reports it): one that arrives when they fill it is dropped, as the system drops it. The
  * receives waiting take them in the order they came.
  *
  * A send is handed to Node at once, which sends them in the order given, each a whole packet, and cannot take one
  * back: its op is held until Node has sent it, so a cancel or a time limit that comes meanwhile is too late, and a
  * close waits for it.
  */
sealed abstract class DatagramHandle private[node] (name: String, socket: js.Dynamic, v6: Boolean)
    extends NodeHandle(name) {
  private[node] val local: IpSocketAddress = {
    val bound = Errors.guard(socket.address())
    Addresses.reported(bound.address, bound.port)
  }
  private val packets = scala.collection.mutable.ArrayDeque.empty[DatagramHandle.Packet]
  private val receives = scala.collection.mutable.ArrayDeque.empty[DatagramHandle.Receive]
  private var held = 0
  private var capacity = receiveBufferSize()
  private var failed: IOException | Null = null

  private def receiveBufferSize(): Int =
    try socket.getRecvBufferSize().asInstanceOf[Int]
    catch case _: js.JavaScriptException => DatagramHandle.DefaultCapacity

  private val onMessage: js.Function2[Uint8Array, js.Dynamic, Unit] = (message, sender) => {
    val packet = new DatagramHandle.Packet(message, sender)
    if receives.nonEmpty then {
      val receive = receives.head
      try {
        receive.deliver(packet)
        receive.run.succeed() // which takes it out of the queue
      } catch case failure: Throwable => receive.run.fail(failure)
    } else if held + packet.cost <= capacity then {
      packets.append(packet)
      held += packet.cost
    } // else dropped: the socket's buffer is full
  }

  /** A failure Node reports of the socket itself: on a connected one, that its peer refused a packet. It fails the
    * receive waiting, or else the next op, once, as the system reports it once.
    */
  private val onError: js.Function1[js.Dynamic, Unit] = failure => {
    val error = Errors.io(failure)
    if receives.nonEmpty then receives.head.run.fail(error) else failed = error
  }

  socket.on("message", onMessage)
  socket.on("error", onError)

  private[node] def kind: SocketOption.Kind

  private def takeFailure(): Unit = {
    val failure = failed
    if failure != null then {
      failed = null
      throw failure
    }
  }

  /** Receives one packet for `run`, behind the receives already waiting. */
  private[node] def receive(run: Run, deliver: DatagramHandle.Packet => Unit): Unit =
    if receives.isEmpty && packets.nonEmpty then {
      val packet = packets.removeHead()
      held -= packet.cost
      deliver(packet)
      run.succeed()
    } else {
      takeFailure()
      val receive = new DatagramHandle.Receive(run, deliver)
      receives.append(receive)
      run.release = () => receives.removeFirst(_ eq receive): Unit
    }

  /** Sends `packet` for `run`, to `to`, or to the peer if null, and calls `sent` once Node has sent it. */
  private[node] def send(run: Run, packet: Int8Array, to: IpSocketAddress | Null, sent: () => Unit): Unit = {
    takeFailure()
    val bytes = new Uint8Array(packet.buffer, packet.byteOffset, packet.length)
    run.hold()
    val done: js.Function1[js.Dynamic, Unit] = failure =>
      if failure != null && !js.isUndefined(failure) then run.fail(Errors.io(failure))
      else {
        sent()
        run.succeed()
      }
    try
      if to == null then socket.send(bytes, done)
      else
        socket.send(bytes, to.port, if v6 then Addresses.hostForIPv6(to.address) else Addresses.host(to.address), done)
    catch case js.JavaScriptException(failure) => run.fail(Errors.io(failure))
  }

  private[node] def option(option: SocketOption[?]): Any = Errors.guard(option match {
    case SocketOption.ReceiveBuffer => socket.getRecvBufferSize().asInstanceOf[Int]
    case SocketOption.SendBuffer    => socket.getSendBufferSize().asInstanceOf[Int]
    case other                      => throw new IllegalArgumentException(s"This backend has no socket option $other")
  })

  private[node] def setOption(option: SocketOption[?], value: Any): Unit = Errors.guard(option match {
    case SocketOption.ReceiveBuffer =>
      socket.setRecvBufferSize(value.asInstanceOf[Int])
      capacity = receiveBufferSize()
    case SocketOption.SendBuffer => socket.setSendBufferSize(value.asInstanceOf[Int]): Unit
    case other                   => throw new IllegalArgumentException(s"This backend has no socket option $other")
  })

  protected def shut(done: () => Unit): Unit = {
    packets.clear()
    held = 0
    val closed: js.Function0[Unit] = () => done()
    Errors.guard(socket.close(closed)): Unit
  }
}

private[node] object DatagramHandle {

  /** What the packets waiting may hold where Node does not tell the socket's receive buffer. */
  inline val DefaultCapacity = 262144

  /** A packet Node handed over, with its sender as Node reports it. */
  final class Packet(val bytes: Uint8Array, val sender: js.Dynamic) {

    /** What the packet takes of the socket's buffer: its bytes, and something for itself, so that empty ones count. */
    def cost: Int = bytes.length + 64
  }

  final class Receive(val run: Run, val deliver: Packet => Unit)

  val ignore: js.Function1[js.Dynamic, Unit] = _ => ()
}

final class BoundDatagramHandle private[node] (socket: js.Dynamic, v6: Boolean)
    extends DatagramHandle("bound datagram socket", socket, v6) {
  private[node] def kind: SocketOption.Kind = SocketOption.Kind.BoundDatagram
}

final class ConnectedDatagramHandle private[node] (
    socket: js.Dynamic,
    v6: Boolean,
    private[node] val peer: IpSocketAddress
) extends DatagramHandle("connected datagram socket", socket, v6) {
  private[node] def kind: SocketOption.Kind = SocketOption.Kind.ConnectedDatagram
}

/** Opens a datagram socket: bound to `local`, and if `remote` is given connected to it, bound to `local` or to any
  * address of the remote's family.
  */
private[node] final class OpenDatagramOp(
    local: IpSocketAddress | Null,
    remote: IpSocketAddress | Null,
    options: SocketOptions,
    into: Slot[? >: BoundDatagramHandle & ConnectedDatagramHandle <: DatagramHandle]
) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(into)

  protected def start(run: Run): Unit = {
    val v6 = (if remote != null then remote else local.nn).address.protocol == Protocol.IPv6
    val kind = if remote == null then SocketOption.Kind.BoundDatagram else SocketOption.Kind.ConnectedDatagram
    val socket = Errors.guard(Dgram.createSocket(js.Dynamic.literal(`type` = if v6 then "udp6" else "udp4")))
    var opened = false
    val onError: js.Function1[js.Dynamic, Unit] = failure => run.fail(Errors.io(failure))
    socket.on("error", onError)
    run.release = () =>
      if !opened then { // failed, cancelled or out of time: nothing is left open
        socket.on("error", DatagramHandle.ignore)
        try socket.close(): Unit
        catch case _: js.JavaScriptException => () // not bound yet, or closed by its failure
      }

    def finish(): Unit =
      if !run.done then
        try {
          for (option, value) <- SocketOption.opening(options, kind) do
            option match {
              case SocketOption.ReceiveBuffer => Errors.guard(socket.setRecvBufferSize(value.asInstanceOf[Int])): Unit
              case SocketOption.SendBuffer    => Errors.guard(socket.setSendBufferSize(value.asInstanceOf[Int])): Unit
              case _                          => ()
            }
          val handle =
            if remote == null then new BoundDatagramHandle(socket, v6)
            else new ConnectedDatagramHandle(socket, v6, remote)
          socket.removeListener("error", onError)
          opened = true
          into.asInstanceOf[Slot[DatagramHandle]].set(handle)
          run.succeed()
        } catch case failure: Throwable => run.fail(failure)

    val connected: js.Function0[Unit] = () => finish()
    val bound: js.Function0[Unit] = () =>
      if !run.done then
        if remote == null then finish()
        else
          try socket.connect(remote.port, Addresses.host(remote.address), connected): Unit
          catch case js.JavaScriptException(failure) => run.fail(Errors.io(failure))
    val to = js.Dynamic.literal(
      address = if local != null then Addresses.host(local.address) else if v6 then "::" else "0.0.0.0",
      port = if local != null then local.port else 0,
      exclusive = true
    )
    Errors.guard(socket.bind(to, bound)): Unit
  }
}

/** Receives one packet into `buf`, cleared first and left flipped, and its sender into `from`, if given. */
private[node] final class ReceiveOp(on: DatagramHandle, buf: ByteBuffer, from: Slot[IpSocketAddress] | Null)
    extends NodeOp
    with Repeatable {
  override protected def slots: List[Slot[?]] = if from == null then Nil else List(from)
  override protected def handle: NodeHandle = on

  override protected def check(): Unit =
    if buf.capacity() == 0 then
      throw new IllegalArgumentException("A receive into a buffer of no capacity would drop a whole packet")

  protected def start(run: Run): Unit = on.receive(
    run,
    packet => {
      val sender = if from == null then null else Addresses.reported(packet.sender.address, packet.sender.port)
      val bytes = Buffers.signed(packet.bytes)
      buf.clear()
      Buffers.fill(Array(buf), bytes.subarray(0, math.min(bytes.length, buf.capacity()))) // the rest is dropped
      buf.flip()
      if from != null then from.set(sender.nn)
    }
  )
}

/** Sends `bufs`, in order, as one packet, to `to`, or to the peer of a connected socket if null. */
private[node] final class SendOp(on: DatagramHandle, bufs: Array[ByteBuffer], to: IpSocketAddress | Null)
    extends NodeOp
    with Repeatable {
  override protected def handle: NodeHandle = on

  override protected def check(): Unit =
    if to == null && Buffers.remaining(bufs) == 0 then
      throw new IllegalArgumentException("A connected datagram socket sends no empty packet")

  protected def start(run: Run): Unit = {
    // Copied without consuming: the buffers are consumed once the packet has been sent, not if the send fails.
    val packet = Buffers.drain(bufs.map(_.duplicate()), Buffers.room(bufs))
    on.send(run, packet, to, () => bufs.foreach(buf => buf.position(buf.limit())))
  }
}
