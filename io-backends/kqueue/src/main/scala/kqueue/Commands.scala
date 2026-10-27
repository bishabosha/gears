package gears.async.io
package kqueue

import gears.async.io.Interest
import gears.async.io.Received
import gears.async.io.Slot
import gears.async.io.internal.NativeBuffer
import gears.async.io.internal.NativePool
import gears.async.io.internal.NonBlocking
import gears.async.io.internal.PosixSockets

import java.nio.ByteBuffer
import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import KqueueBackend.Handle

/** The kqueue commands behind `KqueueOps`. `perform` returns true once the op is done, and false when the descriptor
  * was not ready after all, so the reactor waits again without completing it.
  */

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream; refused if
  * `buf` has no room, since a read of nothing would look like the end of the stream.
  */
final case class ReadIntoBuffer(fd: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Read
  override def refusal: Throwable | Null = if buf.hasRemaining() then null else gears.async.io.Ops.bufferFull(buf)
  private var read = 0 // what the last perform returned: bytes, 0 for would-block, or -1 at EOF
  private var filled = false
  def perform(): Boolean = {
    read = NonBlocking.read(fd, buf) // -1, EOF, leaves the buffer as it was
    filled = !buf.hasRemaining()
    read != 0
  }
  override def transferred: Int = read // -1 at EOF, which stays readable, so readiness is not known from it
  override def mayStillBeReady: Boolean = read < 0 || filled // a short read drained the socket
  override def edgeTriggered: Boolean = true
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read`): as `ReadIntoBuffer`, into a buffer taken when
  * the descriptor is ready, and given back if it was not after all, or if the read fails.
  */
final case class PooledRead(fd: Int, pool: NativePool, into: Slot[Received]) extends Command {
  override private[kqueue] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def interest: Interest = Interest.Read
  private var read = 0
  private var filled = false
  def perform(): Boolean = {
    val slot = pool.take()
    if slot == null then throw new java.io.IOException("The pool has no buffer free")
    val buffer = slot.nn.buffer
    read =
      try NonBlocking.read(fd, buffer) // -1 at EOF, which leaves the buffer as it was
      catch {
        case failure: Throwable =>
          slot.nn.release() // a failed read holds nothing
          throw failure
      }
    if read == 0 then {
      slot.nn.release()
      false
    } else {
      filled = !buffer.hasRemaining()
      buffer.flip()
      into.set(slot.nn)
      true
    }
  }
  override def transferred: Int = read
  override def mayStillBeReady: Boolean = read < 0 || filled
  override def edgeTriggered: Boolean = true
}

/** Drains `buf` between position and limit. Done once at least one byte has been written, or, with nothing remaining,
  * at once, with no system call.
  */
final case class WriteFromBuffer(fd: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Boolean = !buf.hasRemaining() || NonBlocking.write(fd, buf) >= 0
}

/** Accepts one connection on a listening socket, writing its handle into `into`. Done once a connection has been
  * accepted. The same command may be submitted again, after `into` is cleared, to accept the next connection.
  */
final case class Accept(fd: Int, into: Slot[KqueueKinds.Connection]) extends Command {
  override private[kqueue] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def interest: Interest = Interest.Read
  override def edgeTriggered: Boolean = true
  def perform(): Boolean = {
    val client = NonBlocking.accept(fd)
    if client >= 0 then into.set(KqueueKinds.connection(KqueueHandles.handle(client)))
    client >= 0
  }
  override def transferred: Int =
    1 // one connection, against the pending connections a listening socket's event reports
}

/** Accepts the connections pending on a listening socket, at most `max`, writing their handles into `into` (see
  * `SocketOps.acceptAll`). Done once at least one has been accepted.
  */
final class AcceptAll(val fd: Int, into: Slot[Array[KqueueKinds.Connection]], max: Int) extends Command {
  override private[kqueue] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def interest: Interest = Interest.Read
  override def edgeTriggered: Boolean = true
  private var count = 0
  def perform(): Boolean = {
    val clients = new Array[Int](max)
    count = 0
    var more = true
    try
      while more && count < max do {
        val client = NonBlocking.accept(fd)
        if client >= 0 then {
          clients(count) = client
          count += 1
        } else more = false
      }
    catch {
      case failure: Throwable =>
        if count == 0 then throw failure // with some accepted, they are delivered, and the next accept fails instead
    }
    if count > 0 then into.set(Array.tabulate(count)(i => KqueueKinds.connection(KqueueHandles.handle(clients(i)))))
    count > 0
  }
  override def transferred: Int =
    count // connections, against the pending connections a listening socket's event reports
}

/** Waits for a non-blocking connect to finish and checks its outcome. Done once connected; throws if it failed. */
final case class Connect(fd: Int) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Boolean = {
    PosixSockets.checkConnect(fd)
    true
  }
  override def tryFirst: Boolean = false // the check passes while the connection is still in progress
  override def registersReads: Boolean = true
}

/** Receives one packet and writes its sender into `from`, which holds a Unix address if `unix`, and an IP one
  * otherwise. Done once a packet has arrived.
  */
final class ReceiveFrom(val fd: Int, buf: ByteBuffer, from: Slot[? <: SocketAddress], unix: Boolean) extends Command {
  override private[kqueue] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(from)
  def interest: Interest = Interest.Read
  override def refusal: Throwable | Null = if buf.capacity() > 0 then null else gears.async.io.Ops.noCapacity(buf)
  private var received = -1
  override def transferred: Int = received
  override def edgeTriggered: Boolean = true
  def perform(): Boolean = {
    val storage = stackalloc[socket.sockaddr_storage]()
    val length = stackalloc[socket.socklen_t]()
    !length = sizeOf[socket.sockaddr_storage].toUInt
    buf.clear()
    val address = storage.asInstanceOf[Ptr[socket.sockaddr]]
    val size = PosixSockets.receiveDatagram(fd, NativeBuffer.atPosition(buf), buf.remaining(), address, length)
    received = size
    if size >= 0 then {
      NativeBuffer.advance(buf, size)
      buf.flip()
      val sender = KqueueHandles.decode(address, !length)
      if sender.isInstanceOf[gears.async.io.UnixAddress] != unix then
        throw new java.io.IOException(s"Received from $sender on a socket of the other family")
      from.asInstanceOf[Slot[SocketAddress]].set(sender)
    }
    size >= 0
  }
}

/** Sends one packet, to `to` or to the connected peer when `to` is null, consuming `buf`. Done once it has gone. On a
  * connected socket, `to` must be the peer, and the packet goes without an address, as the JDK sends it, since macOS
  * refuses any address there (`EISCONN`) and Linux would send to another; an empty packet is refused there, since the
  * JDK cannot send one (see `SocketOps.send`).
  */
final class SendTo(val fd: Int, buf: ByteBuffer, to: SocketAddress | Null) extends Command {
  def interest: Interest = Interest.Write

  private def peer: SocketAddress | Null = {
    val h = handle
    if h != null then h.peer else KqueueHandles.peerOf(fd)
  }

  override def refusal: Throwable | Null = {
    val connectedTo = peer // looked up once per handle
    if connectedTo == null then (if to == null then gears.async.io.Ops.notConnected() else null)
    else if to != null && to != connectedTo then gears.async.io.Ops.notThePeer(to, connectedTo)
    else if !buf.hasRemaining() then gears.async.io.Ops.emptyPacket(buf)
    else null
  }

  def perform(): Boolean = {
    val size = buf.remaining()
    val sent =
      if to == null || peer != null then PosixSockets.sendDatagram(fd, NativeBuffer.atPosition(buf), size)
      else
        KqueueHandles.withSocketAddr(KqueueHandles.targetFor(fd, to)) { (address, length) =>
          PosixSockets.sendDatagram(fd, NativeBuffer.atPosition(buf), size, address, length)
        }
    if sent then buf.position(buf.limit()) // a datagram goes whole or not at all
    sent
  }
}
