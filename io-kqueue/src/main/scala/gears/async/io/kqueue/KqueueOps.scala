package gears.async.io.kqueue

import gears.async.io.BufferPool
import gears.async.io.ByteStreams
import gears.async.io.Handles
import gears.async.io.Interface
import gears.async.io.IpSocketAddress
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.Received
import gears.async.io.Repeatable
import gears.async.io.Slot
import gears.async.io.SocketOption
import gears.async.io.SocketOptionOps
import gears.async.io.SocketOptions
import gears.async.io.StreamSockets
import gears.async.io.UnixAddress
import gears.async.io.UnixStreams
import gears.async.io.posix.PosixSocketOptions
import gears.async.io.shareable.Checks
import gears.async.io.shareable.HandleOwner
import gears.async.io.shareable.LendingPool

import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration

sealed trait KqueueInterface extends Interface {
  override type Op = KqueueOp

  override def timer(duration: FiniteDuration): KqueueTimerOp & Repeatable = {
    if duration.length < 0 then throw new IllegalArgumentException(s"A timer's time is not negative: $duration")
    KqueueMakeTimer(duration)
  }

  override def deadlineTimer(nanoTime: Long): KqueueDeadlineOp & Repeatable = KqueueMakeDeadline(nanoTime)
}

sealed trait KqueuePipeOps extends Pipes { self: KqueueByteStreams & KqueueInterface =>
  override def pipe(read: Slot[Readable], write: Slot[Writable]): KqueuePipeOp = KqueueMakePipe(read, write)
}

sealed trait KqueueHandles extends Handles { self: KqueueInterface =>
  override type Handle = KqueueHandle

  override def close(handle: KqueueHandle): KqueueCloseOp = KqueueCloseHandle(handle)
}

sealed trait KqueueByteStreams extends ByteStreams { self: KqueueInterface & KqueueHandles =>
  override type Readable = KqueueReadable
  override type Writable = KqueueWritable

  override def read(handle: Readable, buf: ByteBuffer): KqueueReadableOp & Repeatable =
    KqueueRead(handle, buf)

  override def write(handle: Writable, buf: ByteBuffer): KqueueWritableOp & Repeatable =
    KqueueWrite(handle, buf)

  override def read(handle: Readable, bufs: Array[ByteBuffer]): KqueueReadableOp & Repeatable =
    KqueueScatterRead(handle, bufs)

  override def write(handle: Writable, bufs: Array[ByteBuffer]): KqueueWritableOp & Repeatable =
    KqueueGatherWrite(handle, bufs)

  override def read(handle: Readable, pool: BufferPool, into: Slot[Received]): KqueueReadableOp & Repeatable =
    pool match
      case pool: LendingPool => KqueuePooledRead(handle, pool, into)
      case _ => throw new IllegalArgumentException("A pooled read takes a pool of this backend's (Backend.pool)")
}

/** Socket options: every one that applies to a kind of socket, IP or Unix, changed at any time but `ReusePort`, which
  * is set before the socket binds.
  */
sealed trait KqueueSocketOptions extends SocketOptionOps { self: KqueueInterface & KqueueHandles =>
  override def supports(option: SocketOption[?], kind: SocketOption.Kind): Boolean = option.appliesTo(kind)

  override def canChange(option: SocketOption[?], kind: SocketOption.Kind): Boolean =
    SocketOption.onSocket(option, kind) && option != SocketOption.ReusePort

  override def option[V](handle: KqueueHandle, option: SocketOption[V]): V =
    onSocket(handle, option)(socket => PosixSocketOptions.get(socket.fd, socket.ipv6, option).asInstanceOf[V])

  override def setOption[V](handle: KqueueHandle, option: SocketOption[V], value: V): Unit = {
    option.check(value)
    onSocket(handle, option) { socket =>
      if !canChange(option, socket.kind) then
        throw new IllegalArgumentException(s"$option is given as a socket opens, not changed")
      PosixSocketOptions.set(socket.fd, socket.ipv6, option, value)
    }
  }

  /** Runs `body` on `handle`, an open socket that `option` applies to; else an `IllegalArgumentException`. */
  private def onSocket[A](handle: KqueueHandle, option: SocketOption[?])(body: KqueueSocket => A): A = handle match
    case socket: KqueueSocket =>
      socket.claimWork()
      try
        if !SocketOption.onSocket(option, socket.kind) then throw Checks.notFor(option, socket.kind)
        body(socket)
      finally socket.releaseWork()
    case _ => throw new IllegalArgumentException(s"$option applies to sockets, not $handle")
}

sealed trait KqueueStreamSockets extends StreamSockets {
  self: KqueueInterface & KqueueHandles & KqueueByteStreams =>
  override type Connection = KqueueConnection
  override type Listener = KqueueListener

  override def accept(handle: KqueueListener, into: Slot[KqueueConnection]): KqueueAcceptOp & Repeatable =
    KqueueAccept(handle, into)

  override def acceptAll(
      handle: KqueueListener,
      into: Slot[Array[KqueueConnection]],
      max: Int
  ): KqueueAcceptOp & Repeatable = KqueueAcceptAll(handle, into, max)

  override def shutdownOutput(handle: KqueueConnection): KqueueOp = KqueueShutdownOutput(handle)

  override def localAddress(handle: KqueueListener | KqueueConnection): IpSocketAddress | UnixAddress =
    KqueueSockets.addressOf(handle, peer = false)

  override def peerAddress(handle: KqueueConnection): IpSocketAddress | UnixAddress =
    KqueueSockets.addressOf(handle, peer = true)
}

sealed trait KqueueIpStreams extends IpStreams {
  self: KqueueInterface & KqueueHandles & KqueueByteStreams & KqueueStreamSockets & KqueueSocketOptions =>

  override def connect(address: IpSocketAddress, options: SocketOptions, into: Slot[KqueueConnection]): KqueueOp = {
    Checks.checkOptions(options, supports, SocketOption.Kind.Connection)
    KqueueConnect(address, options, into)
  }

  override def listen(address: IpSocketAddress, options: SocketOptions, into: Slot[KqueueListener]): KqueueOp = {
    Checks.checkOptions(options, supports, SocketOption.Kind.Listener)
    KqueueListen(address, options, into)
  }
}

sealed trait KqueueUnixStreams extends UnixStreams {
  self: KqueueInterface & KqueueHandles & KqueueByteStreams & KqueueStreamSockets & KqueueSocketOptions =>

  override def connect(address: UnixAddress, options: SocketOptions, into: Slot[KqueueConnection]): KqueueOp = {
    Checks.checkOptions(options, supports, SocketOption.Kind.UnixConnection)
    KqueueConnect(Checks.unix(address), options, into)
  }

  override def listen(address: UnixAddress, options: SocketOptions, into: Slot[KqueueListener]): KqueueOp = {
    Checks.checkOptions(options, supports, SocketOption.Kind.UnixListener)
    KqueueListen(Checks.unix(address), options, into)
  }
}

object KqueueOps
    extends KqueueInterface,
      KqueueHandles,
      KqueueByteStreams,
      KqueuePipeOps,
      KqueueSocketOptions,
      KqueueStreamSockets,
      KqueueIpStreams,
      KqueueUnixStreams {}
