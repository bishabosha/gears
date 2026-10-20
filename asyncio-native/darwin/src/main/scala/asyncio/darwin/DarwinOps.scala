package asyncio.darwin

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.BlockingOps
import asyncio.Completion
import asyncio.HandleSlot
import asyncio.IpAddress
import asyncio.Ops
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.unsafe.BlockingRun
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixResolver
import asyncio.unsafe.TimerHeap
import DarwinCodes.*

/** Everything `DarwinReactor` can run: its `Op` member type. */
sealed trait DarwinOp {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[darwin] var submitted = false

  /** While the op is pending in the reactor: its completion, so the op is its own record and submitting it allocates
    * nothing. Taken off just before the completion runs, which may then submit the op again. Loop thread only.
    */
  private[darwin] var completion: Completion[DarwinOp] | Null = null

  /** For an op that failed as soon as it was submitted: its failure, until its completion runs. */
  private[darwin] var atOnceFailure: Throwable | Null = null

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[darwin] var attachment: AnyRef | Null = null
}

/** Builds the ops `DarwinReactor` understands. */
object DarwinOps extends Ops[DarwinOp] with BlockingOps[DarwinOp] {
  def read(handle: Int, buf: ByteBuffer): DarwinRead = new DarwinRead(handle, buf)
  def write(handle: Int, buf: ByteBuffer): DarwinWrite = new DarwinWrite(handle, buf)
  def accept(handle: Int, into: HandleSlot): DarwinAccept = new DarwinAccept(handle, into)
  def connect(handle: Int): DarwinOp = new DarwinConnect(handle)
  def receive(handle: Int, buf: ByteBuffer, from: Slot[IpAddress]): DarwinReceive = new DarwinReceive(handle, buf, from)
  def send(handle: Int, buf: ByteBuffer, to: IpAddress): DarwinSend = new DarwinSend(handle, buf, to)
  def send(handle: Int, buf: ByteBuffer): DarwinSend = new DarwinSend(handle, buf, null)
  def timer(milliseconds: Int): DarwinTimer = new DarwinTimer(milliseconds)
  def blocking(body: () => Unit): DarwinBlocking = new DarwinBlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): DarwinBlocking = new DarwinBlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): DarwinOp = new DarwinResolve(host, into)
}

/** An op on a handle. `attempt` tries to finish it on the reactor's thread: true once done, with its results in the
  * caller's buffers and slots; false when it has to wait, a waiter for it registered on the handle's C side with the
  * reactor and the submission's token, which notifies the reactor to attempt again; a failure is thrown. Commands are
  * `Repeatable`: attempting again is always meaningful.
  */
sealed abstract class DarwinCommand extends DarwinOp with Repeatable {
  def handle: Int

  /** What the command waits for, as the C side's kinds of wait. */
  def kind: Int

  /** The current submission's token, and the handle it resolved to when submitted, kept for its attempts since the
    * number may be closed and reused meanwhile. Loop thread only.
    */
  private[darwin] var token = 0L
  private[darwin] var target: DarwinHandle | Null = null

  private[darwin] def attempt(reactor: DarwinReactor): Boolean

  /** Turns a C code into the outcome of an attempt: false for waiting, and a throw otherwise. */
  protected final def waitOrFail(code: Int, what: String): Boolean = code match {
    case Waiting => false
    case Closed  => throw new IOException(s"Handle $handle was closed")
    case Busy    => throw new IllegalStateException(s"A $what is already pending on handle $handle")
    case Error   => throw new IOException(s"$what failed on handle $handle: ${DarwinHandles.errorText(DarwinIo.dn_handle_error(target.nn.ptr))}")
    case other   => throw new IOException(s"$what failed on handle $handle with code $other")
  }
}

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream. */
final class DarwinRead(val handle: Int, buf: ByteBuffer) extends DarwinCommand {
  def kind: Int = Read

  private[darwin] def attempt(reactor: DarwinReactor): Boolean = {
    val h = target.nn
    if h.isFile || h.isDescriptor then {
      val read = NonBlocking.read(h.fd, buf) // -1 at EOF, which leaves the buffer as it was
      if read != 0 || h.isFile then true
      else waitOrFail(DarwinIo.dn_fd_wait(h.ptr, 1, reactor.ptr, token), "read")
    } else {
      val code = DarwinIo.dn_read(h.ptr, NativeBuffer.atPosition(buf), buf.remaining(), reactor.ptr, token)
      if code > 0 then {
        NativeBuffer.advance(buf, code)
        true
      } else if code == Eof then true
      else waitOrFail(code, "read")
    }
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written: for a connection, once the
  * send of what remained, copied, has been processed by the stack.
  */
final class DarwinWrite(val handle: Int, buf: ByteBuffer) extends DarwinCommand {
  def kind: Int = Write
  private var sent = false // the bytes were handed to the C side, which notifies once they are processed

  private[darwin] def attempt(reactor: DarwinReactor): Boolean = {
    val h = target.nn
    if h.isFile || h.isDescriptor then {
      if NonBlocking.write(h.fd, buf) >= 0 || !buf.hasRemaining() || h.isFile then true
      else waitOrFail(DarwinIo.dn_fd_wait(h.ptr, 0, reactor.ptr, token), "write")
    } else if sent then {
      sent = false
      val code = DarwinIo.dn_write_outcome(h.ptr)
      if code > 0 then true else waitOrFail(code, "write")
    } else if !buf.hasRemaining() then true
    else {
      val code = DarwinIo.dn_write(h.ptr, NativeBuffer.atPosition(buf), buf.remaining(), reactor.ptr, token)
      if code > 0 then {
        NativeBuffer.advance(buf, code)
        sent = true
        false
      } else waitOrFail(code, "write")
    }
  }

  private[darwin] def reset(): Unit = sent = false
}

/** Accepts one connection on a listener, writing its handle into `into`. */
final class DarwinAccept(val handle: Int, into: HandleSlot) extends DarwinCommand {
  def kind: Int = Read

  private[darwin] def attempt(reactor: DarwinReactor): Boolean = {
    val code = stackalloc[CInt]()
    val accepted = DarwinIo.dn_accept(target.nn.ptr, reactor.ptr, token, code)
    if accepted != null then {
      into.set(DarwinHandles.accepted(accepted))
      true
    } else waitOrFail(!code, "accept")
  }
}

/** Waits for a connection to be ready; fails with the connection's error if it failed. */
final class DarwinConnect(val handle: Int) extends DarwinCommand {
  def kind: Int = Connect

  private[darwin] def attempt(reactor: DarwinReactor): Boolean = {
    val code = DarwinIo.dn_wait_connected(target.nn.ptr, reactor.ptr, token)
    if code > 0 then true else waitOrFail(code, "connect")
  }
}

/** Receives one datagram into `buf`, which is left flipped for reading, and writes its sender into `from`. */
final class DarwinReceive(val handle: Int, buf: ByteBuffer, from: Slot[IpAddress]) extends DarwinCommand {
  def kind: Int = Read

  private[darwin] def attempt(reactor: DarwinReactor): Boolean = {
    val h = target.nn
    val storage = stackalloc[socket.sockaddr_storage]()
    val length = stackalloc[socket.socklen_t]()
    !length = 0.toUInt
    buf.clear()
    val code =
      DarwinIo.dn_receive_from(h.ptr, NativeBuffer.atPosition(buf), buf.remaining(), storage, length, reactor.ptr, token)
    if code >= 0 then {
      NativeBuffer.advance(buf, code)
      buf.flip()
      val sender: IpAddress =
        if (!length).toInt > 0 then DarwinHandles.decode(storage, !length)
        else if h.remote != null then h.remote.nn
        else throw new IOException("Received a datagram from an unknown sender")
      from.set(sender)
      true
    } else waitOrFail(code, "receive")
  }
}

/** Sends `buf` as one datagram, to `to` or to the connected peer when `to` is null, consuming `buf`. */
final class DarwinSend(val handle: Int, buf: ByteBuffer, to: IpAddress | Null) extends DarwinCommand {
  def kind: Int = Write
  private var sent = false

  private[darwin] def attempt(reactor: DarwinReactor): Boolean = {
    val h = target.nn
    if sent then {
      sent = false
      val code = DarwinIo.dn_write_outcome(h.ptr)
      if code > 0 then true else waitOrFail(code, "send")
    } else {
      val size = buf.remaining()
      val code = to match {
        case null => DarwinIo.dn_write(h.ptr, NativeBuffer.atPosition(buf), size, reactor.ptr, token)
        case IpAddress.IPv4(host, port) => sendTo(h, host, port, size, reactor)
        case IpAddress.IPv6(host, port) => sendTo(h, host, port, size, reactor)
      }
      if code >= 0 then {
        buf.position(buf.limit()) // a datagram goes whole or not at all
        sent = true
        false
      } else waitOrFail(code, "send")
    }
  }

  private def sendTo(h: DarwinHandle, host: String, port: Int, size: Int, reactor: DarwinReactor): Int =
    Zone.acquire { implicit z =>
      DarwinIo.dn_send_to(h.ptr, NativeBuffer.atPosition(buf), size, toCString(host), port, reactor.ptr, token)
    }

  private[darwin] def reset(): Unit = sent = false
}

/** A one-shot timer, kept in the reactor's heap while pending. */
final class DarwinTimer(val milliseconds: Int) extends DarwinOp with Repeatable with TimerHeap.Entry {
  require(milliseconds >= 0, "Timer duration must not be negative")
}

/** A blocking task run on the blocker pool (see `BlockingRun`); once it has returned, the reactor it was submitted to
  * runs its completion on the loop thread.
  */
sealed abstract class DarwinBlocking extends DarwinOp with BlockingRun {
  @volatile private[darwin] var reactor: DarwinReactor | Null = null
  @volatile private[darwin] var failure: Throwable | Null = null

  /** The cancelled submission whose `onCancel` waits for `block` to return. Loop thread only. */
  private[darwin] var cancelling: Completion[DarwinOp] | Null = null

  protected def settled(failure: Throwable | Null): Unit = {
    this.failure = failure
    reactor.nn.resolved(this)
  }
}

final class DarwinBlockingTask(body: () => Unit) extends DarwinBlocking {
  def block(): Unit = body()
}

final class DarwinBlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends DarwinBlocking {
  def block(): Unit = publish(into, body())
}

/** Resolves a host name on the blocker pool, as kqueue's reactor does: Network.framework's public C interface has no
  * resolver of its own, only connections by host name.
  */
final class DarwinResolve(host: String, into: Slot[List[ResolvedAddress]]) extends DarwinBlocking {
  def block(): Unit = PosixResolver.lookup(host) match {
    case Right(addresses) => publish(into, addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}
