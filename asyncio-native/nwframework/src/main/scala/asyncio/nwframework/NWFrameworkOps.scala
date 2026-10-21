package asyncio.nwframework

import java.io.IOException
import java.nio.ByteBuffer

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

/** Everything `NWFrameworkReactor` can run: its `Op` member type. */
sealed trait NWFrameworkOp {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[nwframework] var submitted = false

  /** While the op is pending in the reactor: its completion, so the op is its own record and submitting it allocates
    * nothing. Taken off just before the completion runs, which may then submit the op again. Loop thread only.
    */
  private[nwframework] var completion: Completion[NWFrameworkOp] | Null = null

  /** For an op that failed as soon as it was submitted: its failure, until its completion runs. */
  private[nwframework] var atOnceFailure: Throwable | Null = null

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[nwframework] var attachment: AnyRef | Null = null
}

/** Builds the ops `NWFrameworkReactor` understands. */
object NWFrameworkOps extends Ops[NWFrameworkOp] with BlockingOps[NWFrameworkOp] {
  def read(handle: Int, buf: ByteBuffer): NWFrameworkRead = new NWFrameworkRead(handle, buf)
  def write(handle: Int, buf: ByteBuffer): NWFrameworkWrite = new NWFrameworkWrite(handle, buf)
  def accept(handle: Int, into: HandleSlot): NWFrameworkAccept = new NWFrameworkAccept(handle, into)
  def connect(handle: Int): NWFrameworkOp = new NWFrameworkConnect(handle)
  def receive(handle: Int, buf: ByteBuffer, from: Slot[IpAddress]): NWFrameworkReceive = new NWFrameworkReceive(handle, buf, from)
  def send(handle: Int, buf: ByteBuffer, to: IpAddress): NWFrameworkSend = new NWFrameworkSend(handle, buf, to)
  def send(handle: Int, buf: ByteBuffer): NWFrameworkSend = new NWFrameworkSend(handle, buf, null)
  def timer(milliseconds: Int): NWFrameworkTimer = new NWFrameworkTimer(milliseconds)
  def blocking(body: () => Unit): NWFrameworkBlocking = new NWFrameworkBlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NWFrameworkBlocking = new NWFrameworkBlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): NWFrameworkOp = new NWFrameworkResolve(host, into)
}

/** An op on a handle. `attempt` tries to finish it on the reactor's thread, after the handle has taken in what the
  * framework delivered: true once done, with its results in the caller's buffers and slots; false when it has to
  * wait, registered on the handle with the reactor and the submission's token, so that the handle tells the reactor
  * to attempt again; a failure is thrown. Commands are `Repeatable`: attempting again is always meaningful.
  */
sealed abstract class NWFrameworkCommand extends NWFrameworkOp with Repeatable {
  def handle: Int

  /** What the command waits for (`NWFrameworkHandle.Read`, `Write` or `Connect`). */
  def kind: Int

  /** The current submission's token, and the handle it resolved to when submitted, kept for its attempts since the
    * number may be closed and reused meanwhile. Loop thread only.
    */
  private[nwframework] var token = 0L
  private[nwframework] var target: NWFrameworkHandle | Null = null

  private[nwframework] def attempt(reactor: NWFrameworkReactor, drainFirst: Boolean): Boolean = {
    val h = target.nn
    if drainFirst && !h.isFile then h.drain(reactor.event)
    attemptOn(h, reactor)
  }

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean
}

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream. */
final class NWFrameworkRead(val handle: Int, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if h.isFile || h.isDescriptor then {
      val read = NonBlocking.read(h.fd, buf) // -1 at EOF, which leaves the buffer as it was
      if read != 0 || h.isFile then true
      else h.armed(read = true, reactor, token)
    } else {
      val n = h.read(NativeBuffer.atPosition(buf), buf.remaining(), reactor, token)
      if n > 0 then NativeBuffer.advance(buf, n)
      n >= 0
    }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written: for a connection, once what
  * remained has been copied and sent, which is at once while the handle's outstanding sends are within its window,
  * and otherwise once a send has been processed by the stack (see `NWFrameworkHandle.send`).
  */
final class NWFrameworkWrite(val handle: Int, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Write
  private var sent = false // the bytes were handed over, and the handle tells once a send is processed

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if h.isFile || h.isDescriptor then {
      if NonBlocking.write(h.fd, buf) >= 0 || !buf.hasRemaining() || h.isFile then true
      else h.armed(read = false, reactor, token)
    } else if sent then {
      sent = false
      h.sent()
    } else if !buf.hasRemaining() then true
    else {
      val remaining = buf.remaining()
      val done = h.send(NativeBuffer.atPosition(buf), remaining, reactor, token)
      NativeBuffer.advance(buf, remaining)
      sent = !done
      done
    }

  private[nwframework] def reset(): Unit = sent = false
}

/** Accepts one connection on a listener, writing its handle into `into`. */
final class NWFrameworkAccept(val handle: Int, into: HandleSlot) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    val accepted = h.accept(reactor, token)
    if accepted == null then false
    else {
      into.set(NWFrameworkHandles.accepted(accepted))
      true
    }
  }
}

/** Waits for a connection to be ready; fails with the connection's error if it failed. */
final class NWFrameworkConnect(val handle: Int) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Connect

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = h.connected(reactor, token)
}

/** Receives one datagram into `buf`, which is left flipped for reading, and writes its sender into `from`. */
final class NWFrameworkReceive(val handle: Int, buf: ByteBuffer, from: Slot[IpAddress]) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    buf.clear()
    val packet = h.receivePacket(reactor, token)
    if packet == null then false
    else {
      val n = math.min(packet.bytes.length, buf.remaining()) // truncated beyond the buffer, as a datagram is
      buf.put(packet.bytes, 0, n)
      buf.flip()
      from.set(packet.from)
      true
    }
  }
}

/** Sends `buf` as one datagram, to `to` or to the connected peer when `to` is null, consuming `buf`. */
final class NWFrameworkSend(val handle: Int, buf: ByteBuffer, to: IpAddress | Null) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Write
  private var sent = false

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if sent then {
      sent = false
      h.sent()
    } else {
      val size = buf.remaining()
      val bytes = NativeBuffer.atPosition(buf)
      val done = if to == null then h.send(bytes, size, reactor, token) else h.sendTo(to.nn, bytes, size, reactor, token)
      buf.position(buf.limit()) // a datagram goes whole or not at all
      sent = !done
      done
    }

  private[nwframework] def reset(): Unit = sent = false
}

/** A one-shot timer, kept in the reactor's heap while pending. */
final class NWFrameworkTimer(val milliseconds: Int) extends NWFrameworkOp with Repeatable with TimerHeap.Entry {
  require(milliseconds >= 0, "Timer duration must not be negative")
}

/** A blocking task run on the blocker pool (see `BlockingRun`); once it has returned, the reactor it was submitted to
  * runs its completion on the loop thread.
  */
sealed abstract class NWFrameworkBlocking extends NWFrameworkOp with BlockingRun {
  @volatile private[nwframework] var reactor: NWFrameworkReactor | Null = null
  @volatile private[nwframework] var failure: Throwable | Null = null

  /** The cancelled submission whose `onCancel` waits for `block` to return. Loop thread only. */
  private[nwframework] var cancelling: Completion[NWFrameworkOp] | Null = null

  protected def settled(failure: Throwable | Null): Unit = {
    this.failure = failure
    reactor.nn.resolved(this)
  }
}

final class NWFrameworkBlockingTask(body: () => Unit) extends NWFrameworkBlocking {
  def block(): Unit = body()
}

final class NWFrameworkBlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends NWFrameworkBlocking {
  def block(): Unit = publish(into, body())
}

/** Resolves a host name on the blocker pool, as kqueue's reactor does: Network.framework's public C interface has no
  * resolver of its own, only connections by host name.
  */
final class NWFrameworkResolve(host: String, into: Slot[List[ResolvedAddress]]) extends NWFrameworkBlocking {
  def block(): Unit = PosixResolver.lookup(host) match {
    case Right(addresses) => publish(into, addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}
