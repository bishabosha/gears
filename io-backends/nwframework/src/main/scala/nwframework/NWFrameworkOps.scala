package asyncio.nwframework

import java.io.IOException
import java.nio.ByteBuffer

import asyncio.BlockingOps
import asyncio.BufferPool
import asyncio.Received
import asyncio.Completion
import asyncio.FileOps
import asyncio.Handle
import asyncio.IpAddress
import asyncio.Repeatable
import asyncio.ResolveOps
import asyncio.ResolvedAddress
import asyncio.SocketOps
import asyncio.Slot
import asyncio.unsafe.BlockingRun
import asyncio.unsafe.NativePool
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixResolver
import asyncio.unsafe.TimerHeap
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Everything `NWFrameworkReactor` can run: its `Op` member type. */
sealed trait NWFrameworkOp extends TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

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
object NWFrameworkOps
    extends SocketOps[NWFrameworkOp]
    with BlockingOps[NWFrameworkOp]
    with FileOps[NWFrameworkOp]
    with ResolveOps[NWFrameworkOp] {
  def read(handle: Handle, buf: ByteBuffer): NWFrameworkRead = new NWFrameworkRead(handle, buf)
  def write(handle: Handle, buf: ByteBuffer): NWFrameworkWrite = new NWFrameworkWrite(handle, buf)
  def accept(handle: Handle, into: Handle.Slot): NWFrameworkAccept = new NWFrameworkAccept(handle, into)
  def connect(handle: Handle): NWFrameworkOp = new NWFrameworkConnect(handle)
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]): NWFrameworkReceive =
    new NWFrameworkReceive(handle, buf, from)
  def send(handle: Handle, buf: ByteBuffer, to: IpAddress): NWFrameworkSend = new NWFrameworkSend(handle, buf, to)
  def send(handle: Handle, buf: ByteBuffer): NWFrameworkSend = new NWFrameworkSend(handle, buf, null)
  def timer(milliseconds: Int): NWFrameworkTimer = new NWFrameworkTimer(milliseconds)
  def blocking(body: () => Unit): NWFrameworkBlocking = new NWFrameworkBlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NWFrameworkBlocking = new NWFrameworkBlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): NWFrameworkOp = new NWFrameworkResolve(host, into)
  def read(handle: Handle, pool: BufferPool, into: Slot[Received]): NWFrameworkPooledRead =
    new NWFrameworkPooledRead(handle, NWFrameworkOps.native(pool), into)
  def listening(handle: Handle): NWFrameworkOp = new NWFrameworkListening(handle)
  def close(handle: Handle): NWFrameworkOp = new NWFrameworkClose(handle)
  def open(path: String, write: Boolean, into: Handle.Slot): NWFrameworkOp = new NWFrameworkOpenFile(path, write, into)
  def read(handle: Handle, buf: ByteBuffer, offset: Long): NWFrameworkPositionalRead =
    new NWFrameworkPositionalRead(handle, buf, offset)
  def write(handle: Handle, buf: ByteBuffer, offset: Long): NWFrameworkPositionalWrite =
    new NWFrameworkPositionalWrite(handle, buf, offset)
  def sync(handle: Handle): NWFrameworkFileSync = new NWFrameworkFileSync(handle)

  private def native(pool: BufferPool): NativePool = pool match {
    case native: NativePool => native
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op on a handle. `attempt` tries to finish it on the reactor's thread, after the handle has taken in what the
  * framework delivered: true once done, with its results in the caller's buffers and slots; false when it has to
  * wait, registered on the handle with the reactor and the submission's token, so that the handle tells the reactor
  * to attempt again; a failure is thrown. Commands are `Repeatable`: attempting again is always meaningful.
  */
sealed abstract class NWFrameworkCommand extends NWFrameworkOp with Repeatable {
  def handle: Handle

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
final class NWFrameworkRead(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
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

/** Fills a buffer of `pool`'s, writing it into `into` (see `PooledReadOps.read`): a descriptor is read into one as
  * into the caller's buffer; a connection receives straight into one.
  */
final class NWFrameworkPooledRead(val handle: Handle, pool: NativePool, into: Slot[Received])
    extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if h.isFile || h.isDescriptor then {
      val slot = pool.take()
      if slot == null then throw new IOException("The pool has no buffer free")
      val buffer = slot.nn.buffer
      val read =
        try NonBlocking.read(h.fd, buffer) // -1 at EOF, which leaves the buffer as it was
        catch {
          case failure: Throwable =>
            slot.nn.release() // a failed read holds nothing
            throw failure
        }
      if read != 0 || h.isFile then {
        buffer.flip()
        into.set(slot.nn)
        true
      } else {
        slot.nn.release()
        h.armed(read = true, reactor, token)
      }
    } else {
      val received = h.readPooled(pool, reactor, token)
      if received == null then false
      else {
        into.set(received.nn)
        true
      }
    }
}

/** Completes once a listener from `handles.listener` is listening, or fails (see `SocketOps.listening`); a bound
  * datagram socket was bound when it was made, and a connected one is ready with its connection.
  */
final class NWFrameworkListening(val handle: Handle) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Connect

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    h.kind == NWFrameworkHandle.BoundDatagram || h.connected(reactor, token)
}

/** Closes a handle, completing once it is closed (see `Ops.close`): a connection's cancellation is waited
  * for, and the number is taken back once the op is done, so that the reactor still finds the handle for the
  * cancelled state meanwhile; ops submitted on it meanwhile fail as closed.
  */
final class NWFrameworkClose(val handle: Handle) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Close
  private var begun = false

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    val done =
      if begun then h.closeDone
      else {
        begun = true
        h.closeAsync(reactor, token) // throws if already closed, so the op fails
      }
    if done then NWFrameworkHandles.remove(handle)
    done
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written: for a connection, once what
  * remained has been copied and sent, which is at once while the handle's outstanding sends are within its window,
  * and otherwise once a send has been processed by the stack (see `NWFrameworkHandle.send`).
  */
final class NWFrameworkWrite(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
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
final class NWFrameworkAccept(val handle: Handle, into: Handle.Slot) extends NWFrameworkCommand {
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
final class NWFrameworkConnect(val handle: Handle) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Connect

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = h.connected(reactor, token)
}

/** Receives one datagram into `buf`, which is left flipped for reading, and writes its sender into `from`. */
final class NWFrameworkReceive(val handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]) extends NWFrameworkCommand {
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
final class NWFrameworkSend(val handle: Handle, buf: ByteBuffer, to: IpAddress | Null) extends NWFrameworkCommand {
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

/** A one-shot timer, kept in the reactor's heap while pending (`TimerHeap.Entry`). */
final class NWFrameworkTimer(val milliseconds: Int) extends NWFrameworkOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  /** Whether the pending submission's time limit is shorter than the timer, so it fails when due. Loop thread only. */
  private[nwframework] var expires = false
}

/** A blocking task run on the blocker pool (see `BlockingRun`); once it has returned, the reactor it was submitted to
  * runs its completion on the loop thread. One marked `Repeatable`, such as a file op, is restarted on each submission.
  */
sealed abstract class NWFrameworkBlocking extends NWFrameworkOp with BlockingRun {
  @volatile private[nwframework] var reactor: NWFrameworkReactor | Null = null
  @volatile private[nwframework] var failure: Throwable | Null = null

  /** The cancelled submission whose `onCancel` waits for `block` to return, or whose `onFailure` does, with
    * `cancelledBy`, for one whose time limit ran out. Loop thread only.
    */
  private[nwframework] var cancelling: Completion[NWFrameworkOp] | Null = null
  private[nwframework] var cancelledBy: Throwable | Null = null

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

/** Opens a regular file on the blocker pool and publishes its handle (see `FileOps.open`). A cancel that arrives
  * once the file is open closes it again, since the handle never reaches the caller.
  */
final class NWFrameworkOpenFile(path: String, write: Boolean, into: Handle.Slot) extends NWFrameworkBlocking {
  def block(): Unit = {
    val handle = NWFrameworkHandles.openFile(path, write)
    if !unlessCancelled(into.set(handle)) then NWFrameworkHandles.close(handle)
  }
}

/** A file op on the blocker pool, on the descriptor of a regular file: the handle is resolved when the task runs, so
  * that one closed meanwhile fails the op.
  */
sealed abstract class NWFrameworkFileTask(val handle: Handle) extends NWFrameworkBlocking with Repeatable {
  protected def fd: Int = {
    val h = NWFrameworkHandles.get(handle)
    if !h.isFile then throw new IOException(s"Handle $handle is not a regular file")
    h.fd
  }
}

/** A read at an offset (see `FileOps.read`): `pread` through posixlib's `@blocking` binding, at the op's offset,
  * which each completion advances by the bytes read.
  */
final class NWFrameworkPositionalRead(handle: Handle, buf: ByteBuffer, private var offset: Long)
    extends NWFrameworkFileTask(handle) {
  def block(): Unit = {
    val descriptor = fd
    val read = unistd.pread(descriptor, NativeBuffer.atPosition(buf), buf.remaining().toCSize, offset.toSize)
    if read < 0 then throw new IOException(s"Failed to read descriptor $descriptor at $offset: ${cError()}")
    val n = read.toInt
    unlessCancelled {
      NativeBuffer.advance(buf, n)
      offset += n
    }
  }
}

/** A write at an offset (see `FileOps.write`): `pwrite` at the op's offset, which each completion advances. */
final class NWFrameworkPositionalWrite(handle: Handle, buf: ByteBuffer, private var offset: Long)
    extends NWFrameworkFileTask(handle) {
  def block(): Unit = {
    val descriptor = fd
    val written = unistd.pwrite(descriptor, NativeBuffer.atPosition(buf), buf.remaining().toCSize, offset.toSize)
    if written < 0 then throw new IOException(s"Failed to write descriptor $descriptor at $offset: ${cError()}")
    val n = written.toInt
    unlessCancelled {
      NativeBuffer.advance(buf, n)
      offset += n
    }
  }
}

/** `fsync` (see `FileOps.sync`). */
final class NWFrameworkFileSync(handle: Handle) extends NWFrameworkFileTask(handle) {
  def block(): Unit = {
    val descriptor = fd
    if unistd.fsync(descriptor) < 0 then throw new IOException(s"Failed to sync descriptor $descriptor: ${cError()}")
  }
}
