package gears.async.io
package nwframework

import gears.async.io.BlockingOps
import gears.async.io.BufferPool
import gears.async.io.Completion
import gears.async.io.FifoOps
import gears.async.io.FileOps
import gears.async.io.IpAddress
import gears.async.io.PipeOps
import gears.async.io.Received
import gears.async.io.Repeatable
import gears.async.io.ResolveOps
import gears.async.io.ResolvedAddress
import gears.async.io.Slot
import gears.async.io.SocketOps
import gears.async.io.internal.BlockingRun
import gears.async.io.internal.NativeBuffer
import gears.async.io.internal.NativePool
import gears.async.io.internal.NonBlocking
import gears.async.io.internal.PosixErr.cError
import gears.async.io.internal.PosixResolver
import gears.async.io.internal.TimerHeap

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import NWFrameworkBackend.Handle

/** Everything `NWFrameworkReactor` can run: its `Op` member type. */
sealed trait NWFrameworkOp extends TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

  // Whether this op has been submitted before, so a one-shot op is run once though submitted on two threads at once.
  private val submissions = new java.util.concurrent.atomic.AtomicBoolean()

  /** Records a submission, refusing a one-shot op submitted before with `IllegalStateException`. */
  private[nwframework] final def markSubmitted(): Unit =
    if submissions.getAndSet(true) && oneShot then
      throw new IllegalStateException(s"$this is not Repeatable and was already submitted")

  /** While the op is pending in the reactor: its completion, so the op is its own record and submitting it allocates
    * nothing. Taken off just before the completion runs, which may then submit the op again. Loop thread only.
    */
  private[nwframework] var completion: Completion[NWFrameworkOp] | Null = null

  /** For an op that failed as soon as it was submitted: its failure, until its completion runs. */
  private[nwframework] var atOnceFailure: Throwable | Null = null

  /** The failure of an op submitted with a slot still holding a value (`Ops.notEmpty`), which fails it at once, before
    * it does anything; null if its slots are empty.
    */
  private[nwframework] def slotRefusal: Throwable | Null = null

  /** The claim this op takes on its handle while pending (`Handle.reader` or `writer`); null for one that takes none.
    */
  private[nwframework] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null = null

  /** Whether it may be submitted only once: an op that is not `Repeatable`, or a command that opens or closes. */
  private[nwframework] def oneShot: Boolean = !this.isInstanceOf[Repeatable]

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[nwframework] var attachment: AnyRef | Null = null
}

/** Builds the ops `NWFrameworkReactor` understands. */
object NWFrameworkOps
    extends SocketOps[NWFrameworkOp, Handle]
    with PipeOps[NWFrameworkOp, Handle]
    with FifoOps[NWFrameworkOp, Handle]
    with BlockingOps[NWFrameworkOp, Handle]
    with FileOps[NWFrameworkOp, Handle]
    with ResolveOps[NWFrameworkOp, Handle] {
  type Readable = NWFrameworkKinds.Readable
  type Writable = NWFrameworkKinds.Writable
  type Connection = NWFrameworkKinds.Connection
  type Listener = NWFrameworkKinds.Listener
  type Datagram = NWFrameworkKinds.Datagram
  type UnixDatagram = NWFrameworkKinds.UnixDatagram
  type File = NWFrameworkKinds.File

  def read(handle: Readable, buf: ByteBuffer): NWFrameworkRead = new NWFrameworkRead(handle, buf)
  def write(handle: Writable, buf: ByteBuffer): NWFrameworkWrite = new NWFrameworkWrite(handle, buf)
  def accept(handle: Listener, into: Slot[Connection]): NWFrameworkAccept = new NWFrameworkAccept(handle, into)
  def acceptAll(handle: Listener, into: Slot[Array[Connection]], max: Int): NWFrameworkAcceptAll = {
    require(max > 0, s"max must be positive, not $max")
    new NWFrameworkAcceptAll(handle, into, max)
  }
  def connect(address: IpAddress, into: Slot[Connection]): NWFrameworkOp =
    new NWFrameworkConnectTo(gears.async.io.Ops.ip(address), into)
  def listen(address: IpAddress, into: Slot[Listener]): NWFrameworkOp =
    new NWFrameworkListen(gears.async.io.Ops.ip(address), into)
  def datagram(local: IpAddress | Null, remote: IpAddress | Null, into: Slot[Datagram]): NWFrameworkOp = {
    val (l, r) = gears.async.io.Ops.ipPair(local, remote)
    new NWFrameworkOpenDatagram(l, r, into)
  }
  def pipe(read: Slot[Readable], write: Slot[Writable]): NWFrameworkOp = new NWFrameworkOpenPipe(read, write)
  def openFifoForReading(path: String, into: Slot[Readable]): NWFrameworkOp =
    new NWFrameworkOpenFifo(path, write = false, into, NWFrameworkKinds.readable)
  def openFifoForWriting(path: String, into: Slot[Writable]): NWFrameworkOp =
    new NWFrameworkOpenFifo(path, write = true, into, NWFrameworkKinds.writable)
  def receive(handle: Datagram, buf: ByteBuffer, from: Slot[IpAddress]): NWFrameworkReceive =
    new NWFrameworkReceive(handle, buf, from)
  def send(handle: Datagram, buf: ByteBuffer, to: IpAddress): NWFrameworkSend =
    new NWFrameworkSend(handle, buf, gears.async.io.Ops.ip(to))
  def send(handle: Datagram, buf: ByteBuffer): NWFrameworkSend = new NWFrameworkSend(handle, buf, null)
  def timer(milliseconds: Int): NWFrameworkTimer = new NWFrameworkTimer(milliseconds)
  def blocking(body: () => Unit): NWFrameworkBlocking = new NWFrameworkBlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NWFrameworkBlocking =
    new NWFrameworkBlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): NWFrameworkOp = new NWFrameworkResolve(host, into)
  def read(handle: Readable, pool: BufferPool, into: Slot[Received]): NWFrameworkPooledRead =
    new NWFrameworkPooledRead(handle, NWFrameworkOps.native(pool), into)
  def close(handle: Handle): NWFrameworkOp = new NWFrameworkClose(handle)
  def shutdownOutput(handle: Connection): NWFrameworkOp = new NWFrameworkShutdownOutput(handle)
  def open(path: String, write: Boolean, into: Slot[File]): NWFrameworkOp = new NWFrameworkOpenFile(path, write, into)
  def read(handle: File, buf: ByteBuffer, offset: Long): NWFrameworkPositionalRead =
    new NWFrameworkPositionalRead(handle, buf, offset)
  def write(handle: File, buf: ByteBuffer, offset: Long): NWFrameworkPositionalWrite =
    new NWFrameworkPositionalWrite(handle, buf, offset)
  def sync(handle: File): NWFrameworkFileSync = new NWFrameworkFileSync(handle)

  private def native(pool: BufferPool): NativePool = pool match {
    case native: NativePool => native
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op on a handle. `attempt` tries to finish it on the reactor's thread, after the handle has taken in what the
  * framework delivered: true once done, with its results in the caller's buffers and slots; false when it has to wait,
  * registered on the handle with the reactor and the submission's token, so that the handle tells the reactor to
  * attempt again; a failure is thrown. Commands are `Repeatable`: attempting again is always meaningful.
  */
sealed abstract class NWFrameworkCommand extends NWFrameworkOp with Repeatable {
  def handle: Handle

  /** What the command waits for (`NWFrameworkHandle.Read`, `Write`, `Connect` or `Close`). */
  def kind: Int

  override private[nwframework] def claim: java.util.concurrent.atomic.AtomicReference[AnyRef | Null] | Null =
    if kind == NWFrameworkHandle.Read then handle.reader
    else if kind == NWFrameworkHandle.Write then handle.writer
    else null

  /** The current submission's token, and the handle it resolved to when submitted, kept for its attempts since the
    * handle may be closed meanwhile. Loop thread only.
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

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream; fails if
  * `buf` has no room, since a read of nothing would look like the end of the stream.
  */
final class NWFrameworkRead(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if !buf.hasRemaining() then throw gears.async.io.Ops.bufferFull(buf)
    else if h.isFile || h.isDescriptor then {
      val read = h.using(NonBlocking.read(_, buf)) // -1 at EOF, which leaves the buffer as it was
      if read != 0 || h.isFile then true
      else h.armed(read = true, reactor, token)
    } else {
      val n = h.read(NativeBuffer.atPosition(buf), buf.remaining(), reactor, token)
      if n > 0 then NativeBuffer.advance(buf, n)
      n >= 0
    }
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read`): a descriptor is read into one as into the
  * caller's buffer; a connection receives straight into one.
  */
final class NWFrameworkPooledRead(val handle: Handle, pool: NativePool, into: Slot[Received])
    extends NWFrameworkCommand {
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if h.isFile || h.isDescriptor then {
      val slot = pool.take()
      if slot == null then throw new IOException("The pool has no buffer free")
      val buffer = slot.nn.buffer
      val read =
        try h.using(NonBlocking.read(_, buffer)) // -1 at EOF, which leaves the buffer as it was
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

/** Accepts the connections waiting on a listener, at most `max`, writing their handles into `into`. */
final class NWFrameworkAcceptAll(val handle: Handle, into: Slot[Array[NWFrameworkKinds.Connection]], max: Int)
    extends NWFrameworkCommand {
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    val accepted = h.acceptAll(max, reactor, token)
    if accepted == null then false
    else {
      into.set(accepted.map(a => NWFrameworkKinds.connection(NWFrameworkHandles.accepted(a))))
      true
    }
  }
}

/** A command that opens the handle it waits on, and writes it into a slot (see `Ops`): `open` makes the handle when the
  * command is submitted, starting what Network.framework does asynchronously, a connection or a listener, and the
  * command then waits for it to be ready (`ready`). Done once it is, after writing the handle into `into`; if it fails,
  * or the submission is cancelled first, by `cancel`, a time limit or the reactor closing, `abandon` closes the handle
  * and leaves `into` empty.
  */
sealed abstract class NWFrameworkOpening[K <: Handle](into: Slot[K], kindOf: Handle => K) extends NWFrameworkCommand {
  override private[nwframework] def oneShot: Boolean = true // it opens its handle
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  private var opened: Handle | Null = null
  private var delivered = false
  def kind: Int = NWFrameworkHandle.Connect
  def handle: Handle = opened.nn

  /** Makes the handle. */
  protected def make(): Handle

  /** Whether the handle is ready, once made. */
  protected def ready(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean

  /** Makes the handle, for the reactor, as the command is submitted. */
  private[nwframework] def open(): Unit = {
    opened = (make())
    delivered = false
  }

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    val done =
      try ready(h, reactor)
      catch {
        case t: Throwable =>
          abandon()
          throw t
      }
    if done then {
      deliver(opened.nn)
      delivered = true
    }
    done
  }

  /** Writes what was opened into the caller's slots. */
  protected def deliver(handle: Handle): Unit = into.set(kindOf(handle))

  /** Empties the caller's slots, once delivered. */
  protected def undeliver(): Unit = into.clear(): Unit

  private[nwframework] def abandon(): Unit = if opened != null then {
    if delivered then undeliver()
    NWFrameworkHandles.close(opened.nn)
    opened = null
    delivered = false
  }
}

/** A connection to `address` (see `SocketOps.connect`): ready once Network.framework has it ready, failing with its
  * error if it failed.
  */
final class NWFrameworkConnectTo(address: IpAddress, into: Slot[NWFrameworkKinds.Connection])
    extends NWFrameworkOpening(into, NWFrameworkKinds.connection) {
  protected def make(): Handle = NWFrameworkHandles.connect(address)
  protected def ready(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = h.connected(reactor, token)
}

/** A listener on `address` (see `SocketOps.listen`): ready once it is listening, failing if it cannot. */
final class NWFrameworkListen(address: IpAddress, into: Slot[NWFrameworkKinds.Listener])
    extends NWFrameworkOpening(into, NWFrameworkKinds.listener) {
  protected def make(): Handle = NWFrameworkHandles.listener(address)
  protected def ready(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = h.connected(reactor, token)
}

/** A datagram socket (see `SocketOps.datagram`): a bound one is bound when made, and a connected one ready with its
  * connection.
  */
final class NWFrameworkOpenDatagram(
    local: IpAddress | Null,
    remote: IpAddress | Null,
    into: Slot[NWFrameworkKinds.Datagram]
) extends NWFrameworkOpening(into, NWFrameworkKinds.datagram) {
  protected def make(): Handle = NWFrameworkHandles.datagram(local, remote)
  protected def ready(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    h.kind == NWFrameworkHandle.BoundDatagram || h.connected(reactor, token)
}

/** A FIFO, opened without waiting for its other end (see `FifoOps.openFifoForReading` and `openFifoForWriting`): a
  * descriptor, ready when made.
  */
final class NWFrameworkOpenFifo[K <: Handle](path: String, write: Boolean, into: Slot[K], kindOf: Handle => K)
    extends NWFrameworkOpening(into, kindOf) {
  gears.async.io.Ops.checkPath(path)
  protected def make(): Handle = NWFrameworkHandles.openFifo(path, write)
  protected def ready(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = true
}

/** A pipe, its read end written into `read` and its write end into `write` (see `PipeOps.pipe`): descriptors, ready
  * when made. The command's own handle is the read end.
  */
final class NWFrameworkOpenPipe(read: Slot[NWFrameworkKinds.Readable], write: Slot[NWFrameworkKinds.Writable])
    extends NWFrameworkOpening(read, NWFrameworkKinds.readable) {
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(read, write)
  private var writeEnd: Handle | Null = null
  protected def make(): Handle = {
    val (in, out) = NWFrameworkHandles.pipe()
    writeEnd = (out)
    in
  }
  protected def ready(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = true
  override protected def deliver(handle: Handle): Unit = {
    read.set(NWFrameworkKinds.readable(handle))
    write.set(NWFrameworkKinds.writable(writeEnd.nn))
  }
  override protected def undeliver(): Unit = {
    read.clear(): Unit
    write.clear(): Unit
  }
  override private[nwframework] def abandon(): Unit = {
    if writeEnd != null then NWFrameworkHandles.close(writeEnd.nn)
    writeEnd = null
    super.abandon()
  }
}

/** Closes a handle, completing once it is closed (see `Ops.close`): a connection's cancellation is waited for, and the
  * number is taken back once the op is done, so that the reactor still finds the handle for the cancelled state
  * meanwhile; ops submitted on it meanwhile fail as closed.
  */
final class NWFrameworkClose(val handle: Handle) extends NWFrameworkCommand {
  override private[nwframework] def oneShot: Boolean = true
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

/** Ends a connected stream's sending side (see `SocketOps.shutdownOutput`): at once, the end queued after the sends
  * before it.
  */
final class NWFrameworkShutdownOutput(val handle: Handle) extends NWFrameworkCommand {
  override private[nwframework] def oneShot: Boolean = true
  def kind: Int = NWFrameworkHandle.Write

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    h.shutdownOutput()
    true
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written: for a connection, once what
  * remained has been copied and sent, which is at once while the handle's outstanding sends are within its window, and
  * otherwise once a send has been processed by the stack (see `NWFrameworkHandle.send`). With nothing remaining, done
  * at once, with no system call.
  */
final class NWFrameworkWrite(val handle: Handle, buf: ByteBuffer) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Write
  private var sent = false // the bytes were handed over, and the handle tells once a send is processed

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if !buf.hasRemaining() && !sent then true // nothing to write: done, with no system call
    else if h.isFile || h.isDescriptor then {
      if h.using(NonBlocking.write(_, buf)) >= 0 || !buf.hasRemaining() || h.isFile then true
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
final class NWFrameworkAccept(val handle: Handle, into: Slot[NWFrameworkKinds.Connection]) extends NWFrameworkCommand {
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    val accepted = h.accept(reactor, token)
    if accepted == null then false
    else {
      into.set(NWFrameworkKinds.connection(NWFrameworkHandles.accepted(accepted)))
      true
    }
  }
}

/** Receives one datagram into `buf`, which is left flipped for reading, and writes its sender into `from`. */
final class NWFrameworkReceive(val handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]) extends NWFrameworkCommand {
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(from)
  def kind: Int = NWFrameworkHandle.Read

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean = {
    if buf.capacity() == 0 then throw gears.async.io.Ops.noCapacity(buf) // the packet would be dropped
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

/** Sends `buf` as one datagram, consuming it: on a connected socket to its peer, which `to`, if given, must be, and
  * never an empty one; on an unconnected socket to `to`, without which it fails.
  */
final class NWFrameworkSend(val handle: Handle, buf: ByteBuffer, to: IpAddress | Null) extends NWFrameworkCommand {
  def kind: Int = NWFrameworkHandle.Write
  private var sent = false

  protected def attemptOn(h: NWFrameworkHandle, reactor: NWFrameworkReactor): Boolean =
    if sent then {
      sent = false
      h.sent()
    } else {
      val size = buf.remaining()
      // As the JDK does (see `SocketOps.send`): on a connected socket, only to its peer, and no empty packet.
      val connected = h.kind == NWFrameworkHandle.Datagram
      if connected then {
        if to != null && to != h.remote then throw gears.async.io.Ops.notThePeer(to.nn, h.remote.nn)
        if size == 0 then throw gears.async.io.Ops.emptyPacket(buf)
      } else if to == null then throw gears.async.io.Ops.notConnected()
      val bytes = NativeBuffer.atPosition(buf)
      val done =
        if connected then h.send(bytes, size, reactor, token) else h.sendTo(to.nn, bytes, size, reactor, token)
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
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def block(): Unit = publish(into, body())
}

/** Resolves a host name on the blocker pool, as kqueue's reactor does: Network.framework's public C interface has no
  * resolver of its own, only connections by host name.
  */
final class NWFrameworkResolve(host: String, into: Slot[List[ResolvedAddress]]) extends NWFrameworkBlocking {
  gears.async.io.Ops.checkHost(host)
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def block(): Unit = PosixResolver.lookup(host) match {
    case Right(addresses) => publish(into, gears.async.io.Ops.resolved(host, addresses))
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}

/** Opens a regular file on the blocker pool and publishes its handle (see `FileOps.open`). A cancel that arrives once
  * the file is open closes it again, since the handle never reaches the caller.
  */
final class NWFrameworkOpenFile(path: String, write: Boolean, into: Slot[NWFrameworkKinds.File])
    extends NWFrameworkBlocking {
  gears.async.io.Ops.checkPath(path)
  override private[nwframework] def slotRefusal: Throwable | Null = gears.async.io.Ops.notEmpty(into)
  def block(): Unit = {
    val handle = NWFrameworkHandles.openFile(path, write)
    if !unlessCancelled(into.set(NWFrameworkKinds.file(handle))) then NWFrameworkHandles.close(handle)
  }
}

/** A file op on the blocker pool, on the descriptor of a regular file: the handle is resolved when the task runs, so
  * that one closed meanwhile fails the op, and its descriptor held open while the op uses it, so that a close meanwhile
  * waits for it rather than letting the number be reused under it.
  */
sealed abstract class NWFrameworkFileTask(val handle: Handle) extends NWFrameworkBlocking with Repeatable {
  protected def usingFd[A](body: Int => A): A = {
    val h = NWFrameworkHandles.get(handle)
    if !h.isFile then throw new IOException(s"Handle $handle is not a regular file")
    h.using(body)
  }
}

/** A read at an offset (see `FileOps.read`): `pread` through posixlib's `@blocking` binding, at the op's offset, which
  * each completion advances by the bytes read.
  */
final class NWFrameworkPositionalRead(handle: Handle, buf: ByteBuffer, private var offset: Long)
    extends NWFrameworkFileTask(handle) {
  gears.async.io.Ops.checkOffset(offset)
  def block(): Unit = usingFd { descriptor => // a closed handle fails first
    if !buf.hasRemaining() then throw gears.async.io.Ops.bufferFull(buf)
    val read = unistd.pread(descriptor, NativeBuffer.atPosition(buf), buf.remaining().toCSize, offset.toSize)
    if read < 0 then throw new IOException(s"Failed to read descriptor $descriptor at $offset: ${cError()}")
    val n = read.toInt
    unlessCancelled {
      NativeBuffer.advance(buf, n)
      offset += n
    }: Unit
  }
}

/** A write at an offset (see `FileOps.write`): `pwrite` at the op's offset, which each completion advances. */
final class NWFrameworkPositionalWrite(handle: Handle, buf: ByteBuffer, private var offset: Long)
    extends NWFrameworkFileTask(handle) {
  gears.async.io.Ops.checkOffset(offset)
  def block(): Unit = usingFd { descriptor => // a closed handle fails first, with nothing to write too
    if buf.hasRemaining() then writeAt(descriptor)
  }

  private def writeAt(descriptor: Int): Unit = {
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
  def block(): Unit = usingFd { descriptor =>
    if unistd.fsync(descriptor) < 0 then throw new IOException(s"Failed to sync descriptor $descriptor: ${cError()}")
  }
}
