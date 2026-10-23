package asyncio

import java.nio.ByteBuffer

/** The readiness a custom op waits for before it runs. */
enum Interest {
  case Read, Write
}

/** A mutable outcome of an operation, written by the op and taken by the caller with `clear`. Like a buffer, it belongs
  * to the backend while the op is pending: the caller clears it only after the op's completion has reached them, and
  * the handoff that delivers the completion is what makes the op's write visible. Setting it again before it is cleared
  * is forbidden.
  */
final class Slot[A <: AnyRef] {
  private var value: A | Null = null

  def clear(): A = {
    val v = value
    if v == null then throw new IllegalStateException("Nothing has been written to this slot yet")
    value = null
    v.nn
  }

  private[asyncio] def set(outcome: A): Unit = {
    if value != null then throw new IllegalStateException("Slot was already set")
    value = outcome.nn
  }
}

/** Marks an op that may be submitted again after its previous submission finished, instead of building a new one each
  * time: an accept loop can resubmit one accept op, and a connection can resubmit one read on its buffer. Anything an
  * op reports, such as `Accepted.accepted`, reflects its latest submission, so read it in that submission's completion.
  */
trait Repeatable

/** The ops every backend can build, as values of its `Op` type, on I/O objects named by its handles (see `Handles`):
  * reads and writes of a stream, timers, and closing. Buffers are used between position and limit, and belong to the
  * backend while the op is being performed: on a reactor, from submission until the op's completion.
  */
trait Ops[Op] {

  /** Fills `buf`, which must have room. Completes once at least one byte has been read, or at end of stream, when
    * nothing was added: compare the buffer's position before and after.
    */
  def read(handle: Handle, buf: ByteBuffer): Op & Repeatable

  /** Drains `buf`. Completes once at least one byte has been written; resubmit while `buf` has bytes remaining. */
  def write(handle: Handle, buf: ByteBuffer): Op & Repeatable

  /** Completes once `milliseconds` have passed. */
  def timer(milliseconds: Int): Op & Repeatable

  /** Fills a buffer of `pool`'s, from `Backend.pool`, as the other `read` fills the caller's: completes once at least
    * one byte has been read, or at the end of the stream, writing what it read into `into`, which must be empty, as a
    * `Received` to release. Fails if `pool` has no buffer free when the read needs one; a backend that controls its
    * source's flow may instead stop taking bytes while none is free, so that the read waits (see `BufferPool`). A
    * completion-based backend, such as Network.framework's or io_uring's, receives straight into the pool's buffer and
    * hands it over, where a read into the caller's buffer costs it a copy; one whose facility hands it chunks of its
    * own, as Node's streams do, hands such a chunk over.
    */
  def read(handle: Handle, pool: BufferPool, into: Slot[Received]): Op & Repeatable

  /** Closes `handle`, completing once it is closed. The ops pending on it, on whichever submitter they are, have failed
    * with an `IOException` (a closed channel's) before this completes, their completions getting `onFailure` from that
    * submitter's loop and a `perform` waiting on one throwing it, as the JDK fails a thread blocked on a channel that
    * is closed; its number is then free for reuse. A Unix socket path that a listener or datagram socket bound is
    * removed with it. Fails if `handle` is not open.
    */
  def close(handle: Handle): Op
}

/** The ops of IP sockets, for a backend that has them (see `SocketBackend`); `read` and `write` run on a stream socket
  * as on any stream.
  */
trait SocketOps[Op] extends Ops[Op] {

  /** Accepts one connection on a listening socket. Completes after writing the new connection into `into`, which must
    * be empty.
    */
  def accept(handle: Handle, into: Handle.Slot): Op & Repeatable

  /** Connects `handle`, a socket from `handles.connect`. Completes once connected, or fails. */
  def connect(handle: Handle): Op

  /** Receives one packet on an IP datagram socket into `buf`, which is left flipped for reading, and writes its sender
    * into `from`, which must be empty. Completes once a packet has arrived; its size is `buf.remaining`, which may be 0.
    */
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]): Op & Repeatable

  /** Sends `buf` as one packet on an IP datagram socket, to `to`. Completes once the whole packet has been sent,
    * consuming `buf`.
    */
  def send(handle: Handle, buf: ByteBuffer, to: IpAddress): Op & Repeatable

  /** Sends `buf` as one packet on a connected datagram socket, of any family, to its peer, as the other `send`. */
  def send(handle: Handle, buf: ByteBuffer): Op & Repeatable

  /** Completes once `handle`, from `Handles.listener`, is listening, or fails, as `connect` does for a socket to
    * connect: with an `IOException` if the address is in use or cannot be bound. On a datagram socket from
    * `Handles.datagram`, completes once it is bound, which may be at once (see `SocketHandles.datagram`).
    */
  def listening(handle: Handle): Op
}

/** Regular files as ops, for a backend that has them (see `FileBackend`). A file is opened by an op, since opening does
  * disk I/O, for the path, as reads do, and some environments can only open asynchronously; its reads and writes are
  * positional, as `pread` and `pwrite` are, so that one handle serves concurrent reads at different offsets and no op
  * depends on a shared file position, which io_uring has none of either. `Ops.read` and `Ops.write` on a file handle
  * read and write at the position the handle carries, as a stream, for code that treats a file as one.
  *
  * Cancellation follows `BlockingOps.blocking`: an op not yet started is skipped, one running finishes on its thread
  * with its result discarded, and `onCancel` runs once the thread has stopped touching the buffer.
  */
trait FileOps[Op] extends Ops[Op] {

  /** Opens the regular file at `path` for reading, or for writing with `write`, and completes after writing its handle
    * into `into`, which must be empty; anything but a regular file is refused, failing the op with an `IOException`, as
    * a path that cannot be opened does. A cancel that arrives before the write leaves `into` empty and the file closed.
    */
  def open(path: String, write: Boolean, into: Handle.Slot): Op

  /** Fills `buf` from the file at `offset`, as `Ops.read` fills it from a stream: completes once at least one byte has
    * been read, or at the end of the file, when nothing was added, so a caller resubmits with the offset advanced.
    */
  def read(handle: Handle, buf: ByteBuffer, offset: Long): Op & Repeatable

  /** Drains `buf` into the file at `offset`, as `Ops.write` drains it into a stream: completes once at least one byte
    * has been written; resubmit while `buf` has bytes remaining, with the offset advanced.
    */
  def write(handle: Handle, buf: ByteBuffer, offset: Long): Op & Repeatable

  /** Completes once the file's written bytes have reached the device, as `fsync` does. */
  def sync(handle: Handle): Op & Repeatable
}

/** Name resolution, for a backend that has it (see `ResolveBackend`). */
trait ResolveOps[Op] extends Ops[Op] {

  /** Resolves a host name, without blocking a reactor's thread. Completes after writing its addresses, in numeric form,
    * into `into`, which must be empty; fails with an `IOException` if the name cannot be resolved. A cancel that
    * arrives before the write leaves `into` empty.
    */
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Op
}

/** Readiness, for a backend that has it (see `ReadinessBackend`). */
trait ReadinessOps[Op] extends Ops[Op] {

  /** Waits until `handle` is ready for `interest`, then runs `perform` on the reactor thread. The op completes when
    * `perform` returns true; false means it was not ready after all, and the reactor waits again. For operations the
    * other ops do not cover, such as draining a FIFO.
    */
  def whenReady(handle: Handle, interest: Interest)(perform: () => Boolean): Op & Repeatable
}

/** Blocking code as an op, for a backend that has it (see `BlockingBackend`). */
trait BlockingOps[Op] extends Ops[Op] {

  /** Runs `body` off the reactor thread, completing when it returns. Cancelling it skips `body` if it has not started
    * and interrupts it if it is running; interruption is cooperative, and `onCancel` waits for `body` to return.
    */
  def blocking(body: () => Unit): Op

  /** Like the other `blocking`, writing `body`'s value into `into`, which must be empty, before completing. A cancel
    * that arrives before the write discards the value, leaving `into` empty; nothing is written after a cancel.
    */
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): Op
}

/** The ops of Unix domain datagram sockets, as `SocketOps.receive` and `SocketOps.send` are for IP ones. */
trait UnixDatagramOps[Op] extends SocketOps[Op] {

  /** Receives one packet on a Unix datagram socket, as `receive` does on an IP one; an unnamed sender has the empty
    * path.
    */
  @annotation.targetName("receiveUnix")
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[UnixAddress]): Op & Repeatable

  /** Sends one packet on a Unix datagram socket, to `to`, as `send` does on an IP one. */
  def send(handle: Handle, buf: ByteBuffer, to: UnixAddress): Op & Repeatable
}
