package gears.async.io

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

  private[io] def set(outcome: A): Unit = {
    if value != null then throw new IllegalStateException("Slot was already set")
    value = outcome.nn
  }
}

/** Marks an op that may be submitted again after its previous submission finished, instead of building a new one each
  * time: an accept loop can resubmit one accept op, and a connection can resubmit one read on its buffer. Anything an
  * op reports, such as `Accepted.accepted`, reflects its latest submission, so read it in that submission's completion.
  */
trait Repeatable

/** The ops every backend can build, as values of its `Op` type, on I/O objects named by its handles, values of its
  * `Handle` type, `H` here (see `Backend.Handle`): reads and writes of a stream, timers, and closing. Buffers are used
  * between position and limit, and belong to the backend while the op is being performed: on a reactor, from submission
  * until the op's completion.
  *
  * A handle's kind is in its type, a subtype of `H`, so that an op on the wrong kind of handle does not compile: a
  * stream is read through a `Readable` and written through a `Writable`; a `Connection` (a connected stream socket) and
  * a `File` are both; a `Listener` accepts; a `Datagram` (IP) and a `UnixDatagram` send and receive packets. Every
  * handle is closed with `close`. The opener of each kind writes a handle of that kind into its slot, so a program
  * holds the kind it opened.
  *
  * Handles are opened by ops too, each kind by the capability that has it (`SocketOps.connect`, `PipeOps.pipe`,
  * `FileOps.open` and the rest), since opening may wait, for a connection, a bind or the disk, and some environments
  * can only open asynchronously. Such an op writes what it opened into a slot when it completes; one that fails, is
  * cancelled or times out leaves its slot empty and nothing open, so a handle is the caller's from the moment it is
  * taken from the slot, and the caller closes it with `close`. Failures are `java.io.IOException`s.
  */
trait Ops[Op, H <: AnyRef] {

  /** A handle that can be read as a stream: a connection, a pipe's or FIFO's read end, or a file. */
  type Readable <: H

  /** A handle that can be written as a stream: a connection, a pipe's or FIFO's write end, or a file. */
  type Writable <: H

  /** A connected stream socket, IP or Unix, from `connect` or `accept`: read, written and shut down for writing. */
  type Connection <: Readable & Writable

  /** A listening stream socket, IP or Unix, from `listen`, which accepts connections. */
  type Listener <: H

  /** An IP datagram socket, from `datagram`, which sends and receives packets. */
  type Datagram <: H

  /** A Unix domain datagram socket, from the Unix `datagram`, which sends and receives packets. */
  type UnixDatagram <: H

  /** A regular file, from `open`: read and written as a stream and at offsets, and synced. */
  type File <: Readable & Writable

  /** Fills `buf`, which must have room. Completes once at least one byte has been read, or at end of stream, when
    * nothing was added: compare the buffer's position before and after. A read into a buffer with no room fails with an
    * `IllegalArgumentException` as it is submitted, reading nothing, since it could only add nothing, which means the
    * end of the stream.
    */
  def read(handle: Readable, buf: ByteBuffer): Op & Repeatable

  /** Drains `buf`. Completes once at least one byte has been written; resubmit while `buf` has bytes remaining. With
    * nothing remaining, completes at once, writing nothing.
    */
  def write(handle: Writable, buf: ByteBuffer): Op & Repeatable

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
  def read(handle: Readable, pool: BufferPool, into: Slot[Received]): Op & Repeatable

  /** Closes `handle`, completing once it is closed. The ops pending on it, on whichever submitter they are, have failed
    * with an `IOException` (a closed channel's) before this completes, their completions getting `onFailure` from that
    * submitter's loop and a `perform` waiting on one throwing it, as the JDK fails a thread blocked on a channel that
    * is closed; its number is then free for reuse. A Unix socket path that a listener or datagram socket bound is
    * removed with it. Fails if `handle` is not open.
    */
  def close(handle: H): Op
}

/** The ops of IP sockets, for a backend that has them (see `SocketBackend`); `read` and `write` run on a stream socket
  * as on any stream.
  */
trait SocketOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Accepts one connection on a listening socket. Completes after writing the new connection into `into`, which must
    * be empty.
    */
  def accept(handle: Listener, into: Slot[Connection]): Op & Repeatable

  /** Accepts the connections pending on a listening socket, as `accept` does one: completes once at least one has been
    * accepted, after writing them, at most `max`, into `into`, which must be empty. A server under a burst of
    * connections takes them in one op each time the listener is ready rather than one op apiece, which keeps its listen
    * queue from filling, and the kernel from resetting what arrives meanwhile, while its other work runs.
    */
  def acceptAll(handle: Listener, into: Slot[Array[Connection]], max: Int): Op & Repeatable

  /** Opens a stream socket and connects it to `address`. Completes once connected, after writing the socket into
    * `into`, which must be empty; fails if the connection is refused or cannot be made.
    */
  def connect(address: IpAddress, into: Slot[Connection]): Op

  /** Opens a stream socket listening on `address`. Completes once it is listening, after writing it into `into`, which
    * must be empty; fails if the address is in use or cannot be bound. With `reuseAddr`, the address can be bound while
    * connections from an earlier listener on it are still in `TIME_WAIT`, so a restarted server need not wait for them
    * to expire.
    */
  def listen(address: IpAddress, into: Slot[Listener], reuseAddr: Boolean = true): Op

  /** Shuts down a connected stream socket for writing, as `shutdown(SHUT_WR)` does, leaving it open for reading: what
    * was written before is still sent, then the end of the stream, which the peer reads as it would a close. Completes
    * once the end is queued; writes after it fail. A server that closes once it has replied shuts down first and reads
    * until the client's end before closing, since closing with bytes from the peer unread resets the connection, and a
    * reset can discard the reply the peer has not read yet; a client shuts down once its request is out, so that the
    * server sees where it ends.
    */
  def shutdownOutput(handle: Connection): Op

  /** Opens a datagram socket, bound to `local` and connected to `remote` where given, which must be of the same family.
    * Completes once it is bound, after writing it into `into`, which must be empty; fails if `local` is in use or
    * cannot be bound.
    */
  def datagram(local: IpAddress | Null, remote: IpAddress | Null, into: Slot[Datagram]): Op

  /** Receives one packet on an IP datagram socket into `buf`, which is cleared first and left flipped for reading, and
    * writes its sender into `from`, which must be empty. Completes once a packet has arrived; its size is
    * `buf.remaining`, the bytes beyond `buf`'s capacity being dropped, silently, as every system's `recvfrom` and the
    * JDK drop them: a protocol over datagrams fixes the largest packet it sends, and a receiver gives a buffer that
    * holds it. A buffer of no capacity fails the op with an `IllegalArgumentException`, receiving nothing, since it
    * would drop a whole packet.
    */
  def receive(handle: Datagram, buf: ByteBuffer, from: Slot[IpAddress]): Op & Repeatable

  /** Sends `buf` as one packet on an IP datagram socket, to `to`. Completes once the whole packet has been sent,
    * consuming `buf`; an empty `buf` sends an empty packet on a socket that is not connected. Sent means handed to the
    * system, not delivered: as anywhere in UDP, a packet may still be lost, and on NIO an empty one counts as sent even
    * if the socket had no room for it then, since the JDK reports both as 0 bytes (as Netty does, the same as a loss).
    * On a connected socket, `to` must be its peer, and the packet goes as the other `send` sends it, so an empty one
    * fails; any other address fails the op with an `IllegalArgumentException`, sending nothing, as the JDK does,
    * whatever the system would do. On a socket that is not connected, a packet lost on the way, to a port nothing
    * listens on say, fails neither this send nor a later one, to that peer or another, as no system tells such a socket
    * of its packets' fate.
    */
  def send(handle: Datagram, buf: ByteBuffer, to: IpAddress): Op & Repeatable

  /** Sends `buf` as one packet on a connected datagram socket, of any family, to its peer, as the other `send`. Fails
    * with an `IllegalArgumentException`, sending nothing, on a socket that is not connected, or if `buf` is empty: no
    * backend sends an empty packet on a connected socket, since the JDK cannot.
    */
  def send(handle: Datagram, buf: ByteBuffer): Op & Repeatable
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
trait FileOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Opens the regular file at `path` for reading, or for writing with `write`, and completes after writing its handle
    * into `into`, which must be empty; anything but a regular file is refused, failing the op with an `IOException`, as
    * a path that cannot be opened does. A cancel that arrives before the write leaves `into` empty and the file closed.
    */
  def open(path: String, write: Boolean, into: Slot[File]): Op

  /** Fills `buf` from the file at `offset`, as `Ops.read` fills it from a stream: completes once at least one byte has
    * been read, or at the end of the file, when nothing was added, so a caller resubmits with the offset advanced; and
    * fails, as it does, if `buf` has no room.
    */
  def read(handle: File, buf: ByteBuffer, offset: Long): Op & Repeatable

  /** Drains `buf` into the file at `offset`, as `Ops.write` drains it into a stream: completes once at least one byte
    * has been written, or, if nothing remains, without writing; resubmit while `buf` has bytes remaining, with the
    * offset advanced.
    */
  def write(handle: File, buf: ByteBuffer, offset: Long): Op & Repeatable

  /** Completes once the file's written bytes have reached the device, as `fsync` does. */
  def sync(handle: File): Op & Repeatable
}

/** Name resolution, for a backend that has it (see `ResolveBackend`). */
trait ResolveOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Resolves a host name, without blocking a reactor's thread. Completes after writing its addresses, in numeric form,
    * into `into`, which must be empty; fails with an `IOException` if the name cannot be resolved. A cancel that
    * arrives before the write leaves `into` empty.
    */
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Op
}

/** Readiness, for a backend that has it (see `ReadinessBackend`). */
trait ReadinessOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Completes once `handle` is ready for `interest`. The caller then uses the handle without waiting (`readNow`,
    * `writeNow`) until it would block, and performs the op again to wait for the next readiness. Level-triggered:
    * performed again while the handle is still ready, it completes at once. For what the other ops do not cover, such
    * as draining a FIFO.
    */
  def ready(handle: H, interest: Interest): Op & Repeatable

  /** Reads what is available now into `buf`, without waiting: the bytes read, 0 if nothing is, or -1 at end of stream.
    * Throws `IllegalArgumentException` if `buf` has no room, as `Ops.read` fails. Not an op: it runs on the calling
    * thread and returns.
    */
  def readNow(handle: Readable, buf: ByteBuffer): Int

  /** Writes what fits now from `buf`, without waiting: the bytes written, or -1 if nothing fits; 0 if `buf` has nothing
    * remaining, writing nothing. Not an op.
    */
  def writeNow(handle: Writable, buf: ByteBuffer): Int
}

/** The ops of pipes, for a backend that has them (see `PipeBackend`): `read` on its read end, `write` on its write end.
  */
trait PipeOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Opens a pipe. Completes after writing its read end into `read` and its write end into `write`, which must both be
    * empty; one that fails or is cancelled leaves both empty and neither end open.
    */
  def pipe(read: Slot[Readable], write: Slot[Writable]): Op
}

/** The ops of FIFOs, for a backend that has them (see `FifoBackend`), which are then read and written as pipes are. */
trait FifoOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Opens the FIFO at `path` for reading, without waiting for a writer, and completes after writing it into `into`,
    * which must be empty. Read before a writer has opened it, it is at its end.
    */
  def openFifoForReading(path: String, into: Slot[Readable]): Op

  /** Opens the FIFO at `path` for writing, without waiting for a reader, and completes after writing it into `into`,
    * which must be empty; fails while the FIFO has no reader.
    */
  def openFifoForWriting(path: String, into: Slot[Writable]): Op
}

/** The ops of Unix domain stream sockets, which the ops of IP stream sockets (`accept`, `read`, `write`) also run on.
  */
trait UnixStreamOps[Op, H <: AnyRef] extends SocketOps[Op, H] {

  /** Opens a stream socket and connects it to `address`, as the other `connect` does to an IP address. */
  def connect(address: UnixAddress, into: Slot[Connection]): Op

  /** Opens a stream socket listening on `address`, as the other `listen` does on an IP address, replacing a socket file
    * already there.
    */
  def listen(address: UnixAddress, into: Slot[Listener]): Op
}

/** Blocking code as an op, for a backend that has it (see `BlockingBackend`). */
trait BlockingOps[Op, H <: AnyRef] extends Ops[Op, H] {

  /** Runs `body` off the reactor thread, completing when it returns. Cancelling it skips `body` if it has not started
    * and interrupts it if it is running; interruption is cooperative, and `onCancel` waits for `body` to return.
    */
  def blocking(body: () => Unit): Op

  /** Like the other `blocking`, writing `body`'s value into `into`, which must be empty, before completing. A cancel
    * that arrives before the write discards the value, leaving `into` empty; nothing is written after a cancel.
    */
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): Op
}

/** The ops of Unix domain datagram sockets, as `SocketOps.datagram`, `receive` and `send` are for IP ones. */
trait UnixDatagramOps[Op, H <: AnyRef] extends SocketOps[Op, H] {

  /** Opens a datagram socket, bound to `local` and connected to `remote` where given, as the other `datagram` does. A
    * client needs a `local` path so that replies can reach it.
    */
  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null, into: Slot[UnixDatagram]): Op

  /** Receives one packet on a Unix datagram socket, as `receive` does on an IP one; an unnamed sender has the empty
    * path.
    */
  @annotation.targetName("receiveUnix")
  def receive(handle: UnixDatagram, buf: ByteBuffer, from: Slot[UnixAddress]): Op & Repeatable

  /** Sends one packet on a Unix datagram socket, to `to`, as `send` does on an IP one. */
  def send(handle: UnixDatagram, buf: ByteBuffer, to: UnixAddress): Op & Repeatable

  /** Sends one packet on a connected Unix datagram socket to its peer, as the other `send` does on an IP one. */
  @annotation.targetName("sendUnix")
  def send(handle: UnixDatagram, buf: ByteBuffer): Op & Repeatable
}

private[io] object Ops {

  /** The failure of a read into `buf` when it has no room: such a read could only read nothing, which says the stream
    * has ended, so it fails before anything is read or waited for.
    */
  def bufferFull(buf: ByteBuffer): IllegalArgumentException =
    new IllegalArgumentException(s"The buffer is full, so a read into it can read nothing: $buf")

  /** The failure of a receive into `buf` when it has no capacity, which would drop the whole packet. */
  def noCapacity(buf: ByteBuffer): IllegalArgumentException =
    new IllegalArgumentException(s"The buffer has no capacity, so a packet received into it would be dropped: $buf")

  /** The failure of a send of an empty packet on a connected socket, where not every backend can send one (see
    * `SocketOps.send`).
    */
  def emptyPacket(buf: ByteBuffer): IllegalArgumentException =
    new IllegalArgumentException(s"The buffer is empty, and an empty packet is not sent on a connected socket: $buf")

  /** The failure of a send to `to` on a socket connected to another `peer` (see `SocketOps.send`). */
  def notThePeer(to: AnyRef, peer: AnyRef): IllegalArgumentException =
    new IllegalArgumentException(s"The socket is connected to $peer, so it cannot send to $to")

  /** The failure of a send with no address on a socket that is not connected (see `SocketOps.send`). */
  def notConnected(): IllegalArgumentException =
    new IllegalArgumentException("The datagram socket is not connected, so a packet needs an address")
}
