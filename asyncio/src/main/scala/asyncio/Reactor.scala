package asyncio

import _root_.gears.async.Async
import _root_.gears.async.Cancellable

import java.nio.ByteBuffer

/** What to do when an op finishes. The op comes back with its outcome, so one completion can own many ops. A completion
  * that submits further ops holds on to its reactor itself, since only a reference to that reactor has the right `Op`
  * type.
  */
@FunctionalInterface
trait Completion[-C] {

  /** The op finished. What finishing means for each kind of op is described in `Ops`; anything it produced is in the
    * caller's buffers and slots.
    */
  def onComplete(op: C): Unit

  /** The op failed with an exception instead of finishing. By default the failure propagates out of `Reactor.run`. */
  def onFailure(op: C, failure: Throwable): Unit = throw failure

  /** The op was cancelled before it finished, by `Reactor.cancel` or because the reactor closed. It runs on the reactor
    * thread once the reactor has stopped using the op's buffers and slots, which are the caller's again, and afterwards
    * neither `onComplete` nor `onFailure` runs for this submission. It must not submit ops while the reactor is
    * closing. By default it does nothing.
    */
  def onCancel(op: C): Unit = ()
}

/** A single-threaded event loop. Every completion runs on the thread that drives the loop with `runOnce` or `run`, and
  * apart from `wakeup`, a reactor is only used from that thread.
  */
trait Reactor extends AutoCloseable {

  /** Something submitted to this reactor that later completes. The reactors one `ReactorBackend` opens share it, so an
    * op built by the backend may go to any of them (see `ReactorBackend`); otherwise the type belongs to the reactor
    * value: given `a, b: Reactor`, `a.Op` and `b.Op` are different types.
    */
  type Op

  /** Builds this reactor's ops, as its backend's `ops` does. Building an op does not touch the reactor, so `ops` may be
    * used from any thread.
    */
  def ops: Ops[Op]

  /** Opens, uses without waiting, and closes this reactor's handles, as its backend's `handles` does. It does not touch
    * the reactor either, so it may be used from any thread.
    */
  def handles: Handles

  /** Submits an op with the completion to run when it finishes. An op marked `Repeatable` may be submitted again once
    * its previous submission has completed, failed, or been cancelled; any other op may be submitted only once. No op
    * may be pending twice at the same time. An op may finish during `submit`, such as a read of data already waiting,
    * but its completion never runs inside `submit`: it runs from a later turn of the loop. Nor need the op be issued
    * to the I/O facility inside `submit`: a reactor may hold the ops submitted in a turn and issue them together at
    * the turn's end, as io_uring's submission queue or Network.framework's batches invite, so long as `trySubmit`
    * issues at once and a turn issues everything before it blocks.
    */
  def submit[C <: Op](op: C, completion: Completion[C]): Unit

  /** `op`'s attachment, as its backend's `attachment` returns it. */
  def attachment(op: Op): AnyRef | Null

  /** Sets `op`'s attachment, as its backend's `attach` does. */
  def attach(op: Op, value: AnyRef | Null): Unit

  /** Like `submit`, but an op the reactor can finish at once, as it may for an operation that only reports would-block
    * when not ready, it finishes here: it returns true without ever calling `completion`, and the op's results are
    * already in the caller's buffers and slots; a failure is thrown. Otherwise the op is submitted as by `submit`,
    * `completion` runs when it finishes, and this returns false. A caller can then continue at once instead of waiting
    * for a later turn, but must take care to let other work run if its ops keep finishing at once.
    */
  def trySubmit[C <: Op](op: C, completion: Completion[C]): Boolean

  /** Cancels a submitted op that has not finished, and returns true: its completion's `onComplete` and `onFailure` will
    * never run, and its `onCancel` runs once the reactor has stopped using the op's buffers and slots. That is before
    * this returns for most ops, but later, from `run`, for one still running elsewhere, such as a blocking op that has
    * to be interrupted. Returns false if the op already finished or was never submitted here.
    */
  def cancel(op: Op): Boolean

  /** One turn of the loop: runs the completions of everything that is ready now, and returns. With `block`, if nothing
    * was ready it first waits until something is, or until `wakeup` is called. A scheduler drives the loop with this,
    * running its own tasks between turns and blocking only when it has none.
    */
  def runOnce(block: Boolean): Unit

  /** Makes a blocked `runOnce` return, or the next one not block; it may be called from any thread. A scheduler calls
    * it when another thread gives it a task. Wake-ups requested before the loop gets to them may be merged into one.
    */
  def wakeup(): Unit

  /** Runs turns, blocking between them, until `stop` is called. */
  def run(): Unit

  /** Makes `run` return once the current turn finishes. */
  def stop(): Unit

  /** Releases the reactor's own resources. Every pending op is cancelled, so its completion gets `onCancel` and nothing
    * else; ops still running elsewhere, such as blocking ops, are waited for first. Handles named by ops belong to the
    * caller and stay open (closing one instead fails the ops pending on it: see `Handles.close`). Call it when `run`
    * is not running; closing twice does nothing.
    */
  def close(): Unit
}

object Reactor {

  /** A reactor that runs ops of type `O`: one that a backend whose `Op` is `O` opened. */
  type Of[O] = Reactor { type Op = O }

  /** Opens reactors of kind `R`. Code written against the interface asks for one of these instead of naming an
    * implementation, and states what it needs of the reactor through `R`.
    */
  @FunctionalInterface
  trait Factory[+R <: Reactor] {
    def open(): R
  }

  /** Opens a reactor for `body` and closes it afterwards. The reactor's type comes from the given factory. */
  def scoped[R <: Reactor](using factory: Factory[R])[A](body: R => A): A = {
    val reactor = factory.open()
    try body(reactor)
    finally reactor.close()
  }
}

/** A way of doing I/O: the ops it can perform, the handles they name, and `perform`, which runs one. A backend may run
  * its ops on reactors (`ReactorBackend`), or do each one itself on the calling thread, as `asyncio.vthread`'s does
  * with the JDK's blocking channels on a virtual thread. Ops of another backend are a different type. Building an op or
  * using a handle performs nothing, so either may be done from any thread.
  *
  * What every backend has is here: IP sockets, pipes and regular files, read, written, accepted, connected, sent and
  * received, timers, and name resolution; reads into buffers of the backend's (`pool`); handles that listen and close
  * as ops (`Ops.listening`, `Ops.close`), since a backend whose I/O facility opens and closes asynchronously, such as
  * Network.framework or io_uring, reports an address in use, or a close done, through a completion rather than by
  * blocking the caller. What only some have is a capability the backend mixes in, with its own handles and ops: a
  * reactor (`ReactorBackend`), readiness (`ReadinessBackend`), blocking ops (`BlockingBackend`), `UnixStreamBackend`,
  * `UnixDatagramBackend` and `FifoBackend`. Code that needs one asks for a backend that has it, so a backend without
  * it is refused when compiling, not when running.
  */
trait Backend {

  /** What this backend performs. */
  type Op

  /** Builds this backend's ops. */
  def ops: Ops[Op]

  /** Opens, uses, and closes the handles this backend's ops name. */
  def handles: Handles

  /** Performs `op` and suspends the caller until it is done: it returns when the op finishes, throws its failure, or
    * throws `CancellationException` when the caller is cancelled first. Once it returns or throws, the op's buffers and
    * slots are the caller's again. An op marked `Repeatable` may be performed again once it has returned; any other op
    * only once. Where the caller must be, and what cancelling it does to the op, is the backend's to say.
    */
  def perform(op: Op)(using Async): Unit

  /** Opens a pool of `buffers` buffers of `bufferSize` bytes each, for the reads into buffers of the backend's
    * (`Ops.read` with a pool). Close it once nothing of it is held.
    */
  def pool(bufferSize: Int, buffers: Int): BufferPool
}

/** A backend whose ops run on reactors, `Reactor`s it opens: an op built by `ops` may be submitted to any of them, one
  * submission at a time, so that a task moving from one reactor's thread to another's can take its ops along: once a
  * submission has completed, failed or been cancelled on one reactor, a `Repeatable` op may go to another. `perform`
  * routes an op to a reactor of the caller's program (see `asyncio.gears.ReactorAsync`), which must have been started
  * by `ReactorFutures.run` with this backend.
  */
trait ReactorBackend extends Backend with Reactor.Factory[Reactor] {

  /** The caller's own data for `op`, kept across submissions and never read by a reactor, like kqueue's `udata` or
    * io_uring's `user_data`: for instance a completion to reuse each time `op` is submitted, so that submitting it
    * again allocates nothing. Null until set. Used by one thread at a time: the one submitting the op, which hands it
    * over, attachment included, when the op moves to another reactor's thread.
    */
  def attachment(op: Op): AnyRef | Null

  /** Sets `op`'s attachment (see `attachment`). */
  def attach(op: Op, value: AnyRef | Null): Unit

  /** Opens a reactor that runs this backend's ops. Close it when done. */
  def open(): Reactor.Of[Op]

  /** Submits `op` to a reactor of the caller's program, and suspends the caller until it finishes (see
    * `ReactorAsync.performOnReactor`). Cancelling the caller asks the reactor to cancel the op, and waits until it has.
    */
  final def perform(op: Op)(using Async): Unit = asyncio.gears.ReactorAsync.performOnReactor(this, op)

  /** Performs `op` now, on the calling thread and on no reactor, if this backend can and the op finishes at once, as a
    * read of data already waiting or a write that fits does: then it returns true, the op's results are in the
    * caller's buffers and slots, and a failure is thrown, as from `Reactor.trySubmit`. Otherwise, or if this backend
    * only tries ops on a reactor's thread, it returns false having done nothing that counts, and the op is submitted
    * as usual. For a caller on no reactor's thread, which would otherwise hand every op to one. False by default.
    */
  def tryNow(op: Op): Boolean = false

  /** Whether `tryNow` can finish `op` on the calling thread at all: one it can, which did not finish, may be worth
    * trying again after a yield, where one it cannot, such as a timer, is handed over at once. False by default.
    */
  def triesNow(op: Op): Boolean = false

  /** A blocking op's body, to run on the calling thread rather than on this backend's blocker threads, for a caller
    * that may block anyway and is on no reactor's thread; null if `op` is not a blocking op, or this backend runs them
    * only on its blocker threads. As a submission would, it runs the op once: a one-shot op already submitted is
    * refused with `IllegalStateException`. False by default: null.
    */
  def blockingHere(op: Op): BlockingHere | Null = null

  /** A wait for `op` on the calling thread, for a caller that may block and is on no reactor's thread: it parks the
    * caller until `op` is done, or may be worth trying again with `tryNow`, with no reactor involved. Null if this
    * backend cannot wait for `op` there, or waits only on its reactors. Null by default. A wait whose tries make
    * system calls on a handle (`triesNow`) is taken only while few ops are waiting; one that contends for nothing,
    * such as a timer parked until its time, always.
    */
  def waitHere(op: Op): WaitHere | Null = null
}

/** A wait on the calling thread for an op's handle to be ready (see `ReactorBackend.waitHere`). A `Cancellable`, so
  * that the caller's group can end it.
  */
trait WaitHere extends Cancellable {

  /** Parks the caller until the op is done, or its handle may be ready, or `cancel` has been called, and returns:
    * true if the wait finished the op, as a timer's does once its time has passed, so there is nothing to try; false
    * if the caller should try the op again. Throws if the op's handle has been closed.
    */
  def await(): Boolean

  /** Ends the wait, from any thread: a caller parked in `await` returns, and the next `await` returns at once. */
  def cancel(): Unit
}

/** A blocking op run on the calling thread (see `ReactorBackend.blockingHere`). */
trait BlockingHere {

  /** Runs the body, writing its result into the op's slot, and returns once it has; its failure is thrown. If `cancel`
    * came first, the body is skipped; if during, the body is interrupted, its result not written, and once it returns
    * this throws `CancellationException`.
    */
  def run(): Unit

  /** Cancels the op from any thread, as `Reactor.cancel` would a blocking op: skipped, or interrupted. */
  def cancel(): Unit
}

/** A backend that can say when a handle is ready, and use it without waiting (see `ReadinessOps` and
  * `ReadinessHandles`): one over non-blocking descriptors or channels, such as kqueue's and NIO's.
  */
trait ReadinessBackend extends Backend {
  def ops: ReadinessOps[Op]
  def handles: ReadinessHandles
}

/** A backend that runs blocking code as an op, off the threads that run its other ops (see `BlockingOps`), so that it
  * keeps them free: one whose ops run on reactors. A backend whose callers may block anyway has no need of it; they
  * call the blocking code themselves.
  */
trait BlockingBackend extends Backend {
  def ops: BlockingOps[Op]
}

/** A backend with Unix domain stream sockets (see `UnixStreamHandles`). */
trait UnixStreamBackend extends Backend {
  def handles: UnixStreamHandles
}

/** A backend with Unix domain datagram sockets (see `UnixDatagramHandles` and `UnixDatagramOps`). */
trait UnixDatagramBackend extends Backend {
  def ops: UnixDatagramOps[Op]
  def handles: UnixDatagramHandles
}

/** A backend that waits on FIFOs as it does on pipes (see `FifoHandles`). */
trait FifoBackend extends Backend {
  def handles: FifoHandles
}

/** Buffers a backend fills for the reads into its own buffers, opened by `Backend.pool`. A pool may be used from any thread.
  * `close` frees its buffers, and must come once no read is pending on it and every `Received` from it has been
  * released.
  */
trait BufferPool extends AutoCloseable {
  def bufferSize: Int
  def buffers: Int

  /** Buffers neither held by a pending read nor by an unreleased `Received`. */
  def available: Int

  def close(): Unit
}

/** Bytes a pooled read received: `buffer` is one of the pool's, positioned at 0 with its limit at the bytes received,
  * 0 at the end of the stream. It is the caller's until `release` gives it back to the pool, which must happen exactly
  * once, from any thread; the `Received` is not to be used after.
  */
trait Received {
  def buffer: ByteBuffer
  def release(): Unit
}

/** The readiness a custom op waits for before it runs. */
enum Interest {
  case Read, Write
}

/** Where an IP socket lives: a numeric IPv4 or IPv6 address and a port. */
enum IpAddress {
  case IPv4(host: String, port: Int)
  case IPv6(host: String, port: Int)
}

/** Where a Unix domain socket lives: a path, or the empty path for an unnamed socket. */
final case class UnixAddress(path: String)

/** Opening a backend's handles; `Ops.close` closes them. Every operation is synchronous and never blocks; failures
  * throw `java.io.IOException`.
  *
  * A handle is an `Int` that names an open file, socket, or other I/O object. Its meaning belongs to the backend: it is
  * the I/O object itself where that is an integer, such as a POSIX descriptor, and otherwise an index into a table the
  * backend keeps of references to its I/O objects, such as JVM channels. Handles belong to the caller, who closes them
  * with `Ops.close`; the backend never closes them.
  */
trait Handles {

  /** Opens a regular file for reading or writing; anything else is refused. A regular file is always ready, so ops on it
    * never wait.
    */
  def openFile(path: String, write: Boolean): Int

  /** A stream socket to connect to `address`; perform `ops.connect` to wait until it is connected. */
  def connect(address: IpAddress): Int

  /** A stream socket that will listen on `address` once `ops.listening` is performed on it: binding and listening
    * happen there, so an address in use fails that op, not this. With `reuseAddr`, the address can be bound while
    * connections from an earlier listener on it are still in `TIME_WAIT`, so a restarted server need not wait for
    * them to expire.
    */
  def listener(address: IpAddress, reuseAddr: Boolean = true): Int

  /** A datagram socket, bound to `local` and connected to `remote` where given, which must be of the same family. */
  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Int

  /** A pipe, as its read end and its write end. */
  def pipe(): (Int, Int)
}

/** Using handles without waiting, for a backend whose handles are non-blocking (see `ReadinessBackend`). */
trait ReadinessHandles extends Handles {

  /** Reads what is available now into `buf`: the bytes read, 0 if nothing is, or -1 at end of stream. */
  def readNow(handle: Int, buf: ByteBuffer): Int

  /** Writes what fits now from `buf`: the bytes written, or -1 if nothing fits. */
  def writeNow(handle: Int, buf: ByteBuffer): Int
}

/** Unix domain stream sockets, which the ops for IP stream sockets (`accept`, `connect`, `read`, `write`) also run on. */
trait UnixStreamHandles extends Handles {

  /** A stream socket to connect to `address`; perform `ops.connect` to wait until it is connected. */
  def connect(address: UnixAddress): Int

  /** A stream socket that will listen on `address` once `ops.listening` is performed on it, replacing a socket file
    * already there then.
    */
  def listener(address: UnixAddress): Int
}

/** Unix domain datagram sockets, whose packets `UnixDatagramOps` sends and receives. */
trait UnixDatagramHandles extends Handles {

  /** A datagram socket, bound to `local` and connected to `remote` where given. A client needs a `local` path so that
    * replies can reach it.
    */
  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null): Int
}

/** FIFOs, waited on as pipes are. */
trait FifoHandles extends Handles {

  /** Opens a FIFO for reading or writing without blocking. Opening one for writing fails while it has no reader. */
  def openFifo(path: String, write: Boolean): Int
}

/** A mutable outcome of an operation, written by the op and taken by the caller with `clear`. Like a buffer, it belongs
  * to the reactor while the op is pending: the caller clears it only after the op's completion has reached them, and
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

/** A `Slot` for a handle, which holds it unboxed. Handles are never negative, so -1 marks it empty; otherwise it has
  * the same contract as `Slot`.
  */
final class HandleSlot {
  private var handle = HandleSlot.Empty

  def clear(): Int = {
    val h = handle
    if h == HandleSlot.Empty then throw new IllegalStateException("Nothing has been written to this slot yet")
    handle = HandleSlot.Empty
    h
  }

  private[asyncio] def set(handle: Int): Unit = {
    require(handle >= 0, s"Not a handle: $handle")
    if this.handle != HandleSlot.Empty then throw new IllegalStateException("Slot was already set")
    this.handle = handle
  }
}

object HandleSlot {
  private inline val Empty = -1
}

/** Marks an op that may be submitted again after its previous submission finished, instead of building a new one each
  * time: an accept loop can resubmit one accept op, and a connection can resubmit one read on its buffer. Anything an
  * op reports, such as `Accepted.accepted`, reflects its latest submission, so read it in that submission's completion.
  */
trait Repeatable

enum AddressFamily {
  case IPv4, IPv6
}

final case class ResolvedAddress(family: AddressFamily, host: String)

/** The ops a backend can build, as values of its `Op` type, on I/O objects named by its handles (see `Handles`).
  * Buffers are used between position and limit, and belong to the backend while the op is being performed: on a
  * reactor, from submission until the op's completion.
  */
trait Ops[Op] {

  /** Fills `buf`, which must have room. Completes once at least one byte has been read, or at end of stream, when
    * nothing was added: compare the buffer's position before and after.
    */
  def read(handle: Int, buf: ByteBuffer): Op & Repeatable

  /** Drains `buf`. Completes once at least one byte has been written; resubmit while `buf` has bytes remaining. */
  def write(handle: Int, buf: ByteBuffer): Op & Repeatable

  /** Accepts one connection on a listening socket. Completes after writing the new connection into `into`, which must
    * be empty.
    */
  def accept(handle: Int, into: HandleSlot): Op & Repeatable

  /** Connects `handle`, a socket from `handles.connect`. Completes once connected, or fails. */
  def connect(handle: Int): Op

  /** Receives one packet on an IP datagram socket into `buf`, which is left flipped for reading, and writes its sender
    * into `from`, which must be empty. Completes once a packet has arrived; its size is `buf.remaining`, which may be 0.
    */
  def receive(handle: Int, buf: ByteBuffer, from: Slot[IpAddress]): Op & Repeatable

  /** Sends `buf` as one packet on an IP datagram socket, to `to`. Completes once the whole packet has been sent,
    * consuming `buf`.
    */
  def send(handle: Int, buf: ByteBuffer, to: IpAddress): Op & Repeatable

  /** Sends `buf` as one packet on a connected datagram socket, of any family, to its peer, as the other `send`. */
  def send(handle: Int, buf: ByteBuffer): Op & Repeatable

  /** Completes once `milliseconds` have passed. */
  def timer(milliseconds: Int): Op & Repeatable

  /** Resolves a host name, without blocking a reactor's thread. Completes after writing its addresses, in numeric form,
    * into `into`, which must be empty; fails with an `IOException` if the name cannot be resolved. A cancel that
    * arrives before the write leaves `into` empty.
    */
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Op

  /** Fills a buffer of `pool`'s, from `Backend.pool`, as the other `read` fills the caller's: completes once at least
    * one byte has been read, or at the end of the stream, writing what it read into `into`, which must be empty, as a
    * `Received` to release. Fails if `pool` has no buffer free when the read needs one. A completion-based backend,
    * such as Network.framework's or io_uring's, receives straight into the pool's buffer and hands it over, where a
    * read into the caller's buffer costs it a copy.
    */
  def read(handle: Int, pool: BufferPool, into: Slot[Received]): Op & Repeatable

  /** Completes once `handle`, from `Handles.listener`, is listening, or fails, as `connect` does for a socket to
    * connect: with an `IOException` if the address is in use or cannot be bound.
    */
  def listening(handle: Int): Op

  /** Closes `handle`, completing once it is closed. The ops pending on it, on whichever reactor they are, have failed
    * with an `IOException` (a closed channel's) before this completes, their completions getting `onFailure` from
    * that reactor's loop and a `perform` waiting on one throwing it, as the JDK fails a thread blocked on a channel
    * that is closed; its number is then free for reuse. A Unix socket path that a listener or datagram socket bound is
    * removed with it. Fails if `handle` is not open.
    */
  def close(handle: Int): Op
}

/** Readiness, for a backend that has it (see `ReadinessBackend`). */
trait ReadinessOps[Op] extends Ops[Op] {

  /** Waits until `handle` is ready for `interest`, then runs `perform` on the reactor thread. The op completes when
    * `perform` returns true; false means it was not ready after all, and the reactor waits again. For operations the
    * other ops do not cover, such as draining a FIFO.
    */
  def whenReady(handle: Int, interest: Interest)(perform: () => Boolean): Op & Repeatable
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

/** The ops of Unix domain datagram sockets, as `Ops.receive` and `Ops.send` are for IP ones. */
trait UnixDatagramOps[Op] extends Ops[Op] {

  /** Receives one packet on a Unix datagram socket, as `receive` does on an IP one; an unnamed sender has the empty
    * path.
    */
  @annotation.targetName("receiveUnix")
  def receive(handle: Int, buf: ByteBuffer, from: Slot[UnixAddress]): Op & Repeatable

  /** Sends one packet on a Unix datagram socket, to `to`, as `send` does on an IP one. */
  def send(handle: Int, buf: ByteBuffer, to: UnixAddress): Op & Repeatable
}
