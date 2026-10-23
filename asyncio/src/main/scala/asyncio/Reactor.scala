package asyncio

import java.nio.ByteBuffer

import _root_.gears.async.Async

/** What to do when an op finishes. The op comes back with its outcome, so one completion can own many ops. A completion
  * that submits further ops holds on to its submitter itself, since only a reference to that submitter has the right
  * `Op` type.
  */
@FunctionalInterface
trait Completion[-C] {

  /** The op finished. What finishing means for each kind of op is described in `Ops`; anything it produced is in the
    * caller's buffers and slots.
    */
  def onComplete(op: C): Unit

  /** The op failed with an exception instead of finishing. By default the failure propagates out of `Reactor.run`. */
  def onFailure(op: C, failure: Throwable): Unit = throw failure

  /** The op was cancelled before it finished, by `Submitter.cancel` or because the reactor closed. It runs on the
    * submitter's thread once the submitter has stopped using the op's buffers and slots, which are the caller's again,
    * and afterwards neither `onComplete` nor `onFailure` runs for this submission. It must not submit ops while the
    * reactor is closing. By default it does nothing.
    */
  def onCancel(op: C): Unit = ()
}

/** Where ops are submitted with completions, and whose thread runs the completions: a `Reactor`, whose loop the caller
  * drives, or, for a backend with no loop of its own, such as one over a JavaScript host, the host's loop. Apart from
  * what says otherwise, a submitter is used from its own thread only.
  */
trait Submitter {

  /** Something submitted here that later completes. The submitters of one `CompletionBackend` share it, so an op built
    * by the backend may go to any of them (see `ReactorBackend`); otherwise the type belongs to the submitter value:
    * given `a, b: Submitter`, `a.Op` and `b.Op` are different types.
    */
  type Op

  /** Builds this submitter's ops, as its backend's `ops` does. Building an op does not touch the submitter, so `ops`
    * may be used from any thread.
    */
  def ops: Ops[Op]

  /** Opens, uses without waiting, and closes this submitter's handles, as its backend's `handles` does. It does not
    * touch the submitter either, so it may be used from any thread.
    */
  def handles: Handles

  /** Submits an op with the completion to run when it finishes, and no time limit (see the other `submit`). */
  final def submit[C <: Op](op: C, completion: Completion[C]): Unit = submit(op, completion, Backend.NoTimeout)

  /** Submits an op with the completion to run when it finishes, and how long it may take. An op marked `Repeatable`
    * may be submitted again once its previous submission has completed, failed, or been cancelled; any other op may be
    * submitted only once. No op may be pending twice at the same time. An op may finish during `submit`, such as a read
    * of data already waiting, but its completion never runs inside `submit`: it runs from a later turn of the loop.
    * Nor need the op be issued to the I/O facility inside `submit`: a submitter may hold the ops submitted in a turn
    * and issue them together at the turn's end, as io_uring's submission queue or Network.framework's batches invite,
    * so long as `trySubmit` issues at once and a turn issues everything before it blocks.
    *
    * Unless `timeoutMillis` is negative (`Backend.NoTimeout`), an op not finished that many milliseconds after its
    * submission is stopped, as `cancel` stops it, and fails with `java.util.concurrent.TimeoutException`: its
    * completion gets `onFailure` with that, from the loop, once the submitter has stopped using its buffers and slots,
    * which for an op running elsewhere, such as a blocking op that has to be interrupted, is later. An op that waits
    * for its handle to be ready before it touches anything, as the reads and writes of a readiness-based submitter
    * do, has then done nothing: its buffer is as it was and its handle as usable as before, so a read that timed out
    * is a wait that gave up, not a connection lost. This is how a read is given a timeout: no timer to race it
    * against, and no cancel to confirm. A timer whose own time is longer times out the same way. Zero gives the op
    * until the turn after its submission, as a timer of no time has.
    */
  def submit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Unit

  /** `op`'s attachment, as its backend's `attachment` returns it. */
  def attachment(op: Op): AnyRef | Null

  /** Sets `op`'s attachment, as its backend's `attach` does. */
  def attach(op: Op, value: AnyRef | Null): Unit

  /** `trySubmit` with no time limit. */
  final def trySubmit[C <: Op](op: C, completion: Completion[C]): Boolean =
    trySubmit(op, completion, Backend.NoTimeout)

  /** Like `submit`, but an op the submitter can finish at once, as it may for an operation that only reports
    * would-block when not ready, it finishes here: it returns true without ever calling `completion`, and the op's
    * results are already in the caller's buffers and slots; a failure is thrown. Otherwise the op is submitted as by
    * `submit`, with the same time limit, `completion` runs when it finishes, and this returns false. A caller can then
    * continue at once instead of waiting for a later turn, but must take care to let other work run if its ops keep
    * finishing at once.
    */
  def trySubmit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Boolean

  /** Cancels a submitted op that has not finished, and returns true: its completion's `onComplete` and `onFailure` will
    * never run, and its `onCancel` runs once the submitter has stopped using the op's buffers and slots. That is before
    * this returns for most ops, but later, from the loop, for one still running elsewhere, such as a blocking op that
    * has to be interrupted. Returns false if the op already finished or was never submitted here.
    */
  def cancel(op: Op): Boolean
}

object Submitter {

  /** A submitter that runs ops of type `O`: one of a backend whose `Op` is `O`. */
  type Of[O] = Submitter { type Op = O }
}

/** A single-threaded event loop: a `Submitter` whose loop the caller drives with `runOnce` or `run`. Every completion
  * runs on the thread that drives the loop, and apart from `wakeup`, a reactor is only used from that thread.
  */
trait Reactor extends Submitter with AutoCloseable {

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
    * caller and stay open (closing one instead fails the ops pending on it: see `Ops.close`). Call it when `run` is not
    * running; closing twice does nothing.
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

/** A way of doing I/O: the ops it can perform and the handles they name. How an op is performed is its kind's: a
  * reactor backend submits its ops with completions on reactors it opens (`ReactorBackend`), and a program over it
  * runs on a pool of workers driving them; a caller backend does each op itself on the calling thread, as NIO's
  * `NioOnCaller` does with non-blocking channels from a thread that may block (`CallerBackend`). A program performs
  * ops with its `Io`, the backend bound to where the program runs. Ops of another backend are a different type.
  * Building an op or using a handle performs nothing, so either may be done from any thread.
  *
  * What every backend has is here: streams, read and written, into the caller's buffers or into buffers of the
  * backend's (`pool`), timers, and handles closed as ops (`Ops.close`), since a backend whose I/O facility closes
  * asynchronously, such as Network.framework or io_uring, reports a close done through a completion rather than by
  * blocking the caller. Where the streams come from is a capability the backend mixes in, with its own handles and ops:
  * IP sockets (`SocketBackend`), regular files (`FileBackend`), pipes (`PipeBackend`), name resolution
  * (`ResolveBackend`), a reactor (`ReactorBackend`), readiness (`ReadinessBackend`), blocking ops (`BlockingBackend`),
  * `UnixStreamBackend`, `UnixDatagramBackend` and `FifoBackend`. Code that needs one asks for a backend that has it, so
  * a backend without it, such as one in a browser, which has no sockets or files of these kinds, is refused when
  * compiling, not when running.
  */
trait Backend {

  /** What this backend performs. */
  type Op

  /** What a program over this backend performs its ops with: this backend's ops, handles and capabilities, bound to
    * where the program runs (see `asyncio.gears.Io`): on a pool of its reactors for a reactor backend (`OnPool`), on
    * the calling thread for a caller backend (`OnCaller`). Each capability narrows it to itself, so a program over a
    * `SocketBackend & FileBackend` is given an `Io & SocketBackend & FileBackend`. Made by `ReactorFutures.run` for
    * the program it starts, and by nothing else, so that an op is only ever performed where it can be.
    */
  type Io <: asyncio.gears.Io { type Op = Backend.this.Op }

  /** Builds this backend's ops. */
  def ops: Ops[Op]

  /** Opens, uses, and closes the handles this backend's ops name. */
  def handles: Handles

  /** Starts `program` over this backend with `runner`, the platform's way for a backend of this kind, giving it the
    * backend's `Io` (see `ReactorFutures.run`, which calls this).
    */
  private[asyncio] def start(runner: asyncio.gears.Runner, workers: Int, policy: asyncio.gears.TakePolicy)(
      program: Io => Async.Spawn ?=> Unit
  ): Unit

  /** Opens a pool of `buffers` buffers of up to `bufferSize` bytes each, for the reads into buffers of the backend's
    * (`Ops.read` with a pool). Close it once nothing of it is held.
    */
  def pool(bufferSize: Int, buffers: Int): BufferPool
}

object Backend {

  /** No time limit, for `Io.perform` and `Submitter.submit`: any negative value means the same. */
  inline val NoTimeout = -1
}

/** A backend that performs each op on the calling thread, which may block, as a virtual thread may: NIO's
  * `NioOnCaller`. It is its own `Io` (`OnCaller`), since its ops may be performed from anywhere, so a program over
  * it needs no runner.
  */
trait CallerBackend extends Backend {
  type Io <: asyncio.gears.OnCaller { type Op = CallerBackend.this.Op }

  /** This backend as a program's `Io`: itself. */
  private[asyncio] def bind(): Io

  private[asyncio] final def start(runner: asyncio.gears.Runner, workers: Int, policy: asyncio.gears.TakePolicy)(
      program: Io => Async.Spawn ?=> Unit
  ): Unit = runner.onCaller(this)(program)
}

/** A backend whose ops are submitted with completions, to a `Submitter` (see `Submitter.submit`): one whose ops run on
  * reactors it opens (`ReactorBackend`), or on a loop the backend does not own, such as a JavaScript host's, where the
  * one submitter is that loop. How a submitter is obtained is the backend's to say; what is here is what every such
  * backend's ops carry.
  */
trait CompletionBackend extends Backend {

  /** The caller's own data for `op`, kept across submissions and never read by a submitter, like kqueue's `udata` or
    * io_uring's `user_data`: for instance a completion to reuse each time `op` is submitted, so that submitting it
    * again allocates nothing. Null until set. Used by one thread at a time: the one submitting the op, which hands it
    * over, attachment included, when the op moves to another submitter's thread.
    */
  def attachment(op: Op): AnyRef | Null

  /** Sets `op`'s attachment (see `attachment`). */
  def attach(op: Op, value: AnyRef | Null): Unit
}

/** A backend whose ops run on reactors, `Reactor`s it opens: an op built by `ops` may be submitted to any of them, one
  * submission at a time, so that a task moving from one reactor's thread to another's can take its ops along: once a
  * submission has completed, failed or been cancelled on one reactor, a `Repeatable` op may go to another. A program
  * over this backend runs on a pool of workers driving its reactors, started by `ReactorFutures.run`, and performs its
  * ops with the `OnPool` it is given, which submits each to the reactor of the worker running the caller (see
  * `asyncio.gears.ReactorAsync`).
  */
trait ReactorBackend extends CompletionBackend with Reactor.Factory[Reactor] {

  /** Opens a reactor that runs this backend's ops. Close it when done. */
  def open(): Reactor.Of[Op]

  type Io <: asyncio.gears.OnPool { type Op = ReactorBackend.this.Op }

  /** This backend bound to a pool whose scheduler is `scheduler`, for the program the runner starts on it. */
  private[asyncio] def bind(scheduler: asyncio.gears.ReactorScheduler): Io

  private[asyncio] final def start(runner: asyncio.gears.Runner, workers: Int, policy: asyncio.gears.TakePolicy)(
      program: Io => Async.Spawn ?=> Unit
  ): Unit = runner.onPool(this, workers, policy)(program)
}

/** A backend with IP sockets, stream and datagram (see `SocketHandles` and `SocketOps`): one over an operating system's
  * sockets, or a host's, such as Node's; a browser has none.
  */
trait SocketBackend extends Backend {
  type Io <: asyncio.gears.Io & SocketBackend { type Op = SocketBackend.this.Op }
  def ops: SocketOps[Op]
  def handles: SocketHandles
}

/** A backend with regular files, opened, read, written and synced as ops (see `FileOps`): on a thread pool where the
  * operating system's file I/O blocks, as kqueue's, NIO's and Network.framework's do, asynchronously at the device
  * where it does not, as io_uring's would.
  */
trait FileBackend extends Backend {
  type Io <: asyncio.gears.Io & FileBackend { type Op = FileBackend.this.Op }
  def ops: FileOps[Op]
}

/** A backend that resolves host names (see `ResolveOps`). */
trait ResolveBackend extends Backend {
  type Io <: asyncio.gears.Io & ResolveBackend { type Op = ResolveBackend.this.Op }
  def ops: ResolveOps[Op]
}

/** A backend with pipes (see `PipeHandles`). */
trait PipeBackend extends Backend {
  type Io <: asyncio.gears.Io & PipeBackend { type Op = PipeBackend.this.Op }
  def handles: PipeHandles
}

/** A backend that can say when a handle is ready, and use it without waiting (see `ReadinessOps` and
  * `ReadinessHandles`): one over non-blocking descriptors or channels, such as kqueue's and NIO's.
  */
trait ReadinessBackend extends Backend {
  type Io <: asyncio.gears.Io & ReadinessBackend { type Op = ReadinessBackend.this.Op }
  def ops: ReadinessOps[Op]
  def handles: ReadinessHandles
}

/** A backend that runs blocking code as an op, off the threads that run its other ops (see `BlockingOps`), so that it
  * keeps them free: one whose ops run on reactors. A backend whose callers may block anyway has no need of it; they
  * call the blocking code themselves.
  */
trait BlockingBackend extends Backend {
  type Io <: asyncio.gears.Io & BlockingBackend { type Op = BlockingBackend.this.Op }
  def ops: BlockingOps[Op]
}

/** A backend with Unix domain stream sockets (see `UnixStreamHandles`), on which the socket ops run. */
trait UnixStreamBackend extends SocketBackend {
  type Io <: asyncio.gears.Io & UnixStreamBackend { type Op = UnixStreamBackend.this.Op }
  def handles: UnixStreamHandles
}

/** A backend with Unix domain datagram sockets (see `UnixDatagramHandles` and `UnixDatagramOps`). */
trait UnixDatagramBackend extends SocketBackend {
  type Io <: asyncio.gears.Io & UnixDatagramBackend { type Op = UnixDatagramBackend.this.Op }
  def ops: UnixDatagramOps[Op]
  def handles: UnixDatagramHandles
}

/** A backend that waits on FIFOs as it does on pipes (see `FifoHandles`). */
trait FifoBackend extends Backend {
  type Io <: asyncio.gears.Io & FifoBackend { type Op = FifoBackend.this.Op }
  def handles: FifoHandles
}

/** Buffers a backend fills for the reads into its own buffers, opened by `Backend.pool`. A pool may be used from any
  * thread. `bufferSize` is the most a read into it receives at once: a backend that allocates the pool's buffers fills
  * them, where one whose facility hands it chunks of its own, as Node's streams do, may hand such a chunk over as the
  * `Received` instead, so long as it is no larger. `buffers` bounds how many may be held at once, by pending reads and
  * unreleased `Received`s together, which makes the pool a window on the source: a backend that controls the source's
  * flow may stop taking bytes while none is free, so that a read waits rather than fails. `close` frees its buffers,
  * and must come once no read is pending on it and every `Received` from it has been released.
  */
trait BufferPool extends AutoCloseable {
  def bufferSize: Int
  def buffers: Int

  /** Buffers neither held by a pending read nor by an unreleased `Received`. */
  def available: Int

  def close(): Unit
}

/** Bytes a pooled read received: `buffer` is one of the pool's, positioned at 0 with its limit at the bytes received, 0
  * at the end of the stream. It is the caller's until `release` gives it back to the pool, which must happen exactly
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

/** Opening a backend's handles; `Ops.close` closes them. Every operation is synchronous and never blocks: a handle from
  * here is a name for an I/O object the backend has created, and whatever touches the network, the disk, or another
  * thread to make it usable is an op on it, such as `SocketOps.listening`; what no environment can create
  * synchronously, a regular file, is opened by an op alone (`FileOps.open`). Failures throw `java.io.IOException`.
  *
  * A `Handle` names an open file, socket, or other I/O object; what its number means belongs to the backend (see
  * `Handle`). Handles belong to the caller, who closes them with `Ops.close`; the backend never closes them. What every
  * backend opens is nothing: each kind of handle comes with the capability that has it (`SocketHandles`, `PipeHandles`,
  * and the rest).
  */
trait Handles

/** IP sockets, for a backend that has them (see `SocketBackend`). */
trait SocketHandles extends Handles {

  /** A stream socket to connect to `address`; perform `ops.connect` to wait until it is connected. */
  def connect(address: IpAddress): Handle

  /** A stream socket that will listen on `address` once `ops.listening` is performed on it: binding and listening
    * happen there, so an address in use fails that op, not this. With `reuseAddr`, the address can be bound while
    * connections from an earlier listener on it are still in `TIME_WAIT`, so a restarted server need not wait for them
    * to expire.
    */
  def listener(address: IpAddress, reuseAddr: Boolean = true): Handle

  /** A datagram socket, bound to `local` and connected to `remote` where given, which must be of the same family. A
    * backend whose facility binds asynchronously binds it when `ops.listening` is performed on it; one that binds here,
    * as the backends over POSIX sockets do, completes that op at once, so code that performs it is correct on both, and
    * an address in use fails whichever of the two the backend binds in.
    */
  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Handle
}

/** Pipes, for a backend that has them (see `PipeBackend`). */
trait PipeHandles extends Handles {

  /** A pipe, as its read end and its write end. */
  def pipe(): (Handle, Handle)
}

/** Using handles without waiting, for a backend whose handles are non-blocking (see `ReadinessBackend`). */
trait ReadinessHandles extends Handles {

  /** Reads what is available now into `buf`: the bytes read, 0 if nothing is, or -1 at end of stream. */
  def readNow(handle: Handle, buf: ByteBuffer): Int

  /** Writes what fits now from `buf`: the bytes written, or -1 if nothing fits. */
  def writeNow(handle: Handle, buf: ByteBuffer): Int
}

/** Unix domain stream sockets, which the ops for IP stream sockets (`accept`, `connect`, `read`, `write`) also run on.
  */
trait UnixStreamHandles extends SocketHandles {

  /** A stream socket to connect to `address`; perform `ops.connect` to wait until it is connected. */
  def connect(address: UnixAddress): Handle

  /** A stream socket that will listen on `address` once `ops.listening` is performed on it, replacing a socket file
    * already there then.
    */
  def listener(address: UnixAddress): Handle
}

/** Unix domain datagram sockets, whose packets `UnixDatagramOps` sends and receives. */
trait UnixDatagramHandles extends SocketHandles {

  /** A datagram socket, bound to `local` and connected to `remote` where given. A client needs a `local` path so that
    * replies can reach it.
    */
  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null): Handle
}

/** FIFOs, waited on as pipes are. */
trait FifoHandles extends Handles {

  /** Opens a FIFO for reading or writing without blocking. Opening one for writing fails while it has no reader. */
  def openFifo(path: String, write: Boolean): Handle
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

enum AddressFamily {
  case IPv4, IPv6
}

final case class ResolvedAddress(family: AddressFamily, host: String)

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
