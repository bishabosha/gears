package asyncio

import _root_.gears.async.Async
import _root_.gears.async.AsyncOperations

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
    * backend's `Io`, which is also its `AsyncOperations` (see `ReactorFutures.run`, which calls this).
    */
  private[asyncio] def start(runner: asyncio.gears.Runner, workers: Int, policy: asyncio.gears.TakePolicy)(
      program: Io => (Async.Spawn, AsyncOperations) ?=> Unit
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
      program: Io => (Async.Spawn, AsyncOperations) ?=> Unit
  ): Unit = runner.onCaller(this)(program)
}

/** A backend whose ops run on reactors, `Reactor`s it opens: an op built by `ops` may be submitted to any of them, one
  * submission at a time, so that a task moving from one reactor's thread to another's can take its ops along: once a
  * submission has completed, failed or been cancelled on one reactor, a `Repeatable` op may go to another. A program
  * over this backend runs on a pool of workers driving its reactors, started by `ReactorFutures.run`, and performs its
  * ops with the `OnPool` it is given, which submits each to the reactor of the worker running the caller (see
  * `asyncio.gears.OnPool`).
  */
trait ReactorBackend extends Backend with Reactor.Factory[Reactor] {

  /** Opens a reactor that runs this backend's ops. Close it when done. */
  def open(): Reactor.Of[Op]

  type Io <: asyncio.gears.OnPool { type Op = ReactorBackend.this.Op }

  /** This backend bound to a pool whose scheduler is `scheduler`, for the program the runner starts on it. */
  private[asyncio] def bind(scheduler: asyncio.gears.ReactorScheduler): Io

  private[asyncio] final def start(runner: asyncio.gears.Runner, workers: Int, policy: asyncio.gears.TakePolicy)(
      program: Io => (Async.Spawn, AsyncOperations) ?=> Unit
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
