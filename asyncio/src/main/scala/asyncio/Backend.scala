package asyncio


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
  * blocking the caller. Where the streams come from is a capability the backend mixes in, with its own handles and ops
  * (`Facilities`): IP sockets (`IpSockets`), regular files (`Files`), pipes (`Pipes`), name resolution (`Resolving`),
  * readiness (`Readiness`), blocking ops (`Blocking`), `UnixStreams`, `UnixDatagrams` and `Fifos`; its kind is a
  * reactor (`ReactorBackend`) or the caller (`CallerBackend`). Code that needs a capability asks for a backend that
  * has it (`SocketBackend`, a `Backend & IpSockets`, and the other aliases below), so a backend without it, such as
  * one in a browser, which has no sockets or files of these kinds, is refused when compiling, not when running; a
  * helper over a program's `Io` asks for the capability itself (`Io & IpSockets`).
  */
trait Backend extends Facilities

/** What a backend has, and what a program's `Io` carries of it: the ops it builds, the handles they name, and the
  * pools its reads fill. A `Backend` is one, with a kind that says how its ops run; an `Io` is one, with `perform`;
  * each capability is one, narrowed to its own ops and handles, and is mixed into both. So a backend and its `Io`
  * share their capabilities and nothing else: an `Io` is not a `Backend`, and cannot be taken for one.
  */
trait Facilities {

  /** What this performs. */
  type Op

  /** What a program over this backend performs its ops with: this backend's ops, handles and capabilities, bound to
    * where the program runs (see `Io`): on a pool of its reactors for a reactor backend (`OnPool`), on
    * the calling thread for a caller backend (`OnCaller`). Each capability narrows it to itself, so a program over a
    * `SocketBackend & FileBackend` is given an `Io & IpSockets & Files`. Made by the platform's runner for the
    * program it starts, and by nothing else, so that an op is only ever performed where it can be.
    */
  type Io <: asyncio.Io { type Op = Facilities.this.Op }

  /** Builds this backend's ops. */
  def ops: Ops[Op]

  /** Opens, uses, and closes the handles this backend's ops name. */
  def handles: Handles

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
  type Io <: asyncio.OnCaller { type Op = CallerBackend.this.Op }

  /** This backend as a program's `Io`: itself. */
  private[asyncio] def bind(): Io

}

/** A backend whose ops run on reactors, `Reactor`s it opens: an op built by `ops` may be submitted to any of them, one
  * submission at a time, so that a task moving from one reactor's thread to another's can take its ops along: once a
  * submission has completed, failed or been cancelled on one reactor, a `Repeatable` op may go to another. A program
  * over this backend runs on a pool of workers driving its reactors, started by the platform's runner, and performs
  * its ops with the `OnPool` it is given, which submits each to the reactor of the worker running the caller (see
  * `OnPool`).
  */
trait ReactorBackend extends Backend with Reactor.Factory[Reactor] {

  /** Opens a reactor that runs this backend's ops. Close it when done. */
  def open(): Reactor.Of[Op]

  type Io <: asyncio.OnPool { type Op = ReactorBackend.this.Op }

  /** This backend bound to `pool`, a pool of its own reactors, for the program the runner starts on it. */
  private[asyncio] def bind(pool: Pool.Of[Op]): Io

}

/** IP sockets, stream and datagram (see `SocketHandles` and `SocketOps`): an operating system's, or a host's, such as
  * Node's; a browser has none.
  */
trait IpSockets extends Facilities {
  type Io <: asyncio.Io & IpSockets { type Op = IpSockets.this.Op }
  def ops: SocketOps[Op]
  def handles: SocketHandles
}

/** Regular files, opened, read, written and synced as ops (see `FileOps`): on a thread pool where the operating
  * system's file I/O blocks, as kqueue's, NIO's and Network.framework's do, asynchronously at the device where it
  * does not, as io_uring's would.
  */
trait Files extends Facilities {
  type Io <: asyncio.Io & Files { type Op = Files.this.Op }
  def ops: FileOps[Op]
}

/** Host names resolved (see `ResolveOps`). */
trait Resolving extends Facilities {
  type Io <: asyncio.Io & Resolving { type Op = Resolving.this.Op }
  def ops: ResolveOps[Op]
}

/** Pipes (see `PipeHandles`). */
trait Pipes extends Facilities {
  type Io <: asyncio.Io & Pipes { type Op = Pipes.this.Op }
  def handles: PipeHandles
}

/** Saying when a handle is ready, and using it without waiting (see `ReadinessOps` and `ReadinessHandles`): over
  * non-blocking descriptors whose reactor is the only thread to touch them, such as kqueue's.
  */
trait Readiness extends Facilities {
  type Io <: asyncio.Io & Readiness { type Op = Readiness.this.Op }
  def ops: ReadinessOps[Op]
  def handles: ReadinessHandles
}

/** Blocking code run as an op (see `BlockingOps`): where it may block, with the op's time limit and cancellation
  * interrupting it, the same whichever the kind. On a reactor backend that is off the reactors' threads, which keeps
  * them free; on a caller backend it is the calling thread, which may block anyway, so there the op adds only the
  * limit and the interrupt.
  */
trait Blocking extends Facilities {
  type Io <: asyncio.Io & Blocking { type Op = Blocking.this.Op }
  def ops: BlockingOps[Op]
}

/** Unix domain stream sockets (see `UnixStreamHandles`), on which the socket ops run. */
trait UnixStreams extends IpSockets {
  type Io <: asyncio.Io & UnixStreams { type Op = UnixStreams.this.Op }
  def handles: UnixStreamHandles
}

/** Unix domain datagram sockets (see `UnixDatagramHandles` and `UnixDatagramOps`). */
trait UnixDatagrams extends IpSockets {
  type Io <: asyncio.Io & UnixDatagrams { type Op = UnixDatagrams.this.Op }
  def ops: UnixDatagramOps[Op]
  def handles: UnixDatagramHandles
}

/** FIFOs waited on as pipes are (see `FifoHandles`). */
trait Fifos extends Facilities {
  type Io <: asyncio.Io & Fifos { type Op = Fifos.this.Op }
  def handles: FifoHandles
}

/** A backend with IP sockets: what a program asks for, and the other aliases likewise. */
type SocketBackend = Backend & IpSockets
type FileBackend = Backend & Files
type ResolveBackend = Backend & Resolving
type PipeBackend = Backend & Pipes
type ReadinessBackend = Backend & Readiness
type BlockingBackend = Backend & Blocking
type UnixStreamBackend = Backend & UnixStreams
type UnixDatagramBackend = Backend & UnixDatagrams
type FifoBackend = Backend & Fifos
