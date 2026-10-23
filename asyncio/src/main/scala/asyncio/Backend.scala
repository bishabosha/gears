package asyncio

import _root_.gears.async.Async

/** A way of doing I/O: the ops it can perform, the handles they name, and the pools its reads fill. How an op is
  * performed is its kind's: a reactor backend submits its ops with completions on reactors it opens
  * (`ReactorBackend`), and a program over it runs on a pool of workers driving them; a caller backend does each op
  * itself on the calling thread, as NIO's `NioOnCaller` does with non-blocking channels from a thread that may block
  * (`CallerBackend`). Ops of another backend are a different type. Building an op or using a handle performs nothing,
  * so either may be done from any thread, and a backend performs nothing itself: a program performs its ops with an
  * `Io` over the backend (`Io.backend`), which says where they run.
  *
  * What every backend has is here: streams, read and written, into the caller's buffers or into buffers of the
  * backend's (`pool`), timers, and handles closed as ops (`Ops.close`), since a backend whose I/O facility closes
  * asynchronously, such as Network.framework or io_uring, reports a close done through a completion rather than by
  * blocking the caller. Where the streams come from is a capability the backend mixes in, with its own handles and
  * ops: IP sockets (`IpSockets`), regular files (`Files`), pipes (`Pipes`), name resolution (`Resolving`), readiness
  * (`Readiness`), blocking ops (`Blocking`), `UnixStreams`, `UnixDatagrams` and `Fifos`; its kind is a reactor
  * (`ReactorBackend`) or the caller (`CallerBackend`). Code that needs a capability asks for a backend that has it
  * (`SocketBackend`, a `Backend & IpSockets`, and the other aliases below), or for an `Io` over one
  * (`Io[SocketBackend]`), so a backend without it, such as one in a browser, which has no sockets or files of these
  * kinds, is refused when compiling, not when running.
  */
trait Backend {

  /** What this performs. */
  type Op

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
  * `NioOnCaller`. Its ops may be performed from anywhere, so a program over it needs no runner: its `Io` is an
  * `OnCaller` over it, which anyone may make.
  */
trait CallerBackend extends Backend {

  /** Performs `op` on the calling thread, as `Io.perform` says: what `OnCaller.perform` calls. */
  private[asyncio] def performHere(op: Op, timeoutMillis: Int)(using Async): Unit
}

/** A backend whose ops run on reactors, `Reactor`s it opens: an op built by `ops` may be submitted to any of them, one
  * submission at a time, so that a task moving from one reactor's thread to another's can take its ops along: once a
  * submission has completed, failed or been cancelled on one reactor, a `Repeatable` op may go to another. A program
  * over this backend runs on a pool of workers driving its reactors, started by the platform's runner, and performs
  * its ops with the `OnPool` the runner gives it, which submits each to the reactor of the worker running the caller.
  */
trait ReactorBackend extends Backend with Reactor.Factory[Reactor] {

  /** Opens a reactor that runs this backend's ops. Close it when done. */
  def open(): Reactor.Of[Op]
}

/** IP sockets, stream and datagram (see `SocketHandles` and `SocketOps`): an operating system's, or a host's, such as
  * Node's; a browser has none.
  */
trait IpSockets extends Backend {
  def ops: SocketOps[Op]
  def handles: SocketHandles
}

/** Regular files, opened, read, written and synced as ops (see `FileOps`): on a thread pool where the operating
  * system's file I/O blocks, as kqueue's, NIO's and Network.framework's do, asynchronously at the device where it
  * does not, as io_uring's would.
  */
trait Files extends Backend {
  def ops: FileOps[Op]
}

/** Host names resolved (see `ResolveOps`). */
trait Resolving extends Backend {
  def ops: ResolveOps[Op]
}

/** Pipes (see `PipeHandles`). */
trait Pipes extends Backend {
  def handles: PipeHandles
}

/** Saying when a handle is ready, and using it without waiting (see `ReadinessOps` and `ReadinessHandles`): over
  * non-blocking descriptors whose reactor is the only thread to touch them, such as kqueue's.
  */
trait Readiness extends Backend {
  def ops: ReadinessOps[Op]
  def handles: ReadinessHandles
}

/** Blocking code run as an op (see `BlockingOps`): where it may block, with the op's time limit and cancellation
  * interrupting it, the same whichever the kind. On a reactor backend that is off the reactors' threads, which keeps
  * them free; on a caller backend it is the calling thread, which may block anyway, so there the op adds only the
  * limit and the interrupt.
  */
trait Blocking extends Backend {
  def ops: BlockingOps[Op]
}

/** Unix domain stream sockets (see `UnixStreamHandles`), on which the socket ops run. */
trait UnixStreams extends IpSockets {
  def handles: UnixStreamHandles
}

/** Unix domain datagram sockets (see `UnixDatagramHandles` and `UnixDatagramOps`). */
trait UnixDatagrams extends IpSockets {
  def ops: UnixDatagramOps[Op]
  def handles: UnixDatagramHandles
}

/** FIFOs waited on as pipes are (see `FifoHandles`). */
trait Fifos extends Backend {
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
