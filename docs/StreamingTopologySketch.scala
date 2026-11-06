package gears.streaming.sketch

import gears.async.{Async, ChannelClosedException, ReadableChannel, SendableChannel}
import java.nio.ByteBuffer

/* Revision of the initial push-only sketch for channel interop, graph topology
 * and protocol implementations. Sources can provide native pull access as well
 * as fused traversal; one public Stream type does not imply one universal
 * execution strategy. Methods marked ??? are design proposals, not runtimes.
 *
 * A cold plan, an active port and a shared owner are different things:
 *   Stream[A]       lazy computation / possibly consumptive source description
 *   Reader/Writer   active endpoints, valid inside the owner's use callback
 *   Scoped[Outputs] one materialization owning tasks, queues and shared cleanup
 *
 * Materializing obtains Async NOW. Plans, endpoint fields and callback adapters
 * do not store it. Every suspend/read/send call uses its execution fiber's Async.
 * Scoped uses may later be backed by Resource, but have no unstructured allocated
 * escape hatch. Capture annotations and negative compilation tests are pending.
 */

enum StreamSignal[+A]:
  case Item(value: A)
  case End
  case Failed(cause: Throwable)

/** Single-consumer pull endpoint. EOF/failure are sticky; cancellation throws
  * through Async, never masquerades as End. read consumes one item, not one
  * transport read. StreamSignal is chunk/item-level, not a per-byte parser API.
  */
trait Reader[+A]:
  def read()(using Async): StreamSignal[A]

  /** Consumes this SAME endpoint, not a replayable source. Construction retains
    * the reader, not Async; validity remains bounded by its owner's scope.
    */
  final def stream: Stream[A] =
    val upstream = this
    new Stream[A]:
      def traverse(consume: A => Async ?=> Demand)(using Async): Unit =
        var running = true
        while running do
          checkReaderCancellation()
          upstream.read() match
            case StreamSignal.Item(value) => running = consume(value) == Demand.More
            case StreamSignal.End         => running = false
            case StreamSignal.Failed(err) => throw err

/** An OPTIONAL capability of original event sources, notably channels. Does not
  * promise selectability for arbitrary mapped/effectful streams. Selection
  * commits an item atomically; this is not a readiness-then-read protocol.
  */
trait SelectableReader[+A] extends Reader[A]:
  val readSource: Async.Source[StreamSignal[A]]
  final def read()(using Async): StreamSignal[A] = readSource.awaitResult

trait Writer[-A]:
  def send(value: A)(using Async): Unit

  final def sink: Sink[A, Unit] =
    val downstream = this
    Sink.foreach(a => downstream.send(a))

/** Closed/failure outcomes are decoded AFTER awaiting, away from listener locks.
  * Like SelectableReader, this exposes only cheap transport arbitration.
  */
trait SelectableWriter[-A] extends Writer[A]:
  def sendSource(value: A): Async.Source[Either[Throwable, Unit]]
  final def send(value: A)(using Async): Unit =
    sendSource(value).awaitResult match
      case Right(_)  => ()
      case Left(err) => throw err

/** Authority to finish a stream-owned producer. Graceful finish/fail is ordered
  * AFTER all accepted values; neither should require enqueueing a terminal
  * marker into a full abandoned data queue. The owner can abort separately to
  * wake blocked tasks and discard queued values. Multiple producer completion
  * must be coordinated by the owner, not a close call from every worker.
  */
trait Producer[-A] extends Writer[A]:
  def finish()(using Async): Unit
  def fail(cause: Throwable)(using Async): Unit

trait Scoped[+A]:
  self =>
  def use[R](body: A => Async ?=> R)(using Async): R

  /** Wire/transform active outputs without changing their shared owner. f runs
    * at materialization; constructing this derived plan needs no Async.
    */
  final def map[B](f: A => B): Scoped[B] = new Scoped[B]:
    def use[R](body: B => Async ?=> R)(using Async): R =
      self.use(a => body(f(a)))

private def checkReaderCancellation()(using a: Async): Unit =
  if a.group.isCancelled then throw new java.util.concurrent.CancellationException()

object channels:
  /** Borrowed endpoint; neither reader abandonment nor EOF closes the channel.
    * This tiny source adapter only tags native outcomes: no user transformation
    * or suspending code runs in Listener.complete. Native select remains usable.
    */
  def reader[A](channel: ReadableChannel[A]): SelectableReader[A] = new SelectableReader[A]:
    val readSource: Async.Source[StreamSignal[A]] = channel.readSource.transformValuesWith:
      case Right(value) => StreamSignal.Item(value)
      case Left(_)      => StreamSignal.End

  def writer[A](channel: SendableChannel[A]): SelectableWriter[A] = new SelectableWriter[A]:
    def sendSource(value: A): Async.Source[Either[Throwable, Unit]] =
      channel.sendSource(value).transformValuesWith:
        case Right(_) => Right(())
        case Left(_)  => Left(new ChannelClosedException())

  def fromChannel[A](channel: ReadableChannel[A]): Stream[A] = reader(channel).stream
  def toChannel[A](channel: SendableChannel[A]): Sink[A, Unit] = writer(channel).sink

  /** Explicit protocol adapter when upstream controls the envelope. Natural End
    * travels in-band and DOES NOT close the native channel. Cancel/failure
    * teardown belongs to its owner. A channel closed without End is a truncated
    * stream and must fail rather than report successful EOF. No adapter can
    * recover queued values after the native channel has already discarded them.
    */
  def framed[A](channel: ReadableChannel[StreamSignal[A]]): SelectableReader[A] = ???

  /** A new owned pipe, not a wrapper silently claiming ownership of a caller's
    * channel. capacity=0 uses rendezvous semantics. Data-plane interop can reuse
    * Gears channel matching, but graceful terminal state must be ordered with
    * buffered items. Current Channel.close is abortive w.r.t. its backlog.
    *
    * Acquisition returns endpoints before any data is consumed. On scope exit,
    * abort/wake users, join owned work, dispose queued resources. Closing a
    * borrowed channel is never an implicit side effect of these other adapters.
    */
  def pipe[A](capacity: Int): Scoped[(SelectableReader[A], Producer[A])] = ???

/** Validate limits at plan construction. Bounds queued values + pending
  * handoffs inside the bridge, up to transfer to a consumer. Reader.read does
  * not track values the application retains after receiving them. End-to-end
  * in-flight limits need a separate Delivery acknowledgement (see below).
  * Reserve BEFORE enqueue/read/allocation where the source permits it.
  * Reject a value larger than the entire budget. Arbitrarily sized source items
  * cannot be bounded before reading unless the source supplies a size contract.
  */
final case class QueueLimit[A](maxItems: Int, maxWeight: Long, weight: A => Long):
  require(maxItems > 0 && maxWeight > 0)

enum BranchEnd:
  /** Stop delivering to an abandoned branch; continue until all detach. */
  case Detach
  /** Any early branch completion stops the shared graph successfully. */
  case StopAll

object topology:
  /** Each value to EVERY attached branch; slowest branch back-pressures source.
    * Input runs exactly once. Branches are installed before the producer starts.
    * Leaving an individual consume call must NOT silently detach: an endpoint
    * can be read again in the same scope. Detach is an explicit operation via
    * Branch below, or occurs at owner exit. A branch failure fails the graph.
    */
  def broadcast[A](source: Stream[A], branches: Int, perBranch: QueueLimit[A], onEnd: BranchEnd): Scoped[Vector[Branch[A]]] = ???

  /** Each value to ONE ready worker; no duplication. Slow workers do not block
    * ready ones until the aggregate admission budget is exhausted. No global
    * output ordering promise. An accepted job is not retried automatically if
    * its worker fails, since its side effects may already have happened.
    */
  def balance[A](source: Stream[A], workers: Int, shared: QueueLimit[A]): Scoped[Vector[Branch[A]]] = ???

  /** Each value to its chosen branch (index in 0 until branches). Back-pressure
    * on that destination is part of the contract, unlike load balancing.
    * This generic operation is NOT an HTTP/2 connection demultiplexer: a full
    * data branch must not prevent that protocol from processing control frames.
    */
  def route[A](source: Stream[A], branches: Int, perBranch: QueueLimit[A])(
      destination: A => Int
  ): Scoped[Vector[Branch[A]]] = ???

  /** Per-input order, fair arbitration among ready inputs, EOF after all inputs.
    * One failure fails the merge and cancels/joins every owned input. Fairness
    * requires rotating arbitration, not an always-left-biased select loop.
    */
  def merge[A](inputs: Vector[Stream[A]], shared: QueueLimit[A]): Stream[A] = ???

  /** End with the shorter input; abandon and join the other. Native readers
    * avoid a producer bridge; push-only sources pay for materializing readers.
    */
  def zip[A, B](left: Stream[A], right: Stream[B], capacity: Int): Stream[(A, B)] = ???

  /** Dynamic fan-in with at most maxOpen live child streams and a global output
    * budget. Opens/tears down each child within the running merge's owner.
    */
  def mergeMap[A, B](source: Stream[A], maxOpen: Int, shared: QueueLimit[B])(
      child: A => Async ?=> Stream[B]
  ): Stream[B] = ???

  /** Concurrently start ALL consumers; supervise the first failure immediately,
    * not by awaiting futures in branch order. Successful early returns call
    * branch.stop according to onEnd; cleanup never waits to send into a full
    * abandoned queue. Consumers returning without reading any data are valid.
    */
  def consume[A, R](graph: Scoped[Vector[Branch[A]]])(
      consumers: Vector[Reader[A] => Async ?=> R]
  )(using Async): Vector[R] = ???

trait Branch[+A]:
  def reader: Reader[A]
  /** Ends this consumer's participation, not the shared source's transport.
    * Idempotent. Wakes blocked deliveries and applies the graph's BranchEnd
    * policy. Further reads are invalid. Failure/cancellation uses owner policy.
    */
  def stop()(using Async): Unit

/** Explicit processing/byte credit, distinct from merely freeing a queue slot.
  * The consumer owns value until release, which returns its admission credit
  * and runs the owner's disposal action. Exactly once; no Async retained.
  * A live Delivery must not go through generic filter/broadcast adapters, which
  * do not know how to dispose it. Use a delivery-aware API or useNext below.
  */
trait Delivery[+A]:
  def value: A
  def release(): Unit

trait DeliveryReader[+A]:
  def read()(using Async): StreamSignal[Delivery[A]]

  /** Releases after processing, including exception/cancellation, so bounded
    * work includes the consumer callback. Returning false means graceful EOF.
    * Value and ownership token must not escape the callback's dynamic extent.
    */
  final def useNext(consume: A => Async ?=> Unit)(using Async): Boolean =
    read() match
      case StreamSignal.Item(delivery) =>
        try consume(delivery.value)
        finally delivery.release()
        true
      case StreamSignal.End         => false
      case StreamSignal.Failed(err) => throw err

/** A byte parser hot path distinct from a generic element/channel protocol.
  * Single owner; retain unread bytes across reads and across HTTP messages.
  * Implementations own a bounded buffer/pool and reusable op(s), never Async.
  * No per-byte Option, task, or channel handoff is required by these methods.
  */
trait ByteCursor:
  def available: Int
  /** Ensure minBytes are contiguous, false only when EOF leaves fewer. Bound
    * minBytes by configured cursor capacity; never silently grow without limit.
    */
  def fill(minBytes: Int)(using Async): Boolean
  /** Borrowed read-only view; invalidated by fill/advance/scope exit. A view
    * must not be retained in a Stream or sent to another worker without copying
    * or obtaining an explicit lease. Read-only does not mean independently owned.
    */
  def view: ByteBuffer
  def advance(bytes: Int): Unit

object byteInputs:
  /** Natural I/O pull source. Read op and buffer are bound together for their
    * lifetime. A compact/refill may copy leftovers; it is not claimed to be
    * universally zero-copy. Borrowed handle remains the caller's to close.
    */
  def borrowed[R](using io: gears.async.io.Io[gears.async.io.Backend])(
      handle: io.Readable, bufferBytes: Int
  )(use: ByteCursor => Async ?=> R)(using Async): R = ???

/* Graph/HTTP architecture constraints:
 *
 * - Bounded materialization is a first-class facility, not a cold pair of
 *   streams that independently start one shared producer. Scoped[Tuple] can
 *   later generalize the vector examples without restoring four stream families.
 * - A consumer may not sequentially drain all bounded broadcast branches: it
 *   would wait for a producer blocked on another branch. Use topology.consume
 *   or explicitly start and supervise all consumers together. No silent drops;
 *   lossy sampling/taps require a separately named policy.
 * - Define value ownership separately from topology. The generic examples use
 *   caller-shareable values / immutable ByteChunk. They do NOT make an arbitrary
 *   Stream[Received] or Stream[Lease] safe: filter/take/discard and fan-out need
 *   explicit retain/release/disposal semantics before leases can enter them.
 *   A future lease-aware path must release queued, cancelled and filtered chunks
 *   and allocate one ownership token per broadcast recipient. Do not bolt a
 *   blanket no-op disposer onto arbitrary A and claim this is solved.
 * - The byte cursor permits parsing/framing without copying every transport
 *   read into the public immutable-chunk representation. Owned body chunks are
 *   the simple initial API; a measured, explicit leased-body API is a separate
 *   requirement for eliminating copies across asynchronous body consumers.
 * - HTTP connection tasks belong to the client/server connection owner, not
 *   the first request's Async. Exchange/body ports have their own lifetimes.
 *   Bodies are consumptive scoped values; retry needs a distinct replay policy.
 * - Request/body abandonment invokes protocol cleanup; it does not automatically
 *   close a shared transport. Account admission until framing and cleanup finish.
 * - A connection dispatcher needs bounded byte credits and a control lane that
 *   remains live under data back-pressure. Frame writing needs fair per-stream
 *   scheduling. A generic route->bounded-channel loop cannot provide these
 *   guarantees by itself; protocol flow control owns its receive reservations.
 * - Avoid per-stage tasks/queues on the parser path. Measure channel contention,
 *   handoffs, allocation, buffer copies and cancellation latency before promising
 *   throughput. Generic stream ergonomics do not substitute for a protocol engine.
 * - Validate native channel semantics, graceful End, truncation, branch startup,
 *   early detach, slow branches, per-input order/fairness, failure wakeups, bounded
 *   admission, replay restrictions and scope cleanup. HTTP additionally needs
 *   protocol conformance, adversarial framing tests, real workloads and profiling.
 */

object ChannelExamples:
  /** Composition needs no Async; it is supplied by each eventual traversal. */
  def forwarded[A](in: ReadableChannel[A], out: SendableChannel[A]): (Stream[A], Sink[A, Unit]) =
    (channels.fromChannel(in), channels.toChannel(out))

  def fanOut(input: Stream[Int])(using Async): Vector[Int] =
    val limits = QueueLimit[Int](maxItems = 16, maxWeight = 16L, weight = _ => 1L)
    val graph = topology.broadcast(input, branches = 2, perBranch = limits, onEnd = BranchEnd.Detach)
    topology.consume(graph)(Vector(
      reader => reader.stream.fold(0)(_ + _),
      reader => reader.stream.fold(0)((count, _) => count + 1)
    ))
