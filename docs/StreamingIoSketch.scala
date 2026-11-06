package gears.streaming.sketch

import gears.async.{Async, ReadableChannel, uninterruptible}
import gears.async.io.*
import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import scala.concurrent.duration.FiniteDuration
import scala.util.boundary

/* D.7 interface sketch, based on async-io-streaming (c78dac1a).
 *
 * This is outside the build deliberately. The sequential core, collector, byte
 * reader, file reader and timer source have illustrative implementations. `???`
 * marks proposed interfaces whose runtime is still to be implemented.
 * JavaFlowSketch.scala and OxFlowSketch.scala sketch optional JVM interop.
 * StreamingTopologySketch.scala extends this first push-oriented sketch with
 * scoped readers, native channel interop and shared graph materialization.
 *
 * A Stream is a lazy description, not a running producer. Constructing one never
 * needs Async. Sources may bind a lexical Io; every traversal receives its own
 * Async and allocates its own ops, slots, buffers and state. Io's Repeatable is
 * the reusable-op marker referred to in the design brief.
 *
 * One synchronous traversal primitive keeps user code off Listener completion
 * threads. Sequential operators fuse; channels and child futures appear only
 * at explicit concurrent boundaries. Returning Stop unwinds owned resources.
 * No persistent source, operator, sink or callback adapter should store Async.
 * Context-function callbacks receive the execution/worker's current Async.
 *
 * These signatures alone do not enforce capture safety: a user callback can
 * explicitly close over an outer Async, and a scoped Emitter/Session can escape
 * without capture checking. A production version needs capture annotations and
 * compile-negative tests for these cases. Io-bound plans also stay within the
 * lifetime of Io.run; binding Io does not extend that lifetime.
 */

enum Demand:
  case More, Stop

trait Emitter[-A]:
  /** Synchronous, suspending delivery; valid only on this builder's execution
    * fiber and within its dynamic extent. Never retain or share with a child.
    */
  def emit(value: A)(using Async): Unit

def emit[A](value: A)(using out: Emitter[A], async: Async): Unit =
  out.emit(value)

abstract class Stream[+A]:
  self =>

  /** Calls consume serially, stops immediately on Stop, throws failures and
    * returns on normal EOF. Implementations must cooperate with cancellation.
    * All owned children must have stopped and resources closed before return.
    */
  private[sketch] def traverse(consume: A => Async ?=> Demand)(using Async): Unit

  /** Scoped pull view. Native pull sources should override this to expose their
    * own cursor without a producer fiber/channel. The fallback for an arbitrary
    * Stream.build needs one bounded producer bridge: a synchronous push stack
    * cannot be suspended between read calls for free. capacity>=0.
    * Leaving use abandons this reader and joins its owned producer, not a shared
    * transport underneath it. HTTP maps body abandonment to protocol cleanup.
    */
  def withReader[R](capacity: Int = 16)(use: Reader[A] => Async ?=> R)(using Async): R = ???

  final def map[B](f: A => B): Stream[B] = mapAsync(a => f(a))

  final def mapAsync[B](f: A => Async ?=> B): Stream[B] = new Stream[B]:
    def traverse(consume: B => Async ?=> Demand)(using Async): Unit =
      self.traverse(a => consume(f(a)))

  final def filter(p: A => Boolean): Stream[A] = new Stream[A]:
    def traverse(consume: A => Async ?=> Demand)(using Async): Unit =
      self.traverse(a => if p(a) then consume(a) else Demand.More)

  final def take(n: Long): Stream[A] =
    require(n >= 0)
    new Stream[A]:
      def traverse(consume: A => Async ?=> Demand)(using Async): Unit =
        var left = n // per traversal, never shared between runs
        if left > 0 then
          self.traverse: a =>
            left -= 1
            val next = consume(a)
            if left == 0 then Demand.Stop else next

  /** Ordinary `for a <- stream do ...` is already direct style. */
  final def foreach(f: A => Async ?=> Unit)(using Async): Unit =
    traverse: a =>
      f(a)
      Demand.More

  final def fold[S](initial: => S)(f: (S, A) => Async ?=> S)(using Async): S =
    var state = initial
    foreach(a => state = f(state, a))
    state

  final def run[R](sink: Sink[A, R])(using Async): R = sink.consume(self)

  /** Bounded prefetch. capacity=0 means rendezvous, not unbounded buffering.
    * Owns one child producer inside traverse's Async.group.
    */
  final def buffer(capacity: Int): Stream[A] = ???

  /** Manual or runtime-selected parallel processing, without changing map's
    * meaning. Fixed(n) requires n>0. Auto chooses min(runtime CPUs,maxWorkers).
    * maxInFlight>0 bounds all admitted work together: queued, executing and
    * completed-but-not-delivered, including ordered results. Reserve a credit
    * BEFORE pulling the next input; release it only after delivery/discard.
    * There is one producer reading upstream; workers never read one handle
    * concurrently. Each invokes f with its child Async, retaining lexical Io.
    * ordered=true orders results, not side effects executed within f.
    */
  final def mapPar[B](
      workers: Parallelism,
      maxInFlight: Int,
      ordered: Boolean = true
  )(f: A => Async ?=> B): Stream[B] = ???

  /** Each input is interpreted once. EOF after both; failure cancels both. */
  final def merge[B >: A](other: Stream[B], capacity: Int): Stream[B] = ???

  /** Shared upstream is activated ONCE and both sinks are started together.
    * Bounded queues make the slowest sink apply back-pressure. A sink finishing
    * early detaches its queue; when both finish, upstream is stopped. Failure
    * cancels all branches. No pair of independently runnable "hot" streams.
    */
  final def broadcastTo[L, R](left: Sink[A, L], right: Sink[A, R], capacity: Int)(using Async): (L, R) = ???

enum Parallelism:
  case Fixed(workers: Int)
  case Auto(maxWorkers: Int)

trait Sink[-A, +R]:
  def consume(source: Stream[A])(using Async): R

object Sink:
  def foreach[A](f: A => Async ?=> Unit): Sink[A, Unit] = new Sink[A, Unit]:
    def consume(source: Stream[A])(using Async): Unit = source.foreach(f)

object Stream:
  /** Handles one-to-many output and nested streams without public flatMap.
    * For example:
    *   Stream.build[String]:
    *     for path <- paths do
    *       for line <- text.lines(files.read(path), maxLineBytes = 1048576) do
    *         if line.nonEmpty then emit(line.trim)
    *
    * No producer fiber or channel is needed here. Stop is private control flow
    * through the synchronous builder stack, so nested finally blocks execute.
    * Do not catch control throwables in the builder. Asynchronous boundaries
    * must convert Stop to a protocol signal rather than move boundary labels
    * across fibers. In particular, never call this Emitter from mapPar workers.
    */
  def build[A](body: (Async, Emitter[A]) ?=> Unit): Stream[A] = new Stream[A]:
    def traverse(consume: A => Async ?=> Demand)(using current: Async): Unit =
      boundary[Unit]:
        val out = new Emitter[A]:
          def emit(value: A)(using Async): Unit =
            checkCancelled()
            if consume(value) == Demand.Stop then boundary.break(())
        body(using current, out)

  def fromIterable[A](values: Iterable[A]): Stream[A] = build:
    for value <- values do emit(value)

  /** Borrowed, consumptive input: stopping does not close the external channel.
    * Ordinary Channel.Closed means EOF; richer failure needs an explicit adapter.
    * Preserves the native channel's close semantics (including discarded backlog).
    */
  def fromChannel[A](channel: ReadableChannel[A]): Stream[A] = channels.fromChannel(channel)

  def unfold[S, A](initial: => S)(next: S => Async ?=> Option[(A, S)]): Stream[A] = ???

private def checkCancelled()(using current: Async): Unit =
  if current.group.isCancelled then throw CancellationException()

/** Immutable owned chunks may cross buffers and parallel boundaries freely.
  * A ByteBuffer.asReadOnlyBuffer of reused storage is NOT an owned chunk.
  */
final class ByteChunk private (private val data: Array[Byte]):
  def size: Int = data.length
  def toArray: Array[Byte] = data.clone()
  private[sketch] def view: ByteBuffer = ByteBuffer.wrap(data).asReadOnlyBuffer()

object ByteChunk:
  def copyOf(data: Array[Byte]): ByteChunk = new ByteChunk(data.clone())
  private[sketch] def take(buffer: ByteBuffer): ByteChunk =
    val owned = new Array[Byte](buffer.remaining())
    buffer.get(owned)
    new ByteChunk(owned)

// Each namespace asks for exactly the backend capability it needs. A backend
// with more capabilities works through Io's covariance; imports grant nothing.
// Existing-handle helpers put `using io` first to preserve its dependent types.
object bytes:
  /** Borrowed handle, caller closes it. Single consumer: replaying this plan
    * continues consuming that same handle, and concurrent runs are forbidden.
    */
  def read(using io: Io[Backend])(handle: io.Readable, chunkBytes: Int = 65536): Stream[ByteChunk] =
    require(chunkBytes > 0)
    new Stream[ByteChunk]:
      def traverse(consume: ByteChunk => Async ?=> Demand)(using Async): Unit =
        val buffer = ByteBuffer.allocate(chunkBytes)
        val read: io.Op & Repeatable = io.backend.ops.read(handle, buffer)
        var running = true
        while running do
          checkCancelled()
          buffer.clear()
          io.perform(read) // same op; current traversal's Async on EVERY call
          // This backend contract reports EOF by leaving position unchanged.
          if buffer.position() == 0 then running = false
          else
            buffer.flip()
            running = consume(ByteChunk.take(buffer)) == Demand.More

  /** Borrowed handle. One staging buffer + Repeatable write op per consume;
    * copy each chunk into it in pieces, perform until all bytes are drained,
    * then reuse. Serializes writes and handles partial writes. No Async stored.
    */
  def write(using io: Io[Backend])(handle: io.Writable, bufferBytes: Int = 65536): Sink[ByteChunk, Unit] = ???

object files:
  /** Cold: opens a fresh file and builds a fresh reusable sequential read op
    * for each traversal; closes on EOF, Stop, exception and cancellation.
    * Positional read ops capture an immutable offset, so reusing one of those
    * is NOT a way to advance through a file. Use the base sequential read here.
    */
  def read(path: String, chunkBytes: Int = 65536)(using io: Io[RegularFiles]): Stream[ByteChunk] =
    require(chunkBytes > 0)
    new Stream[ByteChunk]:
      def traverse(consume: ByteChunk => Async ?=> Demand)(using Async): Unit =
        val slot = new Slot[io.backend.File]
        io.perform(io.backend.ops.open(path, write = false, slot))
        val handle = slot.clear()
        owned(io)(handle):
          bytes.read(using io)(handle, chunkBytes).traverse(consume)

  /** Opens an EXISTING regular file for writing: today's backend open neither
    * creates nor truncates. Add explicit backend capabilities before promising
    * create/truncate/append modes. Opens once per sink consumption, closes even
    * on failure, optionally performs sync before close on success.
    */
  def writeExisting(path: String, syncOnSuccess: Boolean = false)(using Io[RegularFiles]): Sink[ByteChunk, Unit] = ???

/** Cleanup is shielded from cancellation; a cleanup failure is suppressed onto
  * the original failure. Io.perform guarantees the read has stopped touching
  * the buffer before it throws, so closing cannot race that read here.
  */
private def owned[R](io: Io[Backend])(handle: io.Handle)(body: Async ?=> R)(using Async): R =
  var primary: Throwable | Null = null
  try body
  catch
    case error: Throwable =>
      primary = error
      throw error
  finally
    try uninterruptible { io.perform(io.backend.ops.close(handle)) }
    catch
      case closing: Throwable =>
        if primary == null then throw closing
        else if closing ne primary then primary.nn.addSuppressed(closing)

/** A duplex session is scoped to connect/serve's callback. It may have one read
  * and one write running concurrently. Returning waits for their child scopes
  * before closing. No queued raw handles that could leak on cancellation.
  */
trait Session:
  def input: Stream[ByteChunk]
  def output: Sink[ByteChunk, Unit]
  def shutdownOutput()(using Async): Unit

object tcp:
  def connect[R](remote: IpSocketAddress)(body: Session => Async ?=> R)(using Io[IpStreams], Async): R = ???

  /** Reuses one accept op + cleared slot per listener run. Acquire a concurrency
    * permit BEFORE accept; transfer the handle into a bracket before spawning.
    * On failed handoff close it. All handlers are joined before listener close.
    */
  def serve(local: IpSocketAddress, maxConnections: Int)(handler: Session => Async ?=> Unit)(using
      Io[IpStreams], Async
  ): Unit = ???

object unix:
  def connect[R](remote: UnixAddress)(body: Session => Async ?=> R)(using Io[UnixStreams], Async): R = ???

final case class Packet(from: IpSocketAddress, data: ByteChunk)

object udp:
  /** Fresh socket/run, one receive op + buffer + sender slot, cleared every
    * receive. Empty packet is DATA, never EOF. maxPacketBytes>0 is also an
    * explicit truncation limit, since the current receive API drops excess.
    * Blocking delivery back-pressures our reads, not remote UDP senders.
    */
  def packets(local: IpSocketAddress, maxPacketBytes: Int)(using Io[IpDatagrams]): Stream[Packet] = ???
  def sendTo(remote: IpSocketAddress)(using Io[IpDatagrams]): Sink[ByteChunk, Unit] = ???

object pipes:
  def use[R](body: (Stream[ByteChunk], Sink[ByteChunk, Unit]) => Async ?=> R)(using Io[Pipes], Async): R = ???

object fifos:
  def read(path: String, chunkBytes: Int = 65536)(using Io[Fifos]): Stream[ByteChunk] = ???

object dns:
  /** Resolves only at traversal, then emits the resulting addresses. */
  def addresses(host: String)(using Io[Resolving]): Stream[IpAddress] = ???

object blocking:
  def map[A, B](input: Stream[A])(f: A => B)(using Io[Blocking]): Stream[B] = ???

  /** Invokes read on the backend's blocking facility, until None. No hidden
    * blocking on an I/O worker. BlockingOps.blocking is NOT Repeatable: this
    * adapter must construct a fresh op each time, rather than fake reuse.
    */
  def repeat[A](read: () => Option[A])(using Io[Blocking]): Stream[A] = ???

object timers:
  /** An Io-enabled operator, in the timer namespace rather than Stream's core.
    * Delays BEFORE each item, so a slow consumer naturally slows the schedule.
    */
  def spaced[A](input: Stream[A], delay: FiniteDuration)(using io: Io[Backend]): Stream[A] =
    require(delay.length > 0)
    new Stream[A]:
      def traverse(consume: A => Async ?=> Demand)(using Async): Unit =
        val wait: io.Op & Repeatable = io.backend.ops.timer(delay)
        input.traverse: a =>
          io.perform(wait)
          consume(a)

  /** Fixed DELAY, first tick after delay, no accumulated catch-up ticks. */
  def ticks(delay: FiniteDuration)(using io: Io[Backend]): Stream[Long] =
    require(delay.length > 0)
    new Stream[Long]:
      def traverse(consume: Long => Async ?=> Demand)(using Async): Unit =
        val tick: io.Op & Repeatable = io.backend.ops.timer(delay)
        var index = 0L
        var running = true
        while running do
          io.perform(tick)
          running = consume(index) == Demand.More
          index += 1

object text:
  /** Stateful UTF-8 decoder and line framing per traversal. Must preserve split
    * characters/CRLF across chunks and specify malformed-input handling. Limit
    * each line's encoded size; otherwise a bounded channel can still hold an
    * unbounded record. Do not decode each I/O chunk independently.
    */
  def lines(input: Stream[ByteChunk], maxLineBytes: Int): Stream[String] = ???

/* Concurrent interpreter requirements (not implemented by this sketch):
 *
 * - Each concurrent traversal enters Async.group internally; constructors do
 *   not request Async or Async.Spawn. Spawn is never stored in the plan.
 * - Child workers await/perform with their child Async. A captured lexical Io
 *   is intentional; an outer Async is not. Ordinary lexical request metadata
 *   travels in callbacks; arbitrary ThreadLocals are NOT propagated implicitly.
 * - Supervise failures explicitly: Async.group cancels/joins on EXIT but does
 *   not by itself wake a parent blocked on a channel when a worker fails.
 *   Record the first failure and wake/cancel blocked producers and consumers.
 * - Queue protocol distinguishes Item, End and Failed; early downstream Stop
 *   is successful completion, distinct from external cancellation. On Stop,
 *   cancel producer/workers, wake blocked sends, discard queued owned values,
 *   join children, then release handles. Never enqueue End into an abandoned
 *   full buffer while trying to clean up. Cancellation is not recoverable EOF.
 * - mapPar uses bounded job/result channels plus the shared admission credits
 *   described above. A semaphore around one Future PER INPUT is insufficient:
 *   it leaves an unbounded population of waiting futures.
 * - Pure map/filter do not implicitly gain parallelism. Auto is explicitly
 *   requested through mapPar and chooses a bounded worker count at run time;
 *   it does not claim to infer purity, parallelize arbitrary loops, or adapt
 *   worker counts continuously. Workers honor cancellation between jobs.
 * - Channels bound item counts; byte budgets additionally need bounded chunks
 *   and record sizes. Pooled zero-copy chunks would need a distinct scoped
 *   lease API, exactly-once release on discard/failure, and capacity admission
 *   BEFORE pooled reads (some backends fail when their pool is exhausted).
 * - Retry/replay is valid for cold sources (e.g. files) only as documented.
 *   Borrowed handles and channels are consumptive. No general replay promise.
 */

object Examples:
  /** A plan that can be built WITHOUT Async, then executed in a later scope
    * within the same Io.run. Neither nested loop requires flatMap.
    */
  def selectedLines(paths: Stream[String])(using Io[RegularFiles]): Stream[String] =
    Stream.build[String]:
      for path <- paths do
        for line <- text.lines(files.read(path), maxLineBytes = 1024 * 1024) do
          if line.nonEmpty then emit(line.trim)

  def parallelLines(paths: Stream[String])(using Io[RegularFiles]): Stream[String] =
    selectedLines(paths).mapPar(Parallelism.Auto(maxWorkers = 8), maxInFlight = 32): line =>
      // Direct asynchronous calls can go here; this receives the WORKER Async.
      line.toUpperCase
