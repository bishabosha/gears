package gears.streaming.sketch.interop

import gears.async.Async
import gears.streaming.sketch.{Sink, Stream}
import java.util.concurrent.Flow

/** JVM-only companion to StreamingIoSketch.scala. Interface proposal, not a
  * Reactive Streams implementation: all bridge runtimes below remain `???`.
  * Neither direction needs Io: protocol adaptation itself performs no backend
  * ops. Io-bound sources retain their lexical Io inside the owner scope.
  *
  * References:
  * https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Flow.html
  * https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Flow.Subscription.html
  */
object javaFlow:
  /** Lazy: subscribe once per traversal; no Async needed to construct the plan.
    * Re-traversal requires the external publisher to accept another subscription.
    *
    * prefetch>0 bounds buffered + outstanding requested + currently consumed
    * elements together. Replenish credits after consumption; reserve capacity
    * before request(n). onNext offers into already reserved slots and never
    * suspends on a Gears channel. Terminal state is separate from the data queue
    * so recording completion/error cannot block behind a full buffer.
    *
    * Callbacks retain thread-safe state and wakeups, never Async. Awaiting and
    * processing happen on the traversal's current Async. Serialize request/cancel
    * calls and handle synchronous/reentrant publishers without recursive demand
    * storms or invoking foreign methods while holding an internal lock.
    *
    * On Stop/cancellation cancel upstream, unblock our consumer and discard late
    * arrivals. A racing late onSubscribe must be cancelled too. Do not wait for
    * onComplete after cancel: Flow does not require a terminal signal then.
    * Deliver already admitted values before normal completion/error.
    */
  def fromPublisher[A](publisher: Flow.Publisher[? <: A], prefetch: Int = 16): Stream[A] = ???

  /** Terminal adapter. Sink construction needs no Async. consume owns the pump
    * and Subscription; returns after completion/cancellation and local cleanup.
    * The provided subscriber is single-use, so this sink is single-use too.
    * Uses the same demand/serialization engine as withPublisher below.
    */
  def toSubscriber[A](subscriber: Flow.Subscriber[? >: A]): Sink[A, Unit] = ???

  /** Export a single-subscription publisher only for the dynamic extent of use.
    * A bare source.toPublisher(using Async) would hide an owner lifetime and
    * encourage storing Async in subscribe; this API makes that lifetime explicit.
    * use must await the foreign operation before returning, not just start it.
    *
    * Internally an owning Async.group runs a supervisor. subscribe, request and
    * cancel update a bounded mailbox/coalesced demand counter, never run Gears
    * suspension themselves. The supervisor starts a pump with its own child
    * Async. No publisher/subscription field stores Async or Async.Spawn.
    *
    * At most one traversal is accepted. Reject further or expired subscriptions
    * with onSubscribe followed by onError. Multicast/replay requires a different
    * explicit policy, especially for HTTP bodies and borrowed handles.
    *
    * Serialize subscriber signals; onSubscribe precedes all others. Each onNext
    * consumes positive demand. Saturate demand arithmetic; request(n<=0) signals
    * IllegalArgumentException. Known completion/error does not require demand.
    * A fallback push traversal may hold one lookahead element while waiting
    * for demand. Native readers can provide tighter demand control; terminal
    * detection must still be specified independently of demand for data.
    *
    * cancel is idempotent and cancels only this pump. Do not synthesize normal
    * completion after cancellation. Scope exit cancels and joins owned work and
    * rejects new subscriptions. A throwing subscriber callback cancels the pump
    * and triggers cleanup; do not attempt more callbacks on that subscriber.
    */
  def withPublisher[A, R](source: Stream[A])(
      use: Flow.Publisher[A] => Async ?=> R
  )(using Async): R = ???

  /** Fresh processor per traversal. Attach its output subscriber before feeding
    * its input; run both sides together under one owner. Failure or early Stop
    * tears down both subscriptions. Returning a plain map of the processor
    * would miss the concurrent bidirectional lifecycle.
    */
  def viaProcessor[A, B](source: Stream[A], prefetch: Int = 16)(
      create: () => Flow.Processor[A, B]
  ): Stream[B] = ???

/* Boundary contracts to validate when implementing:
 *
 * - Flow forbids null items. Reject a null subscriber synchronously; reject null
 *   signals/items and overproduction with deterministic cleanup.
 * - Element credits are NOT byte credits. Generic A must be safely retainable.
 *   Borrowed/mutable buffers need a copy or ownership transfer at the callback
 *   boundary, before onNext returns. For byte budgets, validate element sizes
 *   and fail/cancel oversized elements; prefetch alone cannot bound their size.
 * - JDK HTTP BodyHandlers.ofPublisher returns Publisher[List[ByteBuffer]], not
 *   Publisher[ByteBuffer]. Flattening/copying needs its own bounded byte policy.
 *   BodyPublishers.fromPublisher accepts Publisher[ByteBuffer] for uploads.
 * - An HTTP body's exported publisher belongs inside exchange's response scope.
 *   Subscriber cancellation invokes body abandonment; HTTP owns drain/reset/
 *   connection-close policy. Flow.cancel is not a connection-close instruction.
 *   A foreign publisher's cancel policy is its own, not controlled by this bridge.
 * - Cleanup guarantees local tasks have stopped, not that a foreign publisher
 *   has joined its tasks: Flow.cancel is best-effort and late arrivals are legal.
 * - Tests must cover reentrant request, concurrent demand/cancel, overflow,
 *   zero-demand terminal signals, late onSubscribe, saturated demand, single-use
 *   rejection, scoped cleanup, and byte ownership; run a compliance suite too.
 */
