package gears.streaming.sketch.proposals

import gears.async.{Async, AsyncOperations}
import gears.async.io.{Backend, BufferPool, Io, Ops, Received, Repeatable, Slot}
import java.nio.ByteBuffer

/* Proposals for the async-io-streaming contract, NOT changes to existing
 * backends. These declarations distinguish genuinely new backend semantics
 * from controls expressible in a library on top of today's Io/Async.
 */

enum PoolExhaustion:
  case Wait, Fail

/** Proposed additional overload. Recommended default for ordinary pooled read
  * is Wait, consistently across backends. Keep Fail only as an explicit choice.
  * Today's contract permits failure on exhaustion and permits waiting on some
  * backends, so portable suspension cannot currently be assumed.
  * An exclusively owned pool CAN emulate waiting with a library semaphore whose
  * permit is held through Received.release, provided every user goes through the
  * wrapper. Standardizing Wait removes that duplicate admission layer; it is not
  * a claim that HTTP is impossible without a backend change.
  */
trait PooledReadPolicyOps[Op, H <: AnyRef] extends Ops[Op, H]:
  def read(
      handle: Readable,
      pool: BufferPool,
      into: Slot[Received],
      onExhaustion: PoolExhaustion
  ): Op & Repeatable

/* Wait semantics required from implementations:
 *
 * - Reserve capacity atomically inside the performance, before accepting data
 *   without space to retain it. available is observational, never a reservation.
 *   A portable adapter cannot make if(pool.available>0) race-free across readers.
 * - Waiting consumes neither a worker thread nor source bytes on a reactor.
 *   Cancellation, timeout and handle/pool shutdown wake capacity waiters and
 *   unlink them. Cancelling before reservation leaks no buffer/permit/slot.
 * - Reservation and device readiness must be integrated without spinning or
 *   holding scarce pool buffers indefinitely for idle connections. The precise
 *   strategy is backend-specific; it cannot promise atomic selection of both.
 * - Define result-ownership commit: successful completion writes one Received,
 *   including EOF, which the caller clears and releases. A performance that
 *   throws owns no unretrieved Received and leaves its result slot empty. A
 *   commit winning the cancellation race must be reported consistently; do not
 *   throw after handing ownership into a slot that the caller won't inspect.
 * - The op is reusable after return/throw, never concurrently. A cancelled pool
 *   wait must not close the socket. A successful read may consume source bytes;
 *   cancellation is not a general rollback guarantee for arbitrary I/O.
 * - Shutdown ordering stays explicit: stop/await reads, release all results,
 *   then close pool. Close with outstanding leases should fail deterministically
 *   rather than free their memory. An owner that needs asynchronous shutdown can
 *   implement that sequence; changing close to silently wait risks deadlock.
 */

/** Optional API improvement for one deadline shared by layered operations.
  * A relative timeout is not reset on each header/body read, queue wait, etc.
  * Use System.nanoTime time, not wall-clock time, and wrap-safe comparisons.
  */
final case class AbsoluteDeadline(nanoTime: Long)

trait AbsoluteDeadlineIo[+B <: Backend] extends Io[B]:
  def performUntil(op: Op, deadline: AbsoluteDeadline)(using Async): Unit

/* A deadline is a stop condition, not a hard real-time completion guarantee.
 * Preserve each op's existing cancellation/cleanup rules for already-running
 * blocking calls. An expired operation must not begin new I/O. Waiting for pool
 * capacity, handle serialization, and device readiness all consumes the same
 * budget. Preserve usable timer precision instead of rounding every layer anew.
 *
 * This overload is useful, but not a blocker: an owning timeout scope + existing
 * perform(timeout) can express the policy already, with extra timer/race work.
 * A convenience wrapper recomputing remaining duration cannot alone enforce an
 * absolute cutoff at eventual backend admission; it must be checked there too.
 */

/** Proposed *library* wrapper around current Received. No backend retain op is
  * required: an atomic shared owner can invoke Received.release once when the
  * last independently releasable token is returned. Each token has its own view
  * position/limit. This does not claim generic Stream operators handle leases.
  */
trait ByteLease:
  def view: ByteBuffer
  def size: Int
  def retain(): ByteLease
  def release(): Unit

object ByteLease:
  def owned(received: Received): ByteLease = ???

/** Cooperation is a scheduler control, not a new I/O op. Existing Io is also
  * AsyncOperations, so a fiber-local budget can retain it without retaining
  * Async. A CPU parser loop calls checkpoint periodically, including when its
  * input stays immediately readable. Each child fiber needs its own budget.
  */
trait WorkBudget:
  def checkpoint()(using Async): Unit

object WorkBudget:
  def apply(maxSteps: Int)(using operations: AsyncOperations): WorkBudget =
    require(maxSteps > 0)
    new WorkBudget:
      private var remaining = maxSteps
      def checkpoint()(using current: Async): Unit =
        if current.group.isCancelled then throw new java.util.concurrent.CancellationException()
        remaining -= 1
        if remaining == 0 then
          remaining = maxSteps
          operations.`yield`()

/* Other control decisions:
 *
 * - Graph supervision needs a first-failure latch that wakes the parent and
 *   cancels siblings immediately, preserving the original exception. Async.group
 *   only guarantees cancellation/join on exit. This can be implemented as an
 *   Async library facility; it does not belong in Io.perform and does not need
 *   a stored Async. A runtime-owned CompletionGroup/control handle is distinct
 *   from capturing a caller's entire Async capability in a reusable plan.
 * - Keep the existing guarantee that perform has stopped using buffers/slots
 *   when it returns or throws, and that cancelling a wait leaves its handle
 *   open. Do not weaken either for faster cancellation: connection sharing and
 *   safe buffer reuse depend on them. Preserve at most one active stream reader
 *   and one writer; protocol concurrency is multiplexed above those directions.
 * - Vectored read/write, partial-write positions, output half-close, movable
 *   deadline timers and reusable ops already exist. Use those before proposing
 *   a second networking interface. Write completion means consumed by transport,
 *   not HTTP peer acknowledgement. Reuse a staging vector/op when identities
 *   are stable; no claim that immutable positional file ops can change offset.
 * - A structured nonblocking submit/select-completion API might be worthwhile
 *   if profiling shows per-connection task overhead is material. It would need
 *   explicit operation/buffer ownership and cancellation joining. A public
 *   Async.Source wrapper around arbitrary perform cannot safely provide atomic,
 *   rollback-free races of side-effecting operations. It is not in this sketch.
 * - A duplex byte-transport layer should hide TCP/TLS details from HTTP, while
 *   preserving read/write ownership, EOF, shutdown and bounded buffering. Adding
 *   TLS or protocol negotiation does not mean Async belongs in session fields.
 * - Protocol flow-control reservations and a bounded control-frame reserve are
 *   separate from generic channel capacity. Do not let a pool exhausted by
 *   application-held DATA prevent the connection from reading control traffic;
 *   plan receive-window admission and parser scratch/headroom together.
 *
 * Implement the pooled-read change across a reference backend and its contracts
 * before relying on it. Test exhaustion, release/wakeup, cancellation, timeout,
 * shutdown, commit races and reuse after every outcome. Benchmark fairness and
 * pool contention separately from protocol throughput; no performance claim is
 * implied by these interface declarations.
 */
