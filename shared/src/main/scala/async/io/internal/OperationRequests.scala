package gears.async.io.internal

import gears.async.Async
import gears.async.io.{Backend, Io, Repeatable}

import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration

/** Experimental descriptions, independent of consumer, cancellation and poller state. The referenced handle and buffer
  * retain their ordinary ownership rules; an immutable description does not make its buffer immutable.
  */
private[io] final case class ReadRequest[H <: AnyRef](handle: H, buffer: ByteBuffer) {
  Checks.checkFillable(buffer)
}

/** A relative delay starts anew for each execution. Movable deadlines need a separate control and are not modelled by
  * this experiment.
  */
private[io] final case class TimerRequest(duration: FiniteDuration) {
  require(duration.length >= 0, s"A timer's time is not negative: $duration")
}

/** Lowers data into a backend's execution record. Every call creates an independent execution; there is no identity map
  * or per-poller cache. Retain the returned record for allocation-free sequential reuse through `Io.perform`. The
  * record, rather than the description, is the Gears source, cancellation member and pending submission.
  *
  * For this prototype `Backend.Op` still names the existing execution type. Making descriptions the default API would
  * require splitting that member into request and execution types; this boundary lets us first measure the
  * representation without changing every capability or the runtime's bookkeeping.
  */
private[io] trait RequestInterpreter[B <: Backend] {
  val backend: B
  def prepare(request: ReadRequest[backend.Readable]): backend.Op & Repeatable
  def prepare(request: TimerRequest): backend.Op & Repeatable

  /** Convenient data-only submission creates a fresh execution. Explicit preparation allows callers with a hot
    * sequential loop to retain the execution instead, without an implicit identity cache.
    */
  final def perform(request: ReadRequest[backend.Readable])(using io: Io[backend.type], async: Async): Unit =
    io.perform(prepare(request))
  final def perform(request: TimerRequest)(using io: Io[backend.type], async: Async): Unit =
    io.perform(prepare(request))
}
