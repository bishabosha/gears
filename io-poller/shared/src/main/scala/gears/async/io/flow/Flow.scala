package gears.async.io
package flow

import gears.async.Async

import scala.concurrent.duration.FiniteDuration

/** A program of ops that is performed without suspending anyone: what a future would write as a loop around
  * `io.perform`, built once as a short table of steps, which the workers then run a step at a time, each op's
  * completion going straight on to the next step, with no stack kept between them.
  *
  * It is run from within a future (`Flow.run`), which waits for it as it would for one op, and which may run several at
  * once: they are that future's, never run at the same moment as each other, so they may share its variables as the
  * code of one future does, and they end with it.
  *
  * A flow is built with the steps below, inside `Flow(io) { ... }`. What is written there runs once, as the flow is
  * built, and makes steps: only what is given to a step runs as the flow does. So the flow's buffers and ops are made
  * in the block, as plain values, and what is to happen each time round goes in `exec`, a condition in `loop` or
  * `when`.
  */
final class Flow[O] private[io] (
    private[io] val kinds: Array[Byte],
    private[io] val fns: Array[AnyRef | Null],
    private[io] val targets: Array[Int],
    private[io] val timeouts: Array[Long]
)

object Flow {
  private[io] inline val Exec = 0
  private[io] inline val Perform = 1
  private[io] inline val Unless = 2
  private[io] inline val Jump = 3

  /** What the steps of a flow being built are added to. */
  final class Builder[-O] private[Flow] () {
    private var kinds = new Array[Byte](16)
    private var fns = new Array[AnyRef | Null](16)
    private var targets = new Array[Int](16)
    private var timeouts = new Array[Long](16)
    private var size = 0

    private[Flow] def here: Int = size

    private[Flow] def add(kind: Int, fn: AnyRef | Null, target: Int = -1, timeout: Long = Backend.NoTimeout): Int = {
      if size == kinds.length then {
        kinds = java.util.Arrays.copyOf(kinds, size * 2)
        fns = java.util.Arrays.copyOf(fns, size * 2)
        targets = java.util.Arrays.copyOf(targets, size * 2)
        timeouts = java.util.Arrays.copyOf(timeouts, size * 2)
      }
      kinds(size) = kind.toByte
      fns(size) = fn
      targets(size) = target
      timeouts(size) = timeout
      size += 1
      size - 1
    }

    private[Flow] def aim(at: Int, target: Int): Unit = targets(at) = target

    private[Flow] def result[F](): Flow[F] =
      new Flow(
        java.util.Arrays.copyOf(kinds, size),
        java.util.Arrays.copyOf(fns, size),
        java.util.Arrays.copyOf(targets, size),
        java.util.Arrays.copyOf(timeouts, size)
      )
  }

  /** Builds a flow of `io`'s ops from the steps `body` makes. */
  def apply(io: Io[Interface])(body: Builder[io.Op] ?=> Unit): Flow[io.Op] = {
    val builder = new Builder[io.Op]
    body(using builder)
    builder.result[io.Op]()
  }

  /** A step that performs `op`, evaluated each time the step is come to, and goes on once it is done. */
  def perform[O](op: => O)(using builder: Builder[O]): Unit = builder.add(Perform, () => op): Unit

  /** As `perform`, with a time limit, as `Io.perform` has it. */
  def perform[O](op: => O, timeout: FiniteDuration)(using builder: Builder[O]): Unit = {
    require(timeout.length >= 0, s"A time limit is not negative: $timeout")
    val nanos = timeout.toNanos
    val millis = nanos / 1_000_000L + (if nanos % 1_000_000L == 0 then 0 else 1)
    builder.add(Perform, () => op, timeout = millis): Unit
  }

  /** A step that runs `body`, which performs nothing. */
  def exec[O](body: => Unit)(using builder: Builder[O]): Unit = builder.add(Exec, () => body): Unit

  /** The steps of `body`, again and again while `condition` holds, looked at before each time. */
  def loop[O](condition: => Boolean)(body: Builder[O] ?=> Unit)(using builder: Builder[O]): Unit = {
    val top = builder.add(Unless, () => condition)
    body
    builder.add(Jump, null, top): Unit
    builder.aim(top, builder.here)
  }

  /** The steps of `body`, `times` times. */
  def repeat[O](times: Int)(body: Builder[O] ?=> Unit)(using builder: Builder[O]): Unit = {
    var done = 0
    exec { done = 0 }
    loop(done < times) {
      body
      exec { done += 1 }
    }
  }

  /** The steps of `body`, if `condition` holds as they are come to. */
  def when[O](condition: => Boolean)(body: Builder[O] ?=> Unit)(using builder: Builder[O]): Unit = {
    val test = builder.add(Unless, () => condition)
    body
    builder.aim(test, builder.here)
  }

  /** The steps of `then` if `condition` holds as they are come to, and otherwise those of `otherwise`. */
  def choose[O](condition: => Boolean)(`then`: Builder[O] ?=> Unit)(otherwise: Builder[O] ?=> Unit)(using
      builder: Builder[O]
  ): Unit = {
    val test = builder.add(Unless, () => condition)
    `then`
    val skip = builder.add(Jump, null)
    builder.aim(test, builder.here)
    otherwise
    builder.aim(skip, builder.here)
  }

  /** The steps of `first` and then those of `second`, as one flow. */
  private[io] def concat[O](first: Flow[?], second: Flow[?]): Flow[O] = {
    val shift = first.kinds.length
    new Flow(
      first.kinds ++ second.kinds,
      first.fns ++ second.fns,
      first.targets ++ second.targets.map(target => if target < 0 then target else target + shift),
      first.timeouts ++ second.timeouts
    )
  }

  /** Runs `flows` at once, as the calling future's, and returns when all have ended; the first failure of one cancels
    * the others and is thrown from here once they have stopped, as is the caller's own cancellation. The caller is
    * suspended at most once, however many ops the flows perform, and not at all if none of them had to wait.
    */
  def run(io: Io[Interface])(flows: Flow[io.Op]*)(using Async): Unit = io match
    case pool: shareable.WorkpoolScheduler[?] => pool.performFlows(flows.toArray.asInstanceOf[Array[Flow[?]]])
    case _ => throw new UnsupportedOperationException(s"Flows are not run by this backend yet: ${io.backend}")
}
