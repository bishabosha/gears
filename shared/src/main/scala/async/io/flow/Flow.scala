package gears.async.io.flow

import gears.async.Async
import gears.async.BufferedChannel
import gears.async.Future
import gears.async.uninterruptible

import java.util.concurrent.CancellationException
import scala.util.control.ControlThrowable
import scala.util.control.NonFatal

/** A recipe for a stream of elements, which does nothing until it is run. Building one, from a source and the stages
  * put after it, opens nothing and performs nothing; a call that runs it (`run`, `foreach`, `fold`, `toList`) then does
  * all of it, on the caller's future, and returns when the stream has ended. A flow is a value: it may be run again, or
  * by several futures at once, and each run opens what it needs and has its own state.
  *
  * A run is one stage calling the next: a source gives each chunk of elements to the stage after it, which gives what
  * it makes of them to the one after that, and returns once the last has dealt with them. So nothing is suspended
  * between stages, a slow consumer holds its producer back with no queue between them, and the stack is the scope:
  *   - what a stage acquired (`Flow.resource`, `onEnd`) is released as the stages after it return, however they do: the
  *     stream ended, a stage failed, a later stage wanted no more (`take`), or the future running it was cancelled. A
  *     release is not cut short by that cancel, and happens once, the last acquired first.
  *   - a failure is an exception, thrown through the stages before the one that failed. `recover`, `recoverWith`,
  *     `retry` and `onError` deal with the failures of what comes before them in the flow, never with those of the
  *     stages after them, which only pass through.
  *   - a cancel is gears' own: the op the run waits in throws, and the run unwinds as for a failure. The stages that
  *     run several futures (`buffer`, `merge`, `mapPar`, `Flow.accept`) keep them in a group of their own, which ends
  *     with the stage.
  *
  * The elements of a source of bytes (`Flow.read`) are its own buffer, lent for the call: see there.
  */
abstract class Flow[+A] {

  /** Runs the flow, each chunk given to `emit`, and returns once it has ended. */
  protected[flow] def runWith(emit: Chunk[A] => Unit)(using Async): Unit

  // ---- running

  /** Runs the flow for what it does, its elements dropped. */
  final def run()(using Async): Unit = runWith(_ => ())

  final def foreach(f: A => Unit)(using Async): Unit = runWith(_.foreach(f))

  final def fold[B](zero: B)(f: (B, A) => B)(using Async): B = {
    var result = zero
    runWith(_.foreach(a => result = f(result, a)))
    result
  }

  final def toList(using Async): List[A] = {
    val all = List.newBuilder[A]
    runWith(_.foreach(all += _))
    all.result()
  }

  final def count(using Async): Long = fold(0L)((n, _) => n + 1)

  /** The first element, the flow run no further than it, or `None` if it has none. */
  final def first(using Async): Option[A] = take(1).fold(Option.empty[A])((_, a) => Some(a))

  final def last(using Async): Option[A] = fold(Option.empty[A])((_, a) => Some(a))

  // ---- element by element

  final def mapChunks[B](f: Chunk[A] => Chunk[B]): Flow[B] = Flow.make[B] { emit =>
    runWith { chunk =>
      val out = f(chunk)
      if !out.isEmpty then emit(out)
    }
  }

  final def map[B](f: A => B): Flow[B] = mapChunks(_.map(f))

  /** As `map`, for a function that performs ops or awaits: it is given the run's `Async`. */
  final def mapEval[B](f: A => Async ?=> B): Flow[B] = Flow.make[B] { emit =>
    runWith { chunk =>
      val out = chunk.map(a => f(a))
      if !out.isEmpty then emit(out)
    }
  }

  final def filter(keep: A => Boolean): Flow[A] = mapChunks(_.filter(keep))

  final def collect[B](f: PartialFunction[A, B]): Flow[B] = mapChunks { chunk =>
    val out = new Chunk.Builder[B]
    chunk.foreach(a => if f.isDefinedAt(a) then out += f(a))
    out.result()
  }

  /** Runs `f` on each element as it passes. */
  final def tap(f: A => Unit): Flow[A] = mapChunks { chunk =>
    chunk.foreach(f)
    chunk
  }

  /** Maps with a state each run begins at `initial`: `f` gives the next state and what to emit. */
  final def mapStateful[S, B](initial: => S)(f: (S, A) => (S, B)): Flow[B] = Flow.make[B] { emit =>
    var state = initial
    runWith { chunk =>
      emit(chunk.map { a =>
        val (next, b) = f(state, a)
        state = next
        b
      })
    }
  }

  /** Each element's flow, one after the other. */
  final def flatMap[B](f: A => Flow[B]): Flow[B] = Flow.make[B] { emit =>
    runWith(_.foreach(a => f(a).runWith(emit)))
  }

  /** This flow, and when it has ended, `next`. */
  final def ++[B >: A](next: => Flow[B]): Flow[B] = Flow.make[B] { emit =>
    runWith(emit)
    next.runWith(emit)
  }

  // ---- ending early

  /** The first `n` elements: once they have passed, the stages before this one are ended, as a flow that has no more to
    * give ends, what they acquired released.
    */
  final def take(n: Long): Flow[A] = Flow.make[A] { emit =>
    if n > 0 then {
      val stop = new Flow.Stop
      var left = n
      try
        runWith { chunk =>
          if chunk.size < left then {
            left -= chunk.size
            emit(chunk)
          } else {
            val rest = chunk.take(left.toInt)
            left = 0
            emit(rest)
            throw stop
          }
        }
      catch case stopped: Flow.Stop if stopped eq stop => ()
    }
  }

  /** Elements until the first that `keep` refuses, which ends the stages before this one as `take` does. */
  final def takeWhile(keep: A => Boolean): Flow[A] = Flow.make[A] { emit =>
    val stop = new Flow.Stop
    try
      runWith { chunk =>
        var kept = 0
        while kept < chunk.size && keep(chunk(kept)) do kept += 1
        if kept == chunk.size then emit(chunk)
        else {
          if kept > 0 then emit(chunk.take(kept))
          throw stop
        }
      }
    catch case stopped: Flow.Stop if stopped eq stop => ()
  }

  final def drop(n: Long): Flow[A] = Flow.make[A] { emit =>
    var left = n
    runWith { chunk =>
      if left <= 0 then emit(chunk)
      else if chunk.size <= left then left -= chunk.size
      else {
        val rest = chunk.drop(left.toInt)
        left = 0
        emit(rest)
      }
    }
  }

  // ---- resources and failures

  /** Runs `release` when a run of this flow is over, however it ends, as `Flow.resource` releases. */
  final def onEnd(release: Async ?=> Unit): Flow[A] = Flow.make[A] { emit =>
    Flow.releasing(runWith(emit))(release)
  }

  /** When this flow fails with a failure `handler` takes, the flow goes on as the one `handler` gives. A failure of a
    * stage after this one is not this flow's, and passes through; so does a cancel.
    */
  final def recoverWith[B >: A](handler: PartialFunction[Throwable, Flow[B]]): Flow[B] = Flow.make[B] { emit =>
    var after = false // whether a stage after this one is running, whose failures are not this flow's
    val failure: Throwable | Null =
      try {
        runWith { chunk =>
          after = true
          emit(chunk)
          after = false
        }
        null
      } catch {
        case NonFatal(failure) if !after && !Flow.isCancel(failure) && handler.isDefinedAt(failure) => failure
      }
    if failure != null then handler(failure).runWith(emit)
  }

  /** As `recoverWith`, the flow ending with the one element `handler` gives. */
  final def recover[B >: A](handler: PartialFunction[Throwable, B]): Flow[B] =
    recoverWith(handler.andThen(Flow.of(_)))

  /** Runs `f` with this flow's failure, which goes on as it was. */
  final def onError(f: Throwable => Unit): Flow[A] =
    recoverWith { case failure =>
      f(failure)
      Flow.failed(failure)
    }

  /** When this flow fails it is run again from its beginning, `times` times at most, and then fails as it last did.
    * What it had emitted before failing stays emitted.
    */
  final def retry(times: Int): Flow[A] =
    if times <= 0 then this else recoverWith { case _ => retry(times - 1) }

  // ---- several futures

  /** Runs the stages before this one in a future of their own, `capacity` chunks at most waiting between the two: the
    * stages after it then go on while those before wait, for a read say. Each chunk that crosses costs an await. The
    * elements must be the consumer's to keep: a lent buffer (`Flow.read`) is copied first.
    */
  final def buffer(capacity: Int): Flow[A] = Flow.make[A] { emit =>
    Async.group {
      val crossing = BufferedChannel[Any](capacity)
      Flow.produce(this, crossing)
      Flow.consume(crossing, 1, emit)
    }
  }

  /** The elements of this flow and of `other`, each run in a future of its own, as they come. When either fails, or the
    * stages after want no more, both are ended.
    */
  final def merge[B >: A](other: Flow[B]): Flow[B] = Flow.make[B] { emit =>
    Async.group {
      val crossing = BufferedChannel[Any](1)
      Flow.produce(this, crossing)
      Flow.produce(other, crossing)
      Flow.consume(crossing, 2, emit)
    }
  }

  /** As `mapEval`, with `parallelism` elements at most being mapped at once, each in a future of its own. What is
    * emitted is in the order of the elements.
    */
  final def mapPar[B](parallelism: Int)(f: A => Async ?=> B): Flow[B] = {
    require(parallelism > 0, s"A parallelism is positive: $parallelism")
    Flow.make[B] { emit =>
      Async.group {
        val running = new java.util.ArrayDeque[Future[B]]
        runWith(_.foreach { a =>
          if running.size() == parallelism then emit(Chunk.one(running.removeFirst().await))
          running.addLast(Future(f(a)))
        })
        while !running.isEmpty() do emit(Chunk.one(running.removeFirst().await))
      }
    }
  }
}

object Flow extends FlowIo {

  /** What a stage throws to end the stages before it, which it alone catches. No stack trace, and not a failure:
    * `NonFatal` does not match it.
    */
  private[flow] final class Stop extends ControlThrowable

  private[flow] def isCancel(failure: Throwable): Boolean = failure.isInstanceOf[CancellationException]

  private[flow] def make[A](body: (Chunk[A] => Unit) => Async ?=> Unit): Flow[A] = new Flow[A] {
    protected[flow] def runWith(emit: Chunk[A] => Unit)(using Async): Unit = body(emit)
  }

  /** Runs `body`, and then `release`, however `body` ended. The release is not cancelled with the caller: it is what
    * the caller does about having been cancelled. If both fail, the release's failure is added to the body's.
    */
  private[flow] def releasing[T](body: => T)(release: Async ?=> Unit)(using Async): T = {
    var failure: Throwable | Null = null
    try body
    catch {
      case thrown: Throwable =>
        failure = thrown
        throw thrown
    } finally
      try uninterruptible(release)
      catch {
        case thrown: Throwable =>
          if failure == null then throw thrown
          else if !isCancel(thrown) then failure.nn.addSuppressed(thrown) // a cancel is the body's own already
      }
  }

  // ---- sources

  val empty: Flow[Nothing] = make[Nothing](_ => ())

  def of[A](elements: A*): Flow[A] = fromIterable(elements)

  def fromIterable[A](elements: Iterable[A]): Flow[A] = make[A] { emit =>
    val chunk = Chunk.from(elements)
    if !chunk.isEmpty then emit(chunk)
  }

  /** A flow that fails with `failure` as it is run. */
  def failed(failure: Throwable): Flow[Nothing] = make[Nothing](_ => throw failure)

  /** One element, computed as the flow is run. */
  def eval[A](element: Async ?=> A): Flow[A] = make[A](emit => emit(Chunk.one(element)))

  /** The flow `flow` gives, asked for as this one is run. */
  def defer[A](flow: => Flow[A]): Flow[A] = make[A](emit => flow.runWith(emit))

  /** A flow written as ordinary code: `body` is run with a function that emits an element, and with the run's `Async`,
    * so it may loop, perform ops and await as it likes. Each call of the function returns once the stages after have
    * dealt with the element.
    */
  def emit[A](body: Async ?=> (A => Unit) => Unit): Flow[A] = make[A] { emit =>
    body(a => emit(Chunk.one(a)))
  }

  /** As `emit`, a chunk at a time. */
  def emitChunks[A](body: Async ?=> (Chunk[A] => Unit) => Unit): Flow[A] = make[A] { emit =>
    body(chunk => if !chunk.isEmpty then emit(chunk))
  }

  /** One element, what `acquire` gives, which `release` is given back once the stages after this one have returned,
    * however they did: the stream ended, one of them failed or wanted no more, or the run was cancelled. A release is
    * not cut short by that cancel. With `flatMap`, the flow that uses the resource:
    * `Flow.resource(open)(close).flatMap(file => ...)`.
    */
  def resource[R](acquire: Async ?=> R)(release: R => Async ?=> Unit): Flow[R] = make[R] { emit =>
    val acquired = acquire
    releasing(emit(Chunk.one(acquired)))(release(acquired))
  }

  // ---- between futures

  private final class Failed(val failure: Throwable)
  private object Ended

  /** Runs `flow` in a future of the group's, its chunks sent to `crossing`, and then how it ended. */
  private def produce[A](flow: Flow[A], crossing: BufferedChannel[Any])(using Async.Spawn): Unit =
    Future {
      val ended: AnyRef =
        try {
          flow.runWith(chunk => crossing.send(chunk))
          Ended
        } catch {
          case NonFatal(failure) if !isCancel(failure) => new Failed(failure)
        }
      crossing.send(ended)
    }: Unit

  /** Gives `emit` what `producers` flows send to `crossing`, until all have ended, or one has failed. */
  private def consume[A](crossing: BufferedChannel[Any], producers: Int, emit: Chunk[A] => Unit)(using Async): Unit = {
    var running = producers
    while running > 0 do
      crossing.read() match {
        case Right(chunk: Chunk[?]) => emit(chunk.asInstanceOf[Chunk[A]])
        case Right(failed: Failed)  => throw failed.failure
        case Right(_)               => running -= 1
        case Left(_)                => throw new IllegalStateException("The flow's channel was closed")
      }
  }
}
