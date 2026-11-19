package gears.async.io

import gears.async.Async

import scala.util.boundary
import scala.util.boundary.break

/** EXPERIMENT. A stream, written as the code that produces it:
  *
  * {{{
  * def lines(bytes: Flow[ByteBuffer]): Flow[String] = Flow:
  *   val line = new StringBuilder
  *   for chunk <- bytes do
  *     while chunk.hasRemaining() do
  *       val next = chunk.get().toChar
  *       if next == '\n' then { emit(line.result()); line.clear() } else line += next
  * }}}
  *
  * A flow does nothing until it is run, with `foreach` or what is made of it (`for ... do`, `fold`, `to`, ...); then
  * its code runs, on the caller's own stack, and each `emit` is a call of what consumes the element, which returns when
  * that has done with it. So a slow consumer holds its producer back with nothing said, a consumer that waits for an
  * op or a future has the whole of the pipeline wait with it, and nothing runs between a flow and what it was made of:
  * `map`, `filter` and the rest are functions called in turn.
  *
  * A flow run twice runs its code twice. One that ends early (`take`, `head`) has its producer leave where it was,
  * through its `finally` blocks, as a `break` does.
  *
  * An element may be lent: a connection's bytes are its read buffer, good until the consumer returns. What is kept
  * longer, or given to another flow (`groupBy`), is copied first, by whatever decodes it.
  */
trait Flow[+A] {

  /** Runs the flow: `each` is called with every element, in order, and this returns when the flow has ended. */
  def foreach(each: A => Unit)(using Async): Unit
}

/** What a flow's code emits with: given to the body of `Flow { ... }`. */
final class Emit[-A] private[io] (private[io] val each: A => Unit)

/** What takes elements, one caller's whole and in the order of its calls: `send` returns as soon as the element is
  * taken, which for a connection is before it is written, and waits only if too much is taken and unwritten.
  */
trait Sink[-A] {
  def send(element: A)(using Async): Unit

  /** Waits until all that was sent has gone where it goes. */
  def flush()(using Async): Unit
}

object Sink {
  extension [A](sink: Sink[A]) {

    /** A sink of what `f` makes this one's elements of. */
    def contramap[B](f: B => A): Sink[B] = new Sink[B] {
      def send(element: B)(using Async): Unit = sink.send(f(element))
      def flush()(using Async): Unit = sink.flush()
    }
  }
}

object Flow {

  /** The flow whose elements are what `body` emits, as it emits them. */
  def apply[A](body: Emit[A] ?=> Async ?=> Unit): Flow[A] = new Flow[A] {
    def foreach(each: A => Unit)(using Async): Unit = body(using new Emit[A](each))
  }

  /** Emits `element` from the body of a `Flow { ... }`: returns when its consumer has done with it. */
  def emit[A](element: A)(using to: Emit[A]): Unit = to.each(element)

  def of[A](elements: A*): Flow[A] = from(elements)

  def from[A](elements: IterableOnce[A]): Flow[A] = Flow(elements.iterator.foreach(emit))

  val empty: Flow[Nothing] = Flow(())

  /** How far a key's flow of `groupBy`'s may fall behind. */
  inline val Lag = 64

  extension [A](flow: Flow[A]) {

    // ---- one flow of another, nothing running between them

    def map[B](f: A => B): Flow[B] = Flow(flow.foreach(element => emit(f(element))))

    def filter(keep: A => Boolean): Flow[A] = Flow(flow.foreach(element => if keep(element) then emit(element)))

    /** For `for a <- flow if ... do`. */
    def withFilter(keep: A => Boolean): Flow[A] = filter(keep)

    /** The elements of the flows `f` makes of this one's, one flow after another. */
    def flatMap[B](f: A => Flow[B]): Flow[B] = Flow(flow.foreach(element => f(element).foreach(emit)))

    def collect[B](f: PartialFunction[A, B]): Flow[B] =
      Flow(flow.foreach(element => if f.isDefinedAt(element) then emit(f(element))))

    /** This flow's elements, and then `next`'s. */
    def ++[B >: A](next: Flow[B]): Flow[B] = Flow {
      flow.foreach(emit)
      next.foreach(emit)
    }

    /** The first `count` elements: the flow is left as the last of them has been consumed. */
    def take(count: Long): Flow[A] = Flow {
      if count > 0 then
        boundary {
          var left = count
          flow.foreach { element =>
            emit(element)
            left -= 1
            if left == 0 then break()
          }
        }
    }

    def takeWhile(keep: A => Boolean): Flow[A] = Flow {
      boundary(flow.foreach(element => if keep(element) then emit(element) else break()))
    }

    def drop(count: Long): Flow[A] = Flow {
      var left = count
      flow.foreach(element => if left > 0 then left -= 1 else emit(element))
    }

    /** Each element, as it goes by. */
    def tap(see: A => Unit): Flow[A] = Flow(flow.foreach { element => see(element); emit(element) })

    /** What `f` has made of the elements so far, after each. */
    def scan[B](zero: B)(f: (B, A) => B): Flow[B] = Flow {
      var state = zero
      flow.foreach { element =>
        state = f(state, element)
        emit(state)
      }
    }

    def zipWithIndex: Flow[(A, Long)] = Flow {
      var index = 0L
      flow.foreach { element =>
        emit((element, index))
        index += 1
      }
    }

    /** What `pipe` makes of this flow: `bytes.through(frames).through(requests)`. */
    def through[B](pipe: Flow[A] => Flow[B]): Flow[B] = pipe(flow)

    // ---- running a flow

    def fold[B](zero: B)(f: (B, A) => B)(using Async): B = {
      var result = zero
      flow.foreach(element => result = f(result, element))
      result
    }

    def toList(using Async): List[A] = {
      val elements = List.newBuilder[A]
      flow.foreach(elements += _)
      elements.result()
    }

    /** The first element, the flow left as soon as it has come; none if it ends without one. */
    def headOption(using Async): Option[A] = boundary {
      flow.foreach(element => break(Some(element)))
      None
    }

    /** Sends every element to `sink`, and waits until it has gone. */
    def to(sink: Sink[A])(using Async): Unit = {
      flow.foreach(sink.send)
      sink.flush()
    }

    // ---- many flows at once

    /** The elements dealt by `key`: a flow of the keys, each as it is first met, with a flow of its elements, which
      * ends when this one does. Running the flow of keys is what deals; a key's flow is run beside it, by another
      * flow (`foreachPar`), and has what was dealt to it since. One that falls `Flow.Lag` elements behind has the
      * dealing wait for it, and one that is never run has it wait for good. The elements go from one flow to another,
      * so must not be lent.
      *
      * In a flow of a reactor's, a `Server`'s handler say, the keys' flows are flows of that reactor, on its thread;
      * elsewhere each is a channel, for a future.
      */
    def groupBy[K](key: A => K): Flow[(K, Flow[A])] = Flow {
      val reactor = Reactor.of(summon[Async])
      if reactor == null then dealToChannels(flow, key) else dealOnReactor(flow, key, reactor)
    }

    /** `f` of every element, each in a flow of its own beside the caller's, which goes on running this one; returns
      * when this flow has ended and every one of them has, with the first failure there was thrown. In a flow of a
      * reactor's they are flows of it, on its thread; elsewhere futures.
      */
    def foreachPar(f: A => Async ?=> Unit)(using async: Async): Unit = {
      val reactor = Reactor.of(async)
      if reactor == null then
        Async.group {
          val running = scala.collection.mutable.ArrayBuffer[gears.async.Future[Unit]]()
          flow.foreach(element => running += gears.async.Future(f(element)))
          running.foreach(_.await)
        }
      else {
        val over = new reactor.Signal
        var running = 0
        var failure: Throwable | Null = null
        boundary {
          flow.foreach { element =>
            if failure != null then break()
            running += 1
            reactor.fork(async.group)(f(element)) { thrown =>
              running -= 1
              if thrown != null && failure == null then failure = thrown
              over.wake()
            }
          }
        }
        while running > 0 do over.park()
        val thrown = failure
        if thrown != null then throw thrown
      }
    }
  }

  /** `groupBy` in a flow of `reactor`'s: a key's flow is a queue and a signal, all on the reactor's thread. */
  private def dealOnReactor[A, K](flow: Flow[A], key: A => K, reactor: Reactor[?])(using Emit[(K, Flow[A])], Async): Unit = {
    var ended = false
    final class Dealt extends Flow[A] {
      val elements = new Ring[AnyRef]()
      val arrived = new reactor.Signal
      val room = new reactor.Signal
      def foreach(each: A => Unit)(using Async): Unit = {
        var going = true
        while going do {
          while elements.isEmpty && !ended do arrived.park()
          if elements.isEmpty then going = false
          else {
            if elements.size == Lag then room.wake()
            each(elements.poll().asInstanceOf[A])
          }
        }
      }
    }
    val dealt = new java.util.HashMap[K, Dealt]()
    try
      flow.foreach { element =>
        val k = key(element)
        val known = dealt.get(k)
        if known == null then {
          val made = new Dealt
          made.elements.add(element.asInstanceOf[AnyRef])
          dealt.put(k, made): Unit
          emit((k, made))
        } else {
          known.elements.add(element.asInstanceOf[AnyRef])
          known.arrived.wake()
          while known.elements.size > Lag do known.room.park()
        }
      }
    finally {
      ended = true
      dealt.values().forEach(_.arrived.wake())
    }
  }

  /** `groupBy` under any other `Async`: a key's flow is a channel, which a future of its own reads. */
  private def dealToChannels[A, K](flow: Flow[A], key: A => K)(using Emit[(K, Flow[A])], Async): Unit = {
    // A channel closed forgets what it holds: the end is sent, as an element is.
    object End
    val dealt = new java.util.HashMap[K, gears.async.BufferedChannel[A | End.type]]()
    try
      flow.foreach { element =>
        val k = key(element)
        val known = dealt.get(k)
        if known != null then known.send(element)
        else {
          val made = gears.async.BufferedChannel[A | End.type](Lag)
          dealt.put(k, made): Unit
          made.send(element)
          emit(
            (
              k,
              Flow[A] {
                var going = true
                while going do
                  made.read() match {
                    case Right(next) if next.asInstanceOf[AnyRef] ne End => Flow.emit(next.asInstanceOf[A])
                    case _                                              => going = false
                  }
              }
            )
          )
        }
      }
    finally {
      val keys = dealt.values().iterator()
      while keys.hasNext() do keys.next().send(End)
    }
  }
}
