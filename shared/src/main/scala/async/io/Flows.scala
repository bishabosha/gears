package gears.async.io

import gears.async.Async

/** EXPERIMENT. A stream that is pulled: `pull` gives the next element, or `Flow.End`, suspending its caller, and that
  * alone, for as long as that takes. Nothing is read until something is pulled, which is all its backpressure is. Its
  * combinators are loops over `pull` with nothing running between them.
  *
  * An element may be lent: a connection's bytes are its read buffer, good until the next `pull`. What is kept longer,
  * or given to another flow (`groupBy`), is copied first, by whatever decodes it.
  */
trait Flow[+A] {
  def pull()(using Async): A | Flow.End.type
}

/** What takes elements, one caller's whole and in the order of its calls: `send` returns as soon as the element is
  * taken, which for a connection is before it is written, and waits only if too much is taken and unwritten.
  */
trait Sink[-A] {
  def send(element: A)(using Async): Unit

  /** Waits until all that was sent has gone where it goes. */
  def flush()(using Async): Unit
}

/** A flow made of another: a decoder, with what it keeps between elements. */
trait Pipe[-A, +B] {
  def apply(in: Flow[A]): Flow[B]
}

object Sink {
  extension [A](sink: Sink[A]) {
    inline def contramap[B](inline f: B => A): Sink[B] = new Sink[B] {
      def send(element: B)(using Async): Unit = sink.send(f(element))
      def flush()(using Async): Unit = sink.flush()
    }
  }
}

object Flow {
  object End

  def of[A](elements: A*): Flow[A] = new Flow[A] {
    private val from = elements.iterator
    def pull()(using Async): A | End.type = if from.hasNext then from.next() else End
  }

  extension [A](flow: Flow[A]) {
    inline def map[B](inline f: A => B): Flow[B] = new Flow[B] {
      def pull()(using Async): B | End.type = flow.pull() match {
        case End     => End
        case element => f(element.asInstanceOf[A])
      }
    }

    inline def filter(inline keep: A => Boolean): Flow[A] = new Flow[A] {
      def pull()(using Async): A | End.type = {
        var next = flow.pull()
        while (next.asInstanceOf[AnyRef] ne End) && !keep(next.asInstanceOf[A]) do next = flow.pull()
        next
      }
    }

    def take(count: Long): Flow[A] = new Flow[A] {
      private var left = count
      def pull()(using Async): A | End.type =
        if left <= 0 then End
        else {
          left -= 1
          flow.pull()
        }
    }

    def through[B](pipe: Pipe[A, B]): Flow[B] = pipe(flow)

    inline def foreach(inline f: A => Unit)(using Async): Unit = {
      var next = flow.pull()
      while next.asInstanceOf[AnyRef] ne End do {
        f(next.asInstanceOf[A])
        next = flow.pull()
      }
    }

    inline def fold[B](zero: B)(inline f: (B, A) => B)(using Async): B = {
      var result = zero
      var next = flow.pull()
      while next.asInstanceOf[AnyRef] ne End do {
        result = f(result, next.asInstanceOf[A])
        next = flow.pull()
      }
      result
    }

    /** Sends every element to `sink`, and waits until it has gone. */
    def to(sink: Sink[A])(using Async): Unit = {
      var next = flow.pull()
      while next.asInstanceOf[AnyRef] ne End do {
        sink.send(next.asInstanceOf[A])
        next = flow.pull()
      }
      sink.flush()
    }

    /** The elements dealt by `key`: a flow of the keys, each as it is first met with a flow of its elements. Pulling
      * the keys is what deals; a key's flow has what was dealt to it since, and ends when this flow does. A key's
      * flow that falls `Flow.Lag` elements behind has the dealing wait for it. Its elements go from one flow of a
      * to another, so must not be lent. In a flow of a reactor's the flows are the reactor's; elsewhere each is a channel.
      */
    def groupBy[K](key: A => K)(using async: Async): Flow[(K, Flow[A])] =
      if Reactor.of(async) == null then groupByChannels(flow, key) else groupByOnReactor(key)

    private def groupByOnReactor[K](key: A => K)(using async: Async): Flow[(K, Flow[A])] = {
      val reactor = reactorOf(async)
      var ended = false
      final class Dealt extends Flow[A] {
        val elements = new Ring[AnyRef]()
        val arrived = new reactor.Signal
        val room = new reactor.Signal
        def pull()(using Async): A | End.type = {
          while elements.isEmpty && !ended do arrived.park()
          if elements.isEmpty then End
          else {
            if elements.size == Lag then room.wake()
            elements.poll().asInstanceOf[A]
          }
        }
      }
      val dealt = new java.util.HashMap[K, Dealt]()
      new Flow[(K, Flow[A])] {
        def pull()(using Async): (K, Flow[A]) | End.type = {
          var result: (K, Flow[A]) | End.type | Null = null
          while result == null do
            flow.pull() match {
              case End =>
                ended = true
                dealt.values().forEach(_.arrived.wake())
                result = End
              case next =>
                val element = next.asInstanceOf[A]
                val k = key(element)
                val known = dealt.get(k)
                if known == null then {
                  val made = new Dealt
                  made.elements.add(element.asInstanceOf[AnyRef])
                  dealt.put(k, made): Unit
                  result = (k, made)
                } else {
                  known.elements.add(element.asInstanceOf[AnyRef])
                  known.arrived.wake()
                  while known.elements.size > Lag do known.room.park()
                }
            }
          result.nn
        }
      }
    }

    /** `f` of every element, each in a flow of its own beside the caller's, which goes on pulling; returns when this
      * flow has ended and every one of them has, with the first failure there was thrown. In a flow of a reactor's they
      * are flows of it; elsewhere futures.
      */
    def foreachPar(f: A => Async ?=> Unit)(using async: Async): Unit =
      if Reactor.of(async) == null then foreachFutures(flow, f) else foreachParOnReactor(f)

    private def foreachParOnReactor(f: A => Async ?=> Unit)(using async: Async): Unit = {
      val reactor = reactorOf(async)
      val over = new reactor.Signal
      var running = 0
      var failure: Throwable | Null = null
      var next = flow.pull()
      while (next.asInstanceOf[AnyRef] ne End) && failure == null do {
        val element = next.asInstanceOf[A]
        running += 1
        reactor.fork(async.group)(f(element)) { thrown =>
          running -= 1
          if thrown != null && failure == null then failure = thrown
          over.wake()
        }
        next = flow.pull()
      }
      while running > 0 do over.park()
      val thrown = failure
      if thrown != null then throw thrown
    }
  }

  // ---- the same with no reactor: futures, and channels between them

  /** `groupBy` under any `Async`: a key's flow is a channel, which a future of its own may read. */
  private def groupByChannels[A, K](flow: Flow[A], key: A => K): Flow[(K, Flow[A])] = {
    // A channel closed forgets what it holds: the end is sent, as an element is.
    val dealt = new java.util.HashMap[K, gears.async.BufferedChannel[A | End.type]]()
    new Flow[(K, Flow[A])] {
      def pull()(using Async): (K, Flow[A]) | End.type = {
        var result: (K, Flow[A]) | End.type | Null = null
        while result == null do
          flow.pull() match {
            case End =>
              val keys = dealt.values().iterator()
              while keys.hasNext() do keys.next().send(End)
              result = End
            case next =>
              val element = next.asInstanceOf[A]
              val k = key(element)
              val known = dealt.get(k)
              if known == null then {
                val made = gears.async.BufferedChannel[A | End.type](Lag)
                dealt.put(k, made): Unit
                made.send(element)
                result = (
                  k,
                  new Flow[A] {
                    private var ended = false
                    def pull()(using Async): A | End.type =
                      if ended then End
                      else
                        made.read() match {
                          case Right(element) =>
                            if element.asInstanceOf[AnyRef] eq End then ended = true
                            element
                          case Left(_) => End
                        }
                  }
                )
              } else known.send(element)
          }
        result.nn
      }
    }
  }

  /** `foreachPar` under any `Async`: a future for each element. */
  private def foreachFutures[A](flow: Flow[A], f: A => Async ?=> Unit)(using Async): Unit =
    Async.group {
      val running = scala.collection.mutable.ArrayBuffer[gears.async.Future[Unit]]()
      var next = flow.pull()
      while next.asInstanceOf[AnyRef] ne End do {
        val element = next.asInstanceOf[A]
        running += gears.async.Future(f(element))
        next = flow.pull()
      }
      running.foreach(_.await)
    }

  /** How far a key's flow of `groupBy`'s may fall behind. */
  inline val Lag = 64

  private def reactorOf(async: Async): Reactor[?] = {
    val reactor = Reactor.of(async)
    if reactor == null then throw new IllegalStateException("Only in a flow of a reactor's, a `Server`'s handler say")
    reactor
  }
}
