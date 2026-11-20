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
  * that has done with it. The functions given to `map`, `filter` and the like are inlined into that call, so that an
  * element costs one call for each stage it passes. So a slow consumer holds its producer back with nothing said, a consumer that waits for an
  * op or a future has the whole of the pipeline wait with it, and nothing runs between a flow and what it was made of:
  * `map`, `filter` and the rest are functions called in turn.
  *
  * The stack an element passes through is as deep as its pipeline has stages, whatever the number of elements: a
  * flow of a million elements runs in a loop, not a call inside a call. A pipeline of very many stages, a `map`
  * applied in a loop, `++` folded over a list, is cut every `Flow.Segment` of them: what is above the cut runs on a
  * stack of its own and hands each element across, so that no stack holds more than a segment, however long the
  * pipeline. A hand-over costs what waking a future does, and is paid only by pipelines that long. (A flow that runs
  * itself from its own body is not cut: write the loop.)
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

  /** How many flows this one is made of, one of another: how deep the stack is as an element passes through it. */
  def depth: Int = 1
}

/** What a flow's code emits with: given to the body of `Flow { ... }`. */
final class Emit[-A](val each: A => Unit)

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
  inline def apply[A](inline body: Emit[A] ?=> Async ?=> Unit): Flow[A] =
    new Made[Any, A](null, 1, (_, each, async) => body(using new Emit[A](each))(using async))

  /** What `Flow { ... }` and the combinators make: one class for all of them, each a function of its own, so that a
    * place where a flow is made adds a function to the program and not a class.
    */
  final class Made[U, A](from: Flow[U] | Null, override val depth: Int, run: (Flow[U], A => Unit, Async) => Unit)
      extends Flow[A] {
    def foreach(each: A => Unit)(using async: Async): Unit = run(from.asInstanceOf[Flow[U]], each, async)
  }

  /** A flow made of `upstream`, one stage more: `body` is given what to run in its place, which is `upstream`, or,
    * where that is a segment deep already, `upstream` on a stack of its own.
    */
  private[io] inline def over[U, A](upstream: Flow[U])(inline body: Flow[U] => Emit[A] ?=> Async ?=> Unit): Flow[A] = {
    val from = shallow(upstream)
    new Made[U, A](from, from.depth + 1, (from, each, async) => body(from)(using new Emit[A](each))(using async))
  }

  /** How many flows are made one of another before the next is cut off onto a stack of its own. A stage is two or
    * three calls deep as an element passes, and the least of the platforms' stacks, a WebAssembly stack on Node.js,
    * has room for some thousand stages.
    */
  inline val Segment = 128

  /** `flow`, or, if it is a segment deep, `flow` run on a stack of its own. */
  def shallow[A](flow: Flow[A]): Flow[A] = if flow.depth >= Segment then new OnItsOwnStack(flow) else flow

  /** `upstream` run beside its consumer, not under it (`stepping`), so that it is as if the one had called the other
    * with a stack of its own to do it on.
    */
  private final class OnItsOwnStack[A](upstream: Flow[A]) extends Flow[A] {
    def foreach(each: A => Unit)(using Async): Unit =
      stepping(upstream) { next =>
        var element = next()
        while element.asInstanceOf[AnyRef] ne End do {
          each(element.asInstanceOf[A])
          element = next()
        }
      }
  }

  /** What `stepping`'s `next` gives when there is no more. */
  private object End extends RuntimeException(null, null, false, false)

  /** Runs `flow` beside the caller, and gives `body` the way to take its elements one at a time: `next()` is the next
    * element, or `End`, and the flow goes on only when `next` is called again, so that an element is its taker's until
    * then, lent ones too. A failure of the flow's is thrown by `next`. When `body` is done, by its end or otherwise,
    * a flow that has not ended is left where it is, through its `finally` blocks. In a flow of a reactor's the flow is
    * another flow of the reactor's, begun from its loop; elsewhere a future.
    */
  private def stepping[A, R](flow: Flow[A])(body: (() => A | End.type) => R)(using async: Async): R = {
    val reactor = Reactor.of(async)
    if reactor == null then
      // Leaving the group cancels the producer where it waits, which takes it out through its `finally` blocks.
      Async.group {
        val elements = gears.async.SyncChannel[A | End.type]()
        val taken = gears.async.SyncChannel[Unit]()
        val producing = gears.async.Future {
          var failure: Throwable | Null = null
          try
            flow.foreach { element =>
              elements.send(element)
              taken.read(): Unit
            }
          catch case thrown: Throwable => failure = thrown
          // Cancelled, the taker has left and waits for nothing; otherwise it is told that this is the end.
          if !failure.isInstanceOf[java.util.concurrent.CancellationException] then elements.send(End)
          if failure != null then throw failure
        }
        var owed = false
        body { () =>
          if owed then {
            owed = false
            taken.send(())
          }
          elements.read() match {
            case Right(element) if element.asInstanceOf[AnyRef] ne End =>
              owed = true
              element
            case _ =>
              producing.await
              End
          }
        }
      }
    else {
      var slot: A | Null = null
      var full = false
      var ended = false
      var left = false
      var failure: Throwable | Null = null
      val filled = new reactor.Signal
      val emptied = new reactor.Signal
      // Its own group, so that a taker that leaves early can stop it where it waits.
      val group = new gears.async.CompletionGroup
      group.link(async.group)
      reactor.forkLater(group) {
        try
          flow.foreach { element =>
            if left then throw End
            slot = element
            full = true
            filled.wake()
            while full && !left do emptied.park()
            if left then throw End
          }
        catch case End => ()
      } { thrown =>
        if thrown != null && !left then failure = thrown
        ended = true
        group.unlink()
        filled.wake()
      }
      // Whether the taker has the element in the slot: only then is the slot emptied for the next. The producer may
      // well have filled it before it was first asked.
      var owed = false
      try
        body { () =>
          if owed then {
            owed = false
            slot = null
            full = false
            emptied.wake()
          }
          while !full && !ended do filled.park()
          if full then {
            owed = true
            slot.asInstanceOf[A]
          } else {
            val thrown = failure
            if thrown != null then throw thrown
            End
          }
        }
      finally
        if !ended then {
          left = true
          emptied.wake()
          group.cancel()
        }
    }
  }

  /** Runs `body`, which begins what it likes with `beside`, each beside the caller and one another, and returns when
    * `body` has and all it began have ended, with the first failure there was thrown. In a flow of a reactor's they
    * are flows of it, on its thread; elsewhere futures.
    */
  private def together(body: ((Async ?=> Unit) => Unit) => Unit)(using async: Async): Unit = {
    val reactor = Reactor.of(async)
    if reactor == null then
      Async.group {
        val running = scala.collection.mutable.ArrayBuffer[gears.async.Future[Unit]]()
        body(work => running += gears.async.Future(work))
        running.foreach(_.await)
      }
    else {
      val over = new reactor.Signal
      var running = 0
      var failure: Throwable | Null = null
      boundary {
        body { work =>
          if failure != null then break()
          running += 1
          reactor.fork(async.group)(work) { thrown =>
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

  /** The flow that emits what `step` makes of a state, and goes on from the state it gives, until it gives none. */
  def unfold[S, A](start: S)(step: S => Option[(A, S)]): Flow[A] = Flow {
    var state = step(start)
    while state.isDefined do {
      val (element, next) = state.get
      emit(element)
      state = step(next)
    }
  }

  /** `start`, `next` of it, `next` of that, without end. */
  def iterate[A](start: A)(next: A => A): Flow[A] = Flow {
    var element = start
    while true do {
      emit(element)
      element = next(element)
    }
  }

  /** `element`, without end. */
  def repeat[A](element: A): Flow[A] = Flow { while true do emit(element) }

  /** What the flows `gather` runs hand their elements over with, and begin more of themselves with. */
  private abstract class Gather[A] {

    /** Begins `work` beside the caller and the rest. */
    def beside(work: Async ?=> Unit): Unit

    /** Hands `element` to the gatherer, from a flow begun with `beside`: returns when it has done with it. */
    def put(element: A)(using Async): Unit
  }

  /** Runs what `body` begins with `beside`, each beside the caller and one another, and calls `each`, in the caller,
    * with every element they `put`, one at a time; the one that put it goes on when `each` has returned. Returns when
    * all of them have ended, with the first failure there was thrown; left early, they are left where they are.
    *
    * An element is consumed by the flow that asked for it and no other: what consumes a flow was made with that
    * flow's `Async`, and waits with it. So flows that run beside one another do not emit, they hand over.
    */
  private def gather[A](each: A => Unit)(body: Gather[A] => Unit)(using async: Async): Unit = {
    val reactor = Reactor.of(async)
    if reactor == null then
      Async.group { spawn ?=>
        val elements = gears.async.SyncChannel[A | End.type]()
        val taken = gears.async.SyncChannel[Unit]()
        val turn = new gears.async.Semaphore(1)
        // One for `body`, so that the count is not nothing while it may still begin more.
        val running = new java.util.concurrent.atomic.AtomicInteger(1)
        val failure = new java.util.concurrent.atomic.AtomicReference[Throwable | Null](null)
        val gatherer = new Gather[A] {
          def beside(work: Async ?=> Unit): Unit = {
            running.incrementAndGet(): Unit
            gears.async.Future {
              try work
              catch {
                case _: java.util.concurrent.CancellationException => ()
                // The gatherer is told at once, and leaves, which stops the rest.
                case thrown: Throwable => if failure.compareAndSet(null, thrown) then elements.send(End)
              }
              if running.decrementAndGet() == 0 then elements.send(End)
            }(using spawn): Unit
          }
          def put(element: A)(using Async): Unit = {
            val held = turn.acquire()
            try {
              elements.send(element)
              taken.read(): Unit
            } finally held.release()
          }
        }
        body(gatherer)
        var going = running.decrementAndGet() > 0
        while going do
          elements.read() match {
            case Right(element) if element.asInstanceOf[AnyRef] ne End =>
              each(element.asInstanceOf[A])
              taken.send(())
            case _ => going = false
          }
        val thrown = failure.get()
        if thrown != null then throw thrown
      }
    else {
      var slot: A | Null = null
      var full = false
      var puts = 0L
      var done = 0L
      var running = 1
      var left = false
      var failure: Throwable | Null = null
      val filled = new reactor.Signal
      val emptied = new reactor.Signal
      val group = new gears.async.CompletionGroup
      group.link(async.group)
      val gatherer = new Gather[A] {
        def beside(work: Async ?=> Unit): Unit = {
          running += 1
          reactor.fork(group) {
            try work
            catch case End => ()
          } { thrown =>
            running -= 1
            if thrown != null && failure == null && !left then failure = thrown
            filled.wake()
          }
        }
        def put(element: A)(using Async): Unit = {
          while full && !left do emptied.park()
          if left then throw End
          slot = element
          full = true
          val mine = puts
          puts += 1
          filled.wake()
          while done <= mine && !left do emptied.park()
          if left then throw End
        }
      }
      try {
        body(gatherer)
        running -= 1
        var going = true
        while going do {
          while !full && running > 0 && failure == null do filled.park()
          if failure != null then going = false
          else if full then {
            each(slot.asInstanceOf[A])
            slot = null
            full = false
            done += 1
            emptied.wake()
          } else going = false
        }
      } finally {
        if running > 0 then {
          left = true
          emptied.wake()
          group.cancel()
        }
        group.unlink()
      }
      val thrown = failure
      if thrown != null then throw thrown
    }
  }

  /** The flows' elements as they come from any of them, the flows run beside one another: each element whole, one at
    * a time, in no order among the flows.
    */
  def merge[A](flows: Flow[A]*): Flow[A] = Flow {
    gather[A](element => emit(element)) { gathered =>
      flows.foreach(flow => gathered.beside(flow.foreach(element => gathered.put(element))))
    }
  }

  /** The flows' elements, one flow after another, however many there are. */
  def concat[A](flows: IterableOnce[Flow[A]]): Flow[A] = Flow(flows.iterator.foreach(_.foreach(emit)))

  /** Emits `element` from the body of a `Flow { ... }`: returns when its consumer has done with it. */
  inline def emit[A](element: A)(using to: Emit[A]): Unit = to.each(element)

  def of[A](elements: A*): Flow[A] = from(elements)

  def from[A](elements: IterableOnce[A]): Flow[A] = Flow(elements.iterator.foreach(emit))

  val empty: Flow[Nothing] = Flow(())

  /** How far a key's flow of `groupBy`'s may fall behind. */
  inline val Lag = 64

  extension [A](flow: Flow[A]) {

    // ---- one flow of another, nothing running between them

    inline def map[B](inline f: A => B): Flow[B] = over(flow)(from => from.foreach(element => emit(f(element))))

    inline def filter(inline keep: A => Boolean): Flow[A] = over(flow)(from => from.foreach(element => if keep(element) then emit(element)))

    /** For `for a <- flow if ... do`. */
    inline def withFilter(inline keep: A => Boolean): Flow[A] = filter(keep)

    /** The elements of the flows `f` makes of this one's, one flow after another. */
    inline def flatMap[B](inline f: A => Flow[B]): Flow[B] = over(flow)(from => from.foreach(element => f(element).foreach(emit)))

    def collect[B](f: PartialFunction[A, B]): Flow[B] =
      over(flow)(from => from.foreach(element => if f.isDefinedAt(element) then emit(f(element))))

    /** This flow's elements, and then `next`'s. */
    def ++[B >: A](next: Flow[B]): Flow[B] = {
      val after = shallow(next)
      over(flow) { from =>
        from.foreach(emit)
        after.foreach(emit)
      }
    }

    /** The first `count` elements: the flow is left as the last of them has been consumed. */
    def take(count: Long): Flow[A] = over(flow) { from =>
      if count > 0 then
        boundary {
          var left = count
          from.foreach { element =>
            emit(element)
            left -= 1
            if left == 0 then break()
          }
        }
    }

    inline def takeWhile(inline keep: A => Boolean): Flow[A] = over(flow) { from =>
      boundary(from.foreach(element => if keep(element) then emit(element) else break()))
    }

    def drop(count: Long): Flow[A] = over(flow) { from =>
      var left = count
      from.foreach(element => if left > 0 then left -= 1 else emit(element))
    }

    def dropWhile(skip: A => Boolean): Flow[A] = over(flow) { from =>
      var dropping = true
      from.foreach { element =>
        if dropping && !skip(element) then dropping = false
        if !dropping then emit(element)
      }
    }

    /** The elements `f` makes of each, in turn. */
    inline def mapConcat[B](inline f: A => IterableOnce[B]): Flow[B] =
      over(flow)(from => from.foreach(element => f(element).iterator.foreach(emit)))

    /** The elements in lists of `size`, the last of what is left. */
    def grouped(size: Int): Flow[List[A]] = over(flow) { from =>
      require(size > 0, s"A group has at least one element, not $size")
      var group = List.newBuilder[A]
      var held = 0
      from.foreach { element =>
        group += element
        held += 1
        if held == size then {
          emit(group.result())
          group = List.newBuilder[A]
          held = 0
        }
      }
      if held > 0 then emit(group.result())
    }

    /** The elements, without one that is equal to the one before it. */
    def changes: Flow[A] = over(flow) { from =>
      var first = true
      var last: Any = null
      from.foreach { element =>
        if first || element != last then {
          first = false
          last = element
          emit(element)
        }
      }
    }

    /** The elements, with `between` between each two. */
    def intersperse[B >: A](between: B): Flow[B] = over(flow) { from =>
      var first = true
      from.foreach { element =>
        if first then first = false else emit(between)
        emit(element)
      }
    }

    /** Each element with the one `other` has for it, until either ends: `other` is run beside this one, a step at a
      * time.
      */
    def zip[B](other: Flow[B]): Flow[(A, B)] = over(flow) { from =>
      stepping(other) { next =>
        boundary {
          from.foreach { element =>
            val beside = next()
            if beside.asInstanceOf[AnyRef] eq End then break() else emit((element, beside.asInstanceOf[B]))
          }
        }
      }
    }

    /** This flow's elements and `other`'s, as they come from either (`Flow.merge`). */
    def merge[B >: A](other: Flow[B]): Flow[B] = Flow.merge[B](flow, other)

    /** What `f` makes of each element, `f` run for up to `atOnce` of them at a time, each in a flow of its own: emitted
      * as each is done, so in no order. An element goes to a flow that runs on after this one has, so must not be
      * lent.
      */
    def mapPar[B](atOnce: Int)(f: A => Async ?=> B): Flow[B] = over(flow) { from =>
      require(atOnce > 0, s"At least one at a time, not $atOnce")
      val room = new gears.async.Semaphore(atOnce)
      gather[B](made => emit(made)) { gathered =>
        gathered.beside {
          from.foreach { element =>
            val place = room.acquire()
            gathered.beside {
              try gathered.put(f(element))
              finally place.release()
            }
          }
        }
      }
    }

    /** Each element, as it goes by. */
    inline def tap(inline see: A => Unit): Flow[A] = over(flow)(from => from.foreach { element => see(element); emit(element) })

    /** What `f` has made of the elements so far, after each. */
    inline def scan[B](zero: B)(inline f: (B, A) => B): Flow[B] = over(flow) { from =>
      var state = zero
      from.foreach { element =>
        state = f(state, element)
        emit(state)
      }
    }

    def zipWithIndex: Flow[(A, Long)] = over(flow) { from =>
      var index = 0L
      from.foreach { element =>
        emit((element, index))
        index += 1
      }
    }

    /** What `pipe` makes of this flow: `bytes.through(frames).through(requests)`. */
    def through[B](pipe: Flow[A] => Flow[B]): Flow[B] = pipe(flow)

    // ---- running a flow

    inline def fold[B](zero: B)(inline f: (B, A) => B)(using Async): B = {
      var result = zero
      flow.foreach(element => result = f(result, element))
      result
    }

    def toList(using Async): List[A] = {
      val elements = List.newBuilder[A]
      flow.foreach(elements += _)
      elements.result()
    }

    def toVector(using Async): Vector[A] = {
      val elements = Vector.newBuilder[A]
      flow.foreach(elements += _)
      elements.result()
    }

    /** Runs the flow for what it does, its elements dropped. */
    def run()(using Async): Unit = flow.foreach(_ => ())

    def count(using Async): Long = {
      var seen = 0L
      flow.foreach(_ => seen += 1)
      seen
    }

    def sum[B >: A](using number: Numeric[B], async: Async): B = {
      var total = number.zero
      flow.foreach(element => total = number.plus(total, element))
      total
    }

    /** The elements made one by `f`; none if there are none. */
    def reduceOption[B >: A](f: (B, B) => B)(using Async): Option[B] = {
      var result: Option[B] = None
      flow.foreach(element => result = Some(if result.isEmpty then element else f(result.get, element)))
      result
    }

    def lastOption(using Async): Option[A] = {
      var last: Option[A] = None
      flow.foreach(element => last = Some(element))
      last
    }

    /** The first element `found` is true of, the flow left as soon as it has come. */
    def find(found: A => Boolean)(using Async): Option[A] = boundary {
      flow.foreach(element => if found(element) then break(Some(element)))
      None
    }

    def exists(found: A => Boolean)(using Async): Boolean = find(found).isDefined

    def forall(holds: A => Boolean)(using Async): Boolean = find(element => !holds(element)).isEmpty

    def contains[B >: A](element: B)(using Async): Boolean = exists(_ == element)

    def isEmpty(using Async): Boolean = headOption.isEmpty

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
    def groupBy[K](key: A => K): Flow[(K, Flow[A])] = over(flow) { from =>
      val reactor = Reactor.of(summon[Async])
      if reactor == null then dealToChannels(from, key) else dealOnReactor(from, key, reactor)
    }

    /** `f` of every element, each in a flow of its own beside the caller's, which goes on running this one; returns
      * when this flow has ended and every one of them has, with the first failure there was thrown. In a flow of a
      * reactor's they are flows of it, on its thread; elsewhere futures.
      */
    def foreachPar(f: A => Async ?=> Unit)(using Async): Unit =
      together(beside => flow.foreach(element => beside(f(element))))
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
