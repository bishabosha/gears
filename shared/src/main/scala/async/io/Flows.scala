package gears.async.io

import gears.async.Async
import steps.result.Result

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
  final class Made[U, A](
      val from: Flow[U] | Null,
      override val depth: Int,
      run: (Flow[U], A => Unit, Async) => Unit,
      /** For a flow that is one of another by a stage that never waits: what that stage does to the step after it.
        * Null for a flow that is code of its own.
        */
      val stage: ((Step[A], Drive) => Step[U]) | Null = null
  ) extends Flow[A] {
    def foreach(each: A => Unit)(using async: Async): Unit = run(from.asInstanceOf[Flow[U]], each, async)
  }

  /** What elements are pushed to, one at a time, and told when there are no more: a stage of a pipeline as something
    * that is called and returns, with nothing of it left on a stack between elements.
    */
  abstract class Step[-A] {
    def push(element: A): Unit
    def end(): Unit

    /** The pipeline is given up, by a failure, a cancel, or a stage after this that wants no more: nothing more is
      * pushed, and what the step keeps open is closed (`onComplete`). It is passed on to the step after. It may come
      * after `end`, and more than once: a step that does something for it does it once.
      */
    def abandon(): Unit

    /** What comes before this step has failed with `thrown`: it comes in place of the end, to this step from the one
      * before, and is passed on to the one after unless this step makes something else of it (`recover`). The step
      * with nothing after it throws it, as `Unrecovered`, to whatever steps the pipeline.
      *
      * A stage that throws as it is pushed an element throws up through the stages before it, to what steps the
      * pipeline, which then tells the first step, so every step is told. A step that must know the stages before it
      * from those after (`recover` recovers only the first) notes what the step after it threw.
      */
    def fail(thrown: Throwable): Unit
  }

  /** A failure that nothing in its pipeline made anything else of: for whatever steps the pipeline. */
  final class Unrecovered(val thrown: Throwable) extends RuntimeException(null, thrown, false, false)

  /** Whether `recover` and `onError` are for `thrown`: not a cancel, and nothing fatal. */
  private[io] def recoverable(thrown: Throwable): Boolean =
    scala.util.control.NonFatal(thrown) && !thrown.isInstanceOf[java.util.concurrent.CancellationException] &&
      !thrown.isInstanceOf[Unrecovered]

  /** A step that is a function of an element, and ends the step after it. */
  final class Pushing[A](each: A => Unit, next: Step[?]) extends Step[A] {
    def push(element: A): Unit = each(element)
    def end(): Unit = next.end()
    def abandon(): Unit = next.abandon()
    def fail(thrown: Throwable): Unit = next.fail(thrown)
  }

  /** `Pushing`, with something to do at the end before the step after it is ended: what it kept, pushed on. */
  final class Finishing[A](each: A => Unit, atEnd: () => Unit, next: Step[?]) extends Step[A] {
    def push(element: A): Unit = each(element)
    def end(): Unit = {
      atEnd()
      next.end()
    }
    def abandon(): Unit = next.abandon()
    // What it kept is dropped: the pipeline has failed, not ended.
    def fail(thrown: Throwable): Unit = next.fail(thrown)
  }

  /** `recover`'s step: a failure of what comes before it becomes one last element and the end. What the step after
    * it threw is not what came before it, and is passed on.
    */
  final class Recovering[A](handler: PartialFunction[Throwable, A], next: Step[A], drive: Drive) extends Step[A] {
    private var below = false
    private var over = false
    def push(element: A): Unit =
      if !over then
        try next.push(element)
        catch {
          case thrown: Throwable =>
            below = true
            throw thrown
        }
    def end(): Unit =
      if !over then {
        over = true
        next.end()
      }
    def abandon(): Unit = next.abandon()
    def fail(thrown: Throwable): Unit =
      if !over then {
        over = true
        if below || !recoverable(thrown) || !handler.isDefinedAt(thrown) then next.fail(thrown)
        else {
          next.push(handler(thrown))
          next.end()
          // Nothing more is wanted of what failed.
          drive.stop()
        }
      }
  }

  /** `onError`'s step: `see` is shown a failure of what comes before it, which goes on as it was. */
  final class Erroring[A](see: Throwable => Unit, next: Step[A]) extends Step[A] {
    private var below = false
    def push(element: A): Unit =
      try next.push(element)
      catch {
        case thrown: Throwable =>
          below = true
          throw thrown
      }
    def end(): Unit = next.end()
    def abandon(): Unit = next.abandon()
    def fail(thrown: Throwable): Unit = {
      try if !below && recoverable(thrown) then see(thrown)
      finally next.fail(thrown)
    }
  }

  /** `onComplete`'s step: `atLast` once, as the pipeline ends here or is given up, whichever is first. */
  final class Completing[A](atLast: () => Unit, next: Step[A]) extends Step[A] {
    private var done = false
    def push(element: A): Unit = next.push(element)
    def end(): Unit = {
      if !done then {
        done = true
        atLast()
      }
      next.end()
    }
    def abandon(): Unit = {
      // The steps after are told whatever this one's throws.
      try
        if !done then {
          done = true
          atLast()
        }
      finally next.abandon()
    }
    def fail(thrown: Throwable): Unit = {
      try
        if !done then {
          done = true
          atLast()
        }
      finally next.fail(thrown)
    }
  }

  /** A step that may want no more: `each` gives whether it is done, and then the step after it is ended, what
    * steps the pipeline is told to stop, and whatever still comes is dropped.
    */
  final class Until[A](each: A => Boolean, next: Step[?], drive: Drive) extends Step[A] {
    private var over = false
    def push(element: A): Unit =
      if !over && each(element) then {
        over = true
        next.end()
        drive.stop()
      }
    def end(): Unit =
      if !over then {
        over = true
        next.end()
      }
    def abandon(): Unit = next.abandon()
    // After it wanted no more, what failed is nothing to the steps after it, which have ended.
    def fail(thrown: Throwable): Unit =
      if !over then {
        over = true
        next.fail(thrown)
      }
  }

  /** A flow that elements are pushed into by whoever drives a pipeline: the beginning of one that is not run but
    * stepped. It cannot be run.
    */
  final class Port[A] extends Flow[A] {
    def foreach(each: A => Unit)(using Async): Unit =
      throw new UnsupportedOperationException("This flow is driven, an element at a time: it is not run")
  }

  /** What steps a pipeline, as a stage that waits knows it: where its work is begun, how what the work made comes
    * back among the pipeline's elements, and how the source is held while there is too much under way.
    */
  abstract class Drive {

    /** Begins `work`, which may wait, beside the pipeline. */
    def spawn(work: Async ?=> Unit): Unit

    /** From wherever `work` is: runs `run` where the pipeline's stages are called, between its elements. */
    def later(run: Runnable): Unit

    /** Where the stages are called: no more is taken from the source until `release`. */
    def hold(): Unit
    def release(): Unit

    /** Where the stages are called, by a stage that wants no more (`take`): nothing more need come from the source. */
    def stop(): Unit

    /** Begins `body` beside the pipeline, a future of its own whatever steps the pipeline (`spawn` may do its work in
      * place): for a stage that is code of its own (`direct`). What cancels it.
      */
    def start(body: Async ?=> Unit): Runnable

    /** Whether what steps the pipeline is itself run, by a future that may wait: it then waits (`pause`) for a
      * stage's body to have done with what it was pushed, and `later` runs where it is called. Otherwise the stages
      * are called by something that must not wait, and a body goes on beside it.
      */
    def waits: Boolean

    /** Where `waits`: until the runnable that `register` is given is run. */
    def pause(register: Runnable => Unit): Unit
  }

  /** A pipeline that is run, not stepped, driven by the future or flow that runs it: its work is done where it is,
    * waited for there, one piece after another.
    */
  private final class InPlace(using async: Async.Spawn) extends Drive {
    def spawn(work: Async ?=> Unit): Unit = work(using async)
    def later(run: Runnable): Unit = run.run()
    def hold(): Unit = ()
    def release(): Unit = ()
    def stop(): Unit = ()
    def start(body: Async ?=> Unit): Runnable = {
      val running = gears.async.Future(body)
      () => running.cancel()
    }
    def waits: Boolean = true
    def pause(register: Runnable => Unit): Unit = async.park(register)
  }

  /** `flow` as a step: what to push `root`'s elements to so that `flow`'s reach `last`. Null if `flow` is not made of
    * `root` by stages alone.
    */
  private[io] def compile[A](flow: Flow[A], root: Flow[?], last: Step[A], drive: Drive): Step[Any] | Null =
    if flow eq root then last.asInstanceOf[Step[Any]]
    else
      flow match {
        case made: Made[u, A] @unchecked if made.stage != null && made.from != null =>
          compile(made.from.nn, root, made.stage.nn(last, drive), drive)
        case _ => null
      }

  /** A flow made of `upstream` by a stage that never waits: `body` for when it is run, `make` for when it is stepped. */
  private[io] inline def staged[U, A](upstream: Flow[U])(inline body: Flow[U] => Emit[A] ?=> Async ?=> Unit)(
      inline make: Step[A] => Step[U]
  ): Flow[A] = {
    val from = shallow(upstream)
    new Made[U, A](
      from,
      from.depth + 1,
      (from, each, async) => body(from)(using new Emit[A](each))(using async),
      (next, _) => make(next)
    )
  }

  /** `staged`, for a stage that needs what steps it: one that waits, or that steps pipelines of its own. */
  private[io] inline def stagedWith[U, A](upstream: Flow[U])(inline body: Flow[U] => Emit[A] ?=> Async ?=> Unit)(
      inline make: (Step[A], Drive) => Step[U]
  ): Flow[A] = {
    val from = shallow(upstream)
    new Made[U, A](
      from,
      from.depth + 1,
      (from, each, async) => body(from)(using new Emit[A](each))(using async),
      (next, drive) => make(next, drive)
    )
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
    * a flow that has not ended is left where it is, through its `finally` blocks. The flow is a future.
    */
  private def stepping[A, R](flow: Flow[A])(body: (() => A | End.type) => R)(using async: Async): R = {
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
  }

  /** Runs `body`, which begins what it likes with `beside`, each beside the caller and one another, and returns when
    * `body` has and all it began have ended, with the first failure there was thrown. They are futures.
    */
  private def together(body: ((Async ?=> Unit) => Unit) => Unit)(using async: Async): Unit = {
    Async.group {
      val running = scala.collection.mutable.ArrayBuffer[gears.async.Future[Unit]]()
      body(work => running += gears.async.Future(work))
      running.foreach(_.await)
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
  }

  /** The flows' elements as they come from any of them, the flows run beside one another: each element whole, one at
    * a time, in no order among the flows.
    */
  def merge[A](flows: Flow[A]*): Flow[A] = Flow {
    gather[A](element => emit(element)) { gathered =>
      flows.foreach(flow => gathered.beside(flow.foreach(element => gathered.put(element))))
    }
  }

  /** A step with nothing after it. */
  private object Ended extends Step[Any] {
    def push(element: Any): Unit = ()
    def end(): Unit = ()
    def abandon(): Unit = ()
    def fail(thrown: Throwable): Unit = throw new Unrecovered(thrown)
  }

  /** A key's pipeline that has wanted no more: what still comes for it is dropped. */
  private object Stopped extends Step[Any] {
    def push(element: Any): Unit = ()
    def end(): Unit = ()
    def abandon(): Unit = ()
    def fail(thrown: Throwable): Unit = ()
  }

  /** `groupThrough`'s step: a step for each key, made as the key is first met, all of them into `next`. */
  private final class Dealing[A, K, B](key: A => K, sub: (K, Flow[A]) => Flow[B], next: Step[B], drive: Drive)
      extends Step[A] {
    private val keys = new java.util.HashMap[K, Step[Any]]()
    private var open = 0
    private var ended = false
    // A key's pipeline ends into this: what all of them share ends when the last of them has, which with a stage
    // that waits may be after this has been told the end.
    // What the step after all the keys' threw, which is no key's failure; and whether that step has been told of one.
    private var below: Throwable | Null = null
    private var failed = false
    private val shared = new Step[B] {
      def push(element: B): Unit =
        if !failed then
          try next.push(element)
          catch {
            case thrown: Throwable =>
              below = thrown
              throw thrown
          }
      def end(): Unit = {
        open -= 1
        if ended && open == 0 && !failed then next.end()
      }
      // The one step after all the keys' is told once, by `abandon` below.
      def abandon(): Unit = ()
      // A key's failure that the key's pipeline made nothing else of is the whole's.
      def fail(thrown: Throwable): Unit =
        if !failed then {
          failed = true
          next.fail(thrown)
        }
    }

    def push(element: A): Unit = {
      val k = key(element)
      var step = keys.get(k)
      if step == null then {
        val port = new Port[A]
        val mine = new Array[Step[Any] | Null](1)
        // A key's pipeline that wants no more stops its key, not the source: the rest of the key's are dropped, and
        // the stages before the one that stopped are given up.
        val own = new Drive {
          def spawn(work: Async ?=> Unit): Unit = drive.spawn(work)
          def later(run: Runnable): Unit = drive.later(run)
          def hold(): Unit = drive.hold()
          def release(): Unit = drive.release()
          def stop(): Unit = {
            keys.put(k, Stopped): Unit
            val step = mine(0)
            if step != null then step.abandon()
          }
          def start(body: Async ?=> Unit): Runnable = drive.start(body)
          def waits: Boolean = drive.waits
          def pause(register: Runnable => Unit): Unit = drive.pause(register)
        }
        val made = compile(sub(k, port), port, shared, own)
        if made == null then throw new IllegalArgumentException("A key's flow must be made of the flow it is given, by stages")
        step = made
        mine(0) = made
        open += 1
        keys.put(k, step): Unit
      }
      // A stage of the key's that throws has failed the key's pipeline, which may make something else of it
      // (`recover`): the other keys go on.
      try step.push(element)
      catch {
        case unrecovered: Unrecovered => throw unrecovered
        case thrown: Throwable        => if below eq thrown then throw thrown else step.fail(thrown)
      }
    }

    /** What comes before the keys has failed: each key's pipeline is told, and then the step after them all. */
    def fail(thrown: Throwable): Unit = {
      if below ne thrown then {
        val each = keys.values().toArray()
        var i = 0
        while i < each.length do {
          each(i).asInstanceOf[Step[Any]].fail(thrown)
          i += 1
        }
      }
      if !failed then {
        failed = true
        next.fail(thrown)
      }
    }

    def end(): Unit = {
      ended = true
      if failed then ()
      else if open == 0 then next.end()
      else {
        val each = keys.values().iterator()
        while each.hasNext() do each.next().end()
      }
    }

    def abandon(): Unit = {
      var thrown: Throwable | Null = null
      val each = keys.values().iterator()
      while each.hasNext() do
        try each.next().abandon()
        catch case failed: Throwable => if thrown == null then thrown = failed
      try next.abandon()
      catch case failed: Throwable => if thrown == null then thrown = failed
      if thrown != null then throw thrown
    }
  }

  /** `mapAsync`'s step: work begun for an element, what it made pushed on when it is done, the rest waiting their
    * turn. All of it where the pipeline's stages are called, but the work itself.
    */
  private final class Waiting[A, B](atOnce: Int, f: A => Async ?=> B, next: Step[B], drive: Drive) extends Step[A] {
    private val waiting = new Ring[AnyRef]()
    private var flying = 0
    private var holding = false
    private var ended = false

    private final class Failed(val thrown: Throwable)

    private def begin(element: A): Unit = {
      flying += 1
      drive.spawn {
        val made: Any =
          try f(element)
          catch case thrown: Throwable => new Failed(thrown)
        drive.later(() => landed(made))
      }
    }

    // Its work has failed, or what came before it: the steps after have been told, and are told nothing more.
    private var dead = false

    private def landed(made: Any): Unit = {
      flying -= 1
      if !dead then
        made match {
          case failed: Failed @unchecked =>
            dead = true
            next.fail(failed.thrown)
          case _ => next.push(made.asInstanceOf[B])
        }
      if !dead && !waiting.isEmpty then begin(waiting.poll().asInstanceOf[A])
      if holding && (dead || waiting.isEmpty) then {
        holding = false
        drive.release()
      }
      if ended && flying == 0 && !dead then next.end()
    }

    def push(element: A): Unit =
      if dead then ()
      else if flying < atOnce then begin(element)
      else {
        waiting.add(element.asInstanceOf[AnyRef])
        if !holding then {
          holding = true
          drive.hold()
        }
      }

    def end(): Unit = {
      ended = true
      if flying == 0 && !dead then next.end()
    }

    // Its work under way is what steps the pipeline's to cancel (`Drive`), which knows whose it is.
    def abandon(): Unit = next.abandon()

    def fail(thrown: Throwable): Unit =
      if !dead then {
        dead = true
        next.fail(thrown)
      }
  }

  /** `Waiting`, with what the work makes pushed on in the order the work was begun (`ordered`). */
  private final class WaitingInOrder[A, B](atOnce: Int, ordered: Boolean, f: A => Async ?=> B, next: Step[B], drive: Drive)
      extends Step[A] {
    private val waiting = new Ring[AnyRef]()
    // In order: a place for what each piece of work makes, in the order the work was begun.
    private val places = if ordered then new Ring[Place]() else null
    private var flying = 0
    private var holding = false
    private var ended = false

    private final class Failed(val thrown: Throwable)
    private final class Place {
      var made: Any = null
      var done = false
    }

    private def begin(element: A): Unit = {
      flying += 1
      val place = if ordered then new Place else null
      if ordered then places.nn.add(place.nn)
      drive.spawn {
        val made: Any =
          try f(element)
          catch case thrown: Throwable => new Failed(thrown)
        drive.later(() => landed(made, place))
      }
    }

    private var dead = false

    private def pass(made: Any): Unit =
      if !dead then
        made match {
          case failed: Failed @unchecked =>
            dead = true
            next.fail(failed.thrown)
          case _ => next.push(made.asInstanceOf[B])
        }

    private def landed(made: Any, place: Place | Null): Unit = {
      flying -= 1
      if place == null then pass(made)
      else {
        place.made = made
        place.done = true
        val order = places.nn
        while !order.isEmpty && order.peek().nn.done do pass(order.poll().nn.made)
      }
      if !dead && !waiting.isEmpty then begin(waiting.poll().asInstanceOf[A])
      if holding && (dead || waiting.isEmpty) then {
        holding = false
        drive.release()
      }
      if ended && flying == 0 && !dead then next.end()
    }

    def push(element: A): Unit =
      if dead then ()
      else if flying < atOnce then begin(element)
      else {
        waiting.add(element.asInstanceOf[AnyRef])
        if !holding then {
          holding = true
          drive.hold()
        }
      }

    def end(): Unit = {
      ended = true
      if flying == 0 && !dead then next.end()
    }

    // Its work under way is what steps the pipeline's to cancel (`Drive`), which knows whose it is.
    def abandon(): Unit = next.abandon()

    def fail(thrown: Throwable): Unit =
      if !dead then {
        dead = true
        next.fail(thrown)
      }
  }

  /** `mapPar`, run: its work in futures, awaited in the order it was begun. */
  private def mapParRun[A, B](from: Flow[A], parallelism: Int, f: A => Async ?=> B)(using Emit[B], Async): Unit = {
      Async.group { outer ?=>
        // The work under way, in the order its elements came: begun by a future that runs this flow, and awaited
        // here, each as it comes to the head, so that what is done is emitted without waiting for what follows it.
        val under = gears.async.BufferedChannel[Option[gears.async.Future[B]]](parallelism)
        val room = new gears.async.Semaphore(parallelism)
        val driving = gears.async.Future {
          try
            from.foreach { element =>
              val place = room.acquire()
              // The group's, not this future's: its work goes on when this flow has ended.
              val work = gears.async.Future {
                try f(element)
                finally place.release()
              }(using outer)
              under.send(Some(work))
            }
          finally under.send(None)
        }
        var going = true
        while going do
          under.read() match {
            case Right(Some(work)) => emit(work.await)
            case _                 => going = false
          }
        driving.await
      }
    }

  /** The flows' elements, one flow after another, however many there are. */
  def concat[A](flows: IterableOnce[Flow[A]]): Flow[A] = Flow(flows.iterator.foreach(_.foreach(emit)))

  /** Emits `element` from the body of a `Flow { ... }`: returns when its consumer has done with it. */
  inline def emit[A](element: A)(using to: Emit[A]): Unit = to.each(element)

  def of[A](elements: A*): Flow[A] = from(elements)

  // Named as Ox's `Flow` names them, so that what is written for one reads as the other (see `docs/io`).

  /** The flow of what `withEmit` emits: `Flow { ... }` with the emitter handed over, as Ox has it. */
  inline def usingEmit[A](inline withEmit: (A => Unit) => Async ?=> Unit): Flow[A] = Flow[A](withEmit(emit))

  def fromValues[A](elements: A*): Flow[A] = from(elements)
  def fromIterable[A](elements: Iterable[A]): Flow[A] = from(elements)
  def fromIterator[A](elements: => Iterator[A]): Flow[A] = Flow(elements.foreach(emit))

  def from[A](elements: IterableOnce[A]): Flow[A] = Flow(elements.iterator.foreach(emit))

  val empty: Flow[Nothing] = Flow(())

  /** How far a key's flow of `groupBy`'s may fall behind. */
  inline val Lag = 64

  extension [A](flow: Flow[A]) {

    // ---- one flow of another, nothing running between them

    inline def map[B](inline f: A => B): Flow[B] =
      staged[A, B](flow)(from => from.foreach(element => emit(f(element))))(next =>
        new Pushing[A](element => next.push(f(element)), next)
      )

    inline def filter(inline keep: A => Boolean): Flow[A] =
      staged[A, A](flow)(from => from.foreach(element => if keep(element) then emit(element)))(next =>
        new Pushing[A](element => if keep(element) then next.push(element), next)
      )

    /** For `for a <- flow if ... do`. */
    inline def withFilter(inline keep: A => Boolean): Flow[A] = filter(keep)

    /** The elements of the flows `f` makes of this one's, one flow after another. */
    inline def flatMap[B](inline f: A => Flow[B]): Flow[B] = over(flow)(from => from.foreach(element => f(element).foreach(emit)))

    def collect[B](f: PartialFunction[A, B]): Flow[B] =
      staged[A, B](flow)(from => from.foreach(element => if f.isDefinedAt(element) then emit(f(element))))(next =>
        new Pushing[A](element => if f.isDefinedAt(element) then next.push(f(element)), next)
      )

    /** This flow's elements, and then `next`'s. */
    def ++[B >: A](next: Flow[B]): Flow[B] = {
      val after = shallow(next)
      over(flow) { from =>
        from.foreach(emit)
        after.foreach(emit)
      }
    }

    /** The first `count` elements: the flow is left as the last of them has been consumed. */
    def take(count: Long): Flow[A] =
      stagedWith[A, A](flow) { from =>
        if count > 0 then
          boundary {
            var left = count
            from.foreach { element =>
              emit(element)
              left -= 1
              if left == 0 then break()
            }
          }
      } { (next, drive) =>
        var left = count
        new Until[A](
          element =>
            left <= 0 || {
              next.push(element)
              left -= 1
              left == 0
            },
          next,
          drive
        )
      }

    inline def takeWhile(inline keep: A => Boolean): Flow[A] =
      stagedWith[A, A](flow) { from =>
        boundary(from.foreach(element => if keep(element) then emit(element) else break()))
      } { (next, drive) =>
        new Until[A](element => if keep(element) then { next.push(element); false } else true, next, drive)
      }

    def drop(count: Long): Flow[A] =
      staged[A, A](flow) { from =>
        var left = count
        from.foreach(element => if left > 0 then left -= 1 else emit(element))
      } { next =>
        var left = count
        new Pushing[A](element => if left > 0 then left -= 1 else next.push(element), next)
      }

    def dropWhile(skip: A => Boolean): Flow[A] =
      staged[A, A](flow) { from =>
        var dropping = true
        from.foreach { element =>
          if dropping && !skip(element) then dropping = false
          if !dropping then emit(element)
        }
      } { next =>
        var dropping = true
        new Pushing[A](
          element => {
            if dropping && !skip(element) then dropping = false
            if !dropping then next.push(element)
          },
          next
        )
      }

    /** The elements `f` makes of each, in turn. */
    inline def mapConcat[B](inline f: A => IterableOnce[B]): Flow[B] =
      staged[A, B](flow)(from => from.foreach(element => f(element).iterator.foreach(emit)))(next =>
        new Pushing[A](element => f(element).iterator.foreach(next.push), next)
      )

    /** A stage with something of its own to keep between elements, a decoder say: `handler` is called once, as the
      * flow begins, with what to call for each element it makes, and gives what to call with each element that
      * comes. Neither may wait: it is a stage that is stepped, not run.
      *
      * {{{
      * bytes.process[String] { emit =>
      *   val line = new StringBuilder
      *   chunk => while chunk.hasRemaining() do { ... emit(line.result()) ... }
      * }
      * }}}
      */
    def process[B](handler: (B => Unit) => (A => Unit)): Flow[B] =
      staged[A, B](flow)(from => from.foreach(handler(emit)))(next => new Pushing[A](handler(next.push), next))

    /** The elements dealt by `key`, each key's through a pipeline of its own, and what those make, as it is made:
      * `sub` is called once for a key, as it is first met, with the key and the flow of its elements, and gives what
      * becomes of them. It is `groupBy` for streams that never wait: nothing runs beside anything, a key's stages are
      * called with each of its elements as it comes. `sub`'s flow must be made of the one it is given by stages.
      */
    def groupThrough[K, B](key: A => K)(sub: (K, Flow[A]) => Flow[B]): Flow[B] =
      stagedWith[A, B](flow) { from =>
        // A group, for a key's stage that is code of its own (`direct`), which is a future beside this one.
        Async.group {
          val dealing = new Dealing[A, K, B](key, sub, new Pushing[B](emit, Ended), new InPlace)
          def givenUp(thrown: Throwable): Nothing = {
            try dealing.abandon()
            catch case _: Throwable => ()
            throw (thrown match {
              case unrecovered: Unrecovered => unrecovered.thrown
              case _                        => thrown
            })
          }
          var upstream = true
          try {
            try from.foreach { element =>
              // Left false by a push that throws: that is no failure of what comes before the keys.
              upstream = false
              dealing.push(element)
              upstream = true
            }
            catch {
              // What comes before the keys has failed: the keys' pipelines are told, and may make something of it.
              case thrown: Throwable if upstream && recoverable(thrown) => dealing.fail(thrown)
            }
            dealing.end()
          } catch {
            // A failure nothing made anything else of, a cancel, or a consumer that has left: the keys' pipelines are
            // given up.
            case thrown: Throwable => givenUp(thrown)
          }
        }
      }((next, drive) => new Dealing[A, K, B](key, sub, next, drive))

    /** A stage that waits: `f` of each element, which may perform ops and await, up to `atOnce` of them under way at
      * a time, and what each made as it is done, so in no order among those under way together. With more elements
      * than that, the source is held: nothing more is read until one is done.
      *
      * In a pipeline that is stepped (`Server.serveFlow`) each `f` is a future beside it, and what it made comes back
      * among the pipeline's elements, on the pipeline's thread. In one that is run it is `mapParUnordered`. An element goes to
      * work that goes on after the stage has returned, so must not be lent; and `f` runs beside its like, so what
      * they share is locked.
      */
    def mapAsync[B](atOnce: Int)(f: A => Async ?=> B): Flow[B] = {
      require(atOnce > 0, s"At least one at a time, not $atOnce")
      stagedWith[A, B](flow)(from => from.mapParUnordered(atOnce)(f).foreach(emit))((next, drive) =>
        new Waiting[A, B](atOnce, f, next, drive)
      )
    }

    /** The elements in lists of `size`, the last of what is left. */
    def grouped(size: Int): Flow[List[A]] = {
      require(size > 0, s"A group has at least one element, not $size")
      staged[A, List[A]](flow) { from =>
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
      } { next =>
        var group = List.newBuilder[A]
        var held = 0
        new Finishing[A](
          element => {
            group += element
            held += 1
            if held == size then {
              val full = group.result()
              group = List.newBuilder[A]
              held = 0
              next.push(full)
            }
          },
          () => if held > 0 then next.push(group.result()),
          next
        )
      }
    }

    /** The elements, without one that is equal to the one before it. */
    def changes: Flow[A] =
      staged[A, A](flow) { from =>
        var first = true
        var last: Any = null
        from.foreach { element =>
          if first || element != last then {
            first = false
            last = element
            emit(element)
          }
        }
      } { next =>
        var first = true
        var last: Any = null
        new Pushing[A](
          element =>
            if first || element != last then {
              first = false
              last = element
              next.push(element)
            },
          next
        )
      }

    /** The elements, with `between` between each two. */
    def intersperse[B >: A](between: B): Flow[B] =
      staged[A, B](flow) { from =>
        var first = true
        from.foreach { element =>
          if first then first = false else emit(between)
          emit(element)
        }
      } { next =>
        var first = true
        new Pushing[A](
          element => {
            if first then first = false else next.push(between)
            next.push(element)
          },
          next
        )
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
    def mapParUnordered[B](atOnce: Int)(f: A => Async ?=> B): Flow[B] = over(flow) { from =>
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

    /** What `f` makes of each element, `f` run for up to `parallelism` of them at a time, each in a future of its
      * own, and emitted in the order the elements came, as Ox's `mapPar` does: `mapAsync` in order, and stepped as
      * it is. An element goes to work that runs on after this has, so must not be lent.
      */
    def mapPar[B](parallelism: Int)(f: A => Async ?=> B): Flow[B] = {
      require(parallelism > 0, s"At least one at a time, not $parallelism")
      stagedWith[A, B](flow)(from => mapParRun(from, parallelism, f))((next, drive) =>
        new WaitingInOrder[A, B](parallelism, true, f, next, drive)
      )
    }

    /** A stage with something to keep between elements, which is given and gives back with each: as Ox's. */
    def mapStateful[S, B](initial: => S)(f: (S, A) => (S, B)): Flow[B] =
      flow.process[B] { emit =>
        var state = initial
        element => {
          val (next, made) = f(state, element)
          state = next
          emit(made)
        }
      }

    /** What `f` emits for each element: Ox's name for a stage that may make none or many. */
    def mapUsingEmit[B](f: A => (B => Unit) => Unit): Flow[B] = flow.process[B](emit => element => f(element)(emit))

    /** The elements dealt by `key`, each key's through `child` of it, as Ox's `groupBy`: `groupThrough` under its
      * name there. `parallelism` is Ox's bound on the keys there are at once, which here is not kept: a key's
      * pipeline is a step, not a thread, and there may be any number.
      */
    def groupBy[K, B](parallelism: Int, key: A => K)(child: K => Flow[A] => Flow[B]): Flow[B] =
      flow.groupThrough(key)((k, elements) => child(k)(elements))

    /** `next` after this flow. */
    def concat[B >: A](next: Flow[B]): Flow[B] = flow ++ next

    // Running a flow, under Ox's names.
    def runForeach(sink: A => Unit)(using Async): Unit = flow.foreach(sink)
    def runToList()(using Async): List[A] = flow.toList
    def runDrain()(using Async): Unit = flow.run()
    def runFold[B](zero: B)(f: (B, A) => B)(using Async): B = flow.fold(zero)(f)
    def runLastOption()(using Async): Option[A] = flow.lastOption

    /** `atLast`, once, when this flow has ended, however it did: with its last element, by a failure, by a cancel, or
      * because what consumes it wants no more. Ox's name for it. It is where a stage closes what it opened, and it
      * is run though the future that runs the flow, or the server that steps it, has been cancelled. It must not wait.
      */
    def onComplete(atLast: => Unit): Flow[A] =
      staged[A, A](flow) { from =>
        try from.foreach(emit)
        finally atLast
      }(next => new Completing[A](() => atLast, next))

    /** A failure of this flow, where `handler` has something for it, becomes one last element, and the flow ends
      * there as if it had ended: Ox's name, and what ZIO calls catching. It is this flow's failures only, those of
      * the stages before `recover`; what a stage after it throws is not recovered by it. A cancel is no failure.
      *
      * In a key's pipeline (`groupThrough`) it ends that key and leaves the others; with nothing to recover it, a
      * key's failure is the whole flow's, and a connection's pipeline's is its connection's.
      */
    def recover[B >: A](handler: PartialFunction[Throwable, B]): Flow[B] =
      stagedWith[A, B](flow) { from =>
        var below = false
        try
          from.foreach { element =>
            try emit(element)
            catch {
              case thrown: Throwable =>
                below = true
                throw thrown
            }
          }
        catch {
          case thrown: Throwable if !below && recoverable(thrown) && handler.isDefinedAt(thrown) => emit(handler(thrown))
        }
      }((next, drive) => new Recovering[B](handler, next, drive).asInstanceOf[Step[A]])

    /** `see`, shown a failure of this flow, which fails all the same: Ox's name. As `recover`, not what a stage
      * after it throws, and not a cancel.
      */
    def onError(see: Throwable => Unit): Flow[A] =
      staged[A, A](flow) { from =>
        var below = false
        try
          from.foreach { element =>
            try emit(element)
            catch {
              case thrown: Throwable =>
                below = true
                throw thrown
            }
          }
        catch {
          case thrown: Throwable if !below && recoverable(thrown) =>
            see(thrown)
            throw thrown
        }
      }(next => new Erroring[A](see, next))

    /** A stage that is code: `body` is given this flow, and what it emits is the stage's. It is written as a
      * `Flow { ... }` is, a loop over the flow that may keep what it likes between elements, wait, perform ops, and
      * leave early, and its `try` is the stage's error handling: a failure of this flow is thrown from the loop over
      * it, where `catch` and `finally` are as anywhere. What `body` throws is the stage's failure, for the stages
      * after it; what a stage after it throws does not come back to it.
      *
      * {{{
      * requests.direct[Response] { requests =>
      *   val session = open()
      *   try for request <- requests do emit(session.answer(request))
      *   catch case _: Malformed => emit(Response.BadRequest)
      *   finally session.close()
      * }
      * }}}
      *
      * Unlike `Flow { ... }` it is a stage, so a pipeline with it in is still stepped (`Server.serveFlow`): the body
      * is then a future beside the pipeline, handed each element and waited for at each `emit` by nothing but
      * itself, and cancelled with its connection. That is a hand-over an element each way, which the stages that
      * are functions (`map`, `process`) have not: they are for what must be quick, and this for what is easier said
      * as code. An element is handed to code that runs on, so must not be lent.
      */
    def direct[B](body: Flow[A] => Emit[B] ?=> Async ?=> Unit): Flow[B] =
      stagedWith[A, B](flow)(from => runDirect(from, body))((next, drive) => new Direct[A, B](body, next, drive))

    /** Each element, as it goes by. */
    inline def tap(inline see: A => Unit): Flow[A] =
      staged[A, A](flow)(from => from.foreach { element => see(element); emit(element) })(next =>
        new Pushing[A](element => { see(element); next.push(element) }, next)
      )

    /** What `f` has made of the elements so far, after each. */
    inline def scan[B](zero: B)(inline f: (B, A) => B): Flow[B] =
      staged[A, B](flow) { from =>
        var state = zero
        from.foreach { element =>
          state = f(state, element)
          emit(state)
        }
      } { next =>
        var state = zero
        new Pushing[A](
          element => {
            state = f(state, element)
            next.push(state)
          },
          next
        )
      }

    def zipWithIndex: Flow[(A, Long)] =
      staged[A, (A, Long)](flow) { from =>
        var index = 0L
        from.foreach { element =>
          emit((element, index))
          index += 1
        }
      } { next =>
        var index = 0L
        new Pushing[A](
          element => {
            val at = index
            index += 1
            next.push((element, at))
          },
          next
        )
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
      */
    def groupBy[K](key: A => K): Flow[(K, Flow[A])] = over(flow)(from => dealToChannels(from, key))

    /** `f` of every element, each in a flow of its own beside the caller's, which goes on running this one; returns
      * when this flow has ended and every one of them has, with the first failure there was thrown. They are futures.
      */
    def foreachPar(f: A => Async ?=> Unit)(using Async): Unit =
      together(beside => flow.foreach(element => beside(f(element))))
  }

  /** `groupBy`: a key's flow is a queue that one future fills and another empties, each parked
    * when it has nothing to do there (`Async.park`), with no lock and no listener: woken by the other, or not at all.
    */
  private def dealToChannels[A, K](flow: Flow[A], key: A => K)(using Emit[(K, Flow[A])], Async): Unit = {
    val dealt = new java.util.HashMap[K, Mailbox]()
    try
      flow.foreach { element =>
        val k = key(element)
        val known = dealt.get(k)
        if known != null then known.put(element.asInstanceOf[AnyRef])
        else {
          val made = new Mailbox
          dealt.put(k, made): Unit
          made.put(element.asInstanceOf[AnyRef])
          emit((k, new FromMailbox[A](made)))
        }
      }
    finally {
      val keys = dealt.values().iterator()
      while keys.hasNext() do keys.next().put(End)
    }
  }

  /** A key's flow of `groupBy`'s: what its mailbox is given, until the end. A class, not a `Flow { ... }`: it is where
    * a stream's future waits, and whatever is between that and its beginning is copied at each wait.
    */
  private final class FromMailbox[A](mailbox: Mailbox) extends Flow[A] {
    def foreach(each: A => Unit)(using Async): Unit = {
      var next = mailbox.take()
      while next ne End do {
        each(next.asInstanceOf[A])
        next = mailbox.take()
      }
    }
  }

  // ---- Failures as values: a flow of `Result`s (lampepfl/steps), with no exception in it.

  extension [A](flow: Flow[A]) {

    /** A stage that is code, as `direct`, whose failures are values: the body is a `Result.task`, in which a `Result`
      * is unwrapped with `.ok`, checked with `.check`, and an error raised with `raise`, and the stage's elements are
      * `Ok` of what the body emits, and then, if it raised one, the `Err`, which is the last.
      *
      * {{{
      * import steps.result.Result.eval.{ok, raise}
      *
      * requests.directResult[Response, Malformed] { requests =>
      *   val session = open()
      *   try for request <- requests do emit(session.answer(parse(request).ok))
      *   finally session.close()
      * }
      * }}}
      *
      * Nothing is thrown for a failure and nothing is caught: an error of what comes before is an element like any
      * other, an `Err`, to unwrap, match on or pass by; what is to be cleaned up is cleaned up in a `finally` with no
      * `catch` beside it, which is run however the stage ends, a cancel among the ways.
      */
    def directResult[B, E](
        body: Flow[A] => (Emit[B], boundary.Label[Result[Unit, E]], Async) ?=> Unit
    ): Flow[Result[B, E]] =
      flow.direct[Result[B, E]] { elements =>
        val out = summon[Emit[Result[B, E]]]
        val ended: Result[Unit, E] =
          Result.task[E](body(elements)(using new Emit[B](element => out.each(Result.Ok(element)))))
        if ended.isErr then out.each(ended.asInstanceOf[Result[B, E]])
      }

    /** This flow's elements as `Ok`, and what it throws as a last `Err`: from failures that are thrown to failures
      * that are values. A cancel is not a failure, nor is anything fatal.
      */
    def attempt: Flow[Result[A, Throwable]] =
      flow.map[Result[A, Throwable]](Result.Ok(_)).recover { case thrown => Result.Err(thrown) }
  }

  extension [A, E](flow: Flow[Result[A, E]]) {

    /** The flow as far as its first `Err`, which is its last element: one failure ends it. The stages below that
      * may make an `Err` do this themselves; it is for a flow of `Result`s made otherwise, by `map` say, which goes
      * on past an `Err`. In a key's pipeline (`groupThrough`) it ends that key.
      */
    def failFast: Flow[Result[A, E]] =
      stagedWith[Result[A, E], Result[A, E]](flow) { from =>
        boundary {
          from.foreach { element =>
            emit(element)
            if element.isErr then break()
          }
        }
      }((next, drive) => new Until[Result[A, E]](element => { next.push(element); element.isErr }, next, drive))

    /** `f` of each `Ok`; an `Err` as it is. */
    def mapOk[B](f: A => B): Flow[Result[B, E]] = flow.map(_.map(f))

    /** `f` of each `Ok`, which may fail: the first `Err` it makes is the flow's last element. */
    def flatMapOk[B, E1 >: E](f: A => Result[B, E1]): Flow[Result[B, E1]] = flow.map(_.flatMap(f)).failFast

    /** `f` of each `Err`; an `Ok` as it is. */
    def mapErr[E1](f: E => E1): Flow[Result[A, E1]] = flow.map(_.mapErr(f))

    /** Shown each `Err`, which goes on as it was. */
    def tapErr(see: E => Unit): Flow[Result[A, E]] = flow.tap(_.tapErr(see): Unit)

    /** The values: an `Ok`'s, and for an `Err` what `f` makes of it. What recovers a failure, a response to send
      * in its place say.
      */
    def handleErr[B >: A](f: E => B): Flow[B] =
      flow.map {
        case Result.Ok(value)  => value
        case Result.Err(error) => f(error)
      }

    /** A stage that is code over the values (`directResult`): an `Err` of this flow ends it, and is its last. */
    def directOk[B, E1 >: E](
        body: Flow[A] => (Emit[B], boundary.Label[Result[Unit, E1]], Async) ?=> Unit
    ): Flow[Result[B, E1]] =
      flow.directResult[B, E1] { elements =>
        val label = summon[boundary.Label[Result[Unit, E1]]]
        val values = new Flow[A] {
          def foreach(each: A => Unit)(using Async): Unit =
            elements.foreach {
              case Result.Ok(value) => each(value)
              case failed           => boundary.break(failed.asInstanceOf[Result[Unit, E1]])(using label)
            }
        }
        body(values)
      }

    /** Runs the flow: `Ok` when it has ended, or its first `Err`, at which it is left. */
    def runResult()(using Async): Result[Unit, E] =
      boundary[Result[Unit, E]] {
        flow.foreach(element => if element.isErr then break(element.asInstanceOf[Result[Unit, E]]))
        Result.done
      }

    /** Runs the flow: its values, or its first `Err`, at which it is left. */
    def toListResult()(using Async): Result[List[A], E] =
      boundary[Result[List[A], E]] {
        val values = List.newBuilder[A]
        flow.foreach {
          case Result.Ok(value) => values += value
          case failed           => break(failed.asInstanceOf[Result[List[A], E]])
        }
        Result.Ok(values.result())
      }
  }

  /** Thrown into a `direct` stage's body from its `emit` when what consumes the stage has thrown, or left: not the
    * body's failure to catch, though its `finally` is run. What was thrown is thrown on when the body has ended.
    */
  private object Gone extends scala.util.control.ControlThrowable

  /** `direct`, run: the body where the flow is run, but that what the stages after it throw is kept from its `catch`. */
  private def runDirect[A, B](from: Flow[A], body: Flow[A] => Emit[B] ?=> Async ?=> Unit)(using to: Emit[B], async: Async): Unit = {
    var below: Throwable | Null = null
    val guarded = new Emit[B](element => {
      if below != null then throw Gone
      try to.each(element)
      catch {
        case thrown: Throwable =>
          below = thrown
          throw Gone
      }
    })
    try body(from)(using guarded)(using async)
    catch case thrown: Throwable => if below == null then throw thrown
    val thrown = below
    if thrown != null then throw thrown
  }

  /** `direct`'s step: the stage's body is a future beside the pipeline, which is handed what the step is pushed and
    * hands back what it emits, to be pushed on where the pipeline's stages are called.
    */
  private final class Direct[A, B](body: Flow[A] => Emit[B] ?=> Async ?=> Unit, next: Step[B], drive: Drive)
      extends Step[A] {
    import java.util.concurrent.atomic.AtomicReference
    private inline val Most = 64

    private object End
    private final class Failed(val thrown: Throwable)

    // From the pipeline's thread to the body: elements, the end, a failure of what came before.
    private val inbox = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]()
    private val pending = new java.util.concurrent.atomic.AtomicInteger(0)
    private val reader = new AtomicReference[Runnable | Null](null)
    // What steps the pipeline, waiting for the body to have done with what it was handed (`Drive.waits`).
    private val watcher = new AtomicReference[Runnable | Null](null)
    @volatile private var idle = false
    @volatile private var holding = false
    @volatile private var upstreamEnded = false
    // What a step after this one threw as the body's element was pushed to it: not the body's to see.
    @volatile private var below: Throwable | Null = null
    // The pipeline's thread's: the steps after have been told the end, or a failure, or the stage is given up.
    @volatile private var over = false
    private var cancel: Runnable | Null = null

    private def wake(parked: AtomicReference[Runnable | Null]): Unit =
      if parked.get() != null then {
        val resume = parked.getAndSet(null)
        if resume != null then resume.run()
      }

    // ---- the body's side

    private def take()(using async: Async): AnyRef = {
      var got = inbox.poll()
      while got == null do {
        idle = true
        wake(watcher)
        async.park { resume =>
          reader.set(resume)
          if !inbox.isEmpty() then wake(reader)
        }
        idle = false
        got = inbox.poll()
      }
      if pending.decrementAndGet() == 0 && holding then
        drive.later { () =>
          if holding then {
            holding = false
            drive.release()
          }
        }
      got
    }

    private val source = new Flow[A] {
      def foreach(each: A => Unit)(using Async): Unit = {
        var going = true
        while going do
          take() match {
            case End                       => going = false
            case failed: Failed @unchecked => throw failed.thrown
            case element                   => each(element.asInstanceOf[A])
          }
      }
    }

    /** `run` where the pipeline's stages are called, and the body goes on when it has been. */
    private def hand(run: () => Unit)(using async: Async): Unit =
      if drive.waits then {
        // What steps the pipeline is waiting for this body, so it is run here; what it throws is that one's.
        try run()
        catch {
          case thrown: Throwable =>
            below = thrown
            throw Gone
        }
      } else {
        val gate = new AtomicReference[AnyRef | Null](null)
        drive.later { () =>
          try run()
          catch {
            case thrown: Throwable =>
              below = thrown
              throw thrown
          } finally {
            val parked = gate.getAndSet(gate)
            if parked != null then parked.asInstanceOf[Runnable].run()
          }
        }
        if gate.get() == null then async.park(resume => if !gate.compareAndSet(null, resume) then resume.run())
      }

    private def run()(using Async): Unit = {
      var thrown: Throwable | Null = null
      try body(source)(using new Emit[B](element => hand(() => if !over then next.push(element))))
      catch case failed: Throwable => thrown = failed
      val failure = thrown
      try
        if below == null && !failure.isInstanceOf[java.util.concurrent.CancellationException] && (failure ne Gone) then
          hand { () =>
            if !over then {
              over = true
              if failure != null then next.fail(failure)
              else {
                next.end()
                // It left before the flow it was given had ended: nothing more is wanted of that.
                if !upstreamEnded then drive.stop()
              }
            }
          }
      catch {
        case _: java.util.concurrent.CancellationException => ()
        case Gone                                          => ()
      } finally {
        idle = true
        wake(watcher)
      }
    }

    // ---- the pipeline's side

    private def give(what: AnyRef): Unit = {
      if cancel == null then cancel = drive.start(run())
      inbox.add(what): Unit
      if pending.incrementAndGet() > Most && !holding && !drive.waits then {
        holding = true
        drive.hold()
      }
      wake(reader)
      if drive.waits then {
        drive.pause { resume =>
          watcher.set(resume)
          if idle && inbox.isEmpty() then wake(watcher)
        }
        val thrown = below
        if thrown != null then {
          over = true
          throw thrown
        }
      }
    }

    // The pipeline's thread's: the body has been handed the end, or a failure, and is handed nothing after it.
    private var closed = false

    def push(element: A): Unit = if !over && !closed then give(element.asInstanceOf[AnyRef])

    def end(): Unit =
      if !over && !closed then {
        closed = true
        upstreamEnded = true
        give(End)
      }

    def fail(thrown: Throwable): Unit =
      if below != null then {
        // A step after this one threw it: it is theirs, and the body is done with.
        over = true
        val stop = cancel
        if stop != null then stop.run()
        next.fail(thrown)
      } else if !over && !closed then {
        // Not `upstreamEnded`: if the body makes an end of it, what failed is told that no more is wanted.
        closed = true
        give(new Failed(thrown))
      }

    def abandon(): Unit = {
      over = true
      val stop = cancel
      if stop != null then stop.run()
      next.abandon()
    }
  }

  /** A queue from one future to another, one that fills it and one that empties it: a ring with a place for each to
    * say how far it has got, and nothing made for an element.
    */
  private final class Mailbox {
    // Room for what the writer may put before it waits, and the end.
    private inline val Size = 128
    private val slots = new java.util.concurrent.atomic.AtomicReferenceArray[AnyRef | Null](Size)
    private val put = new java.util.concurrent.atomic.AtomicLong(0)
    private val taken = new java.util.concurrent.atomic.AtomicLong(0)
    private val reader = new java.util.concurrent.atomic.AtomicReference[Runnable | Null](null)
    private val writer = new java.util.concurrent.atomic.AtomicReference[Runnable | Null](null)

    private def wake(parked: java.util.concurrent.atomic.AtomicReference[Runnable | Null]): Unit =
      if parked.get() != null then {
        val resume = parked.getAndSet(null)
        if resume != null then resume.run()
      }

    /** Adds `element`, and waits first if the reader is `Lag` behind. The writer's alone. */
    def put(element: AnyRef)(using async: Async): Unit = {
      val at = put.get()
      while at - taken.get() >= Lag do
        async.park { resume =>
          writer.set(resume)
          // Taken meanwhile: nobody was there to wake.
          if at - taken.get() < Lag then wake(writer)
        }
      slots.set((at & (Size - 1)).toInt, element)
      put.set(at + 1)
      wake(reader)
    }

    /** The next element, waited for. The reader's alone. */
    def take()(using async: Async): AnyRef = {
      val at = taken.get()
      while put.get() == at do
        async.park { resume =>
          reader.set(resume)
          if put.get() != at then wake(reader)
        }
      val index = (at & (Size - 1)).toInt
      val element = slots.get(index).nn
      slots.set(index, null)
      taken.set(at + 1)
      wake(writer)
      element
    }
  }

}
