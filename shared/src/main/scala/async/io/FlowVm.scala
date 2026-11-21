package gears.async.io

import scala.annotation.switch

/** EXPERIMENT, to compare with `Flow`'s stages.
  *
  * A pipeline as a program: its stages are instructions in an array, made once and shared by everything that is
  * stepped through it, and what a stage keeps between elements is in registers, an array for each connection. An
  * element passes through a run of one-to-one instructions in a loop, with no call but the stage's own function; an
  * instruction that makes many (`process`, `mapConcat`) or deals by key enters the loop again at the instruction
  * after it.
  *
  * Against a `Flow` of stages, which is made for each connection and compiled to a chain of steps, each a closure of
  * the next: there a connection costs a flow, a step and a closure for every stage, and here an array. What a
  * connection's stages share is not captured by closures, there being one program, but made for each connection
  * (`Pipe.local`) and handed to the instructions that ask for it (`mapWith`, `processWith`).
  */
final class Pipe[A, B] private[io] (
    private[io] val code: Vector[Pipe.Instr],
    private[io] val local: (() => AnyRef) | Null
) {
  import Pipe.*
  private def and[C](op: Int, arg: AnyRef): Pipe[A, C] = new Pipe[A, C](code :+ new Instr(op, arg), local)

  def map[C](f: B => C): Pipe[A, C] = and(Map, f)
  def filter(keep: B => Boolean): Pipe[A, B] = and(Filter, keep)
  def mapConcat[C](f: B => IterableOnce[C]): Pipe[A, C] = and(MapConcat, f)

  /** What `f` has made of the elements so far, after each. */
  def scan[C](zero: C)(f: (C, B) => C): Pipe[A, C] = and(Scan, new Scanning(zero.asInstanceOf[AnyRef], f.asInstanceOf[(Any, Any) => Any]))

  /** As `Flow.process`: `handler` is called once for a connection, and gives what to call with each element. */
  def process[C](handler: (C => Unit) => (B => Unit)): Pipe[A, C] =
    and(Process, ((_: Any, emit: Any => Unit) => handler(emit)).asInstanceOf[AnyRef])

  /** `process`, given what the pipeline made for its connection (`Pipe.local`). */
  def processWith[S, C](handler: (S, C => Unit) => (B => Unit)): Pipe[A, C] = and(Process, handler)

  /** `map` by a function made once for a connection, or for a key of one, of what the pipeline made for its
    * connection: one that has something of its own, a buffer say.
    */
  def mapWith[S, C](make: S => B => C): Pipe[A, C] = and(MapWith, make)

  /** As `Flow.groupThrough`, but the keys share one program: `sub` is called once, here. */
  def groupThrough[K, C](key: B => K)(sub: Pipe[B, B] => Pipe[B, C]): Pipe[A, C] =
    and(Group, new Grouping(key.asInstanceOf[Any => Any], sub(new Pipe[B, B](Vector.empty, null)).program))

  /** The program: made once, started for each connection. */
  def program: Program[A, B] = {
    val ops = new Array[Int](code.length)
    val args = new Array[AnyRef](code.length)
    val slots = new Array[Int](code.length)
    var registers = 0
    for (instr, at) <- code.zipWithIndex do {
      ops(at) = instr.op
      args(at) = instr.arg
      if instr.op >= Scan then {
        slots(at) = registers
        registers += 1
      }
    }
    new Program[A, B](ops, args, slots, registers, local)
  }
}

object Pipe {

  /** A pipeline of nothing yet. */
  def from[A]: Pipe[A, A] = new Pipe[A, A](Vector.empty, null)

  /** A pipeline of nothing yet, with something made for each connection it is started for. */
  def local[S <: AnyRef, A](make: () => S): Pipe[A, A] = new Pipe[A, A](Vector.empty, make)

  private[io] final class Instr(val op: Int, val arg: AnyRef)
  private[io] final class Scanning(val zero: AnyRef, val f: (Any, Any) => Any)
  private[io] final class Grouping(val key: Any => Any, val sub: Program[?, ?])

  // Those from `Scan` on have a register.
  private[io] inline val Map = 0
  private[io] inline val Filter = 1
  private[io] inline val MapConcat = 2
  private[io] inline val Scan = 3
  private[io] inline val Process = 4
  private[io] inline val MapWith = 5
  private[io] inline val Group = 6

  final class Program[A, B] private[io] (
      private[io] val ops: Array[Int],
      private[io] val args: Array[AnyRef],
      private[io] val slots: Array[Int],
      private[io] val registers: Int,
      private[io] val local: (() => AnyRef) | Null
  ) {

    /** A step for one connection's elements, what comes out pushed to `out`. */
    def start(out: Flow.Step[B]): Flow.Step[A] = {
      val made = if local == null then null else local.nn()
      new Running(this, out.asInstanceOf[Flow.Step[Any]], made).asInstanceOf[Flow.Step[A]]
    }
  }

  /** A group instruction's register: its keys' pipelines, and where what they make goes on from. */
  private final class Keys(val running: java.util.HashMap[Any, Running], val after: Flow.Step[Any])

  private final class Running(program: Program[?, ?], out: Flow.Step[Any], local: AnyRef | Null) extends Flow.Step[Any] {
    private val ops = program.ops
    private val args = program.args
    private val slots = program.slots
    private val registers = new Array[AnyRef | Null](program.registers)

    locally {
      var pc = 0
      while pc < ops.length do {
        val after = pc + 1
        (ops(pc): @switch) match {
          case Scan    => registers(slots(pc)) = args(pc).asInstanceOf[Scanning].zero
          case Process =>
            registers(slots(pc)) = args(pc).asInstanceOf[(Any, Any => Unit) => AnyRef](local, element => run(after, element))
          case MapWith => registers(slots(pc)) = args(pc).asInstanceOf[Any => AnyRef](local)
          case Group   =>
            val on = new Flow.Step[Any] {
              def push(element: Any): Unit = run(after, element)
              def end(): Unit = ()
              def abandon(): Unit = ()
            }
            registers(slots(pc)) = new Keys(new java.util.HashMap[Any, Running](), on)
          case _ => ()
        }
        pc += 1
      }
    }

    def push(element: Any): Unit = run(0, element)

    private def run(from: Int, first: Any): Unit = {
      var pc = from
      var element = first
      val length = ops.length
      while pc < length do {
        (ops(pc): @switch) match {
          case Map =>
            element = args(pc).asInstanceOf[Any => Any](element)
            pc += 1
          case Filter =>
            if args(pc).asInstanceOf[Any => Boolean](element) then pc += 1 else return
          case MapWith =>
            element = registers(slots(pc)).asInstanceOf[Any => Any](element)
            pc += 1
          case Scan =>
            element = args(pc).asInstanceOf[Scanning].f(registers(slots(pc)), element)
            registers(slots(pc)) = element.asInstanceOf[AnyRef]
            pc += 1
          case Process =>
            registers(slots(pc)).asInstanceOf[Any => Unit](element)
            return
          case MapConcat =>
            val each = args(pc).asInstanceOf[Any => IterableOnce[Any]](element).iterator
            while each.hasNext do run(pc + 1, each.next())
            return
          case Group =>
            val grouping = args(pc).asInstanceOf[Grouping]
            val keys = registers(slots(pc)).asInstanceOf[Keys]
            val key = grouping.key(element)
            var running = keys.running.get(key)
            if running == null then {
              running = new Running(grouping.sub, keys.after, local)
              keys.running.put(key, running): Unit
            }
            running.push(element)
            return
        }
      }
      out.push(element)
    }

    private def groups(each: Running => Unit): Unit = {
      var pc = 0
      while pc < ops.length do {
        if ops(pc) == Group then registers(slots(pc)).asInstanceOf[Keys].running.values().forEach(running => each(running))
        pc += 1
      }
    }

    def end(): Unit = {
      groups(_.end())
      out.end()
    }

    def abandon(): Unit = {
      groups(_.abandon())
      out.abandon()
    }
  }
}
