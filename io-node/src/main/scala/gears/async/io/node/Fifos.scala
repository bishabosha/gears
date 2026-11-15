package gears.async.io
package node

import java.io.IOException
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.Uint8Array

/** A FIFO's read end, read by its descriptor with `fs.read`, which on a descriptor that does not block returns at once
  * with what the system has: bytes; nothing, the end, when no writer has the FIFO open; or that it would block, when
  * one has and nothing is written. So a read finds the end where the interface says it is, before the first writer and
  * after each, and goes on to the next writer's bytes.
  *
  * What Node does not give is word of when a FIFO that would block can be read: its streams, which wait for that, never
  * report a FIFO's end when its writer closes (probed on macOS, Node 26). A read that would block therefore looks
  * again, after a millisecond at first and less often as it goes on waiting, up to `SlowestLook`: the one place this
  * backend polls.
  */
final class FifoReadEnd private[node] (fd: Int) extends NodeHandle("FIFO's read end") with SourceEnd {
  private val looking = new FifoReadEnd.Looking(fd)
  private[node] def source: Source = looking

  protected def shut(done: () => Unit): Unit = looking.close { () =>
    val closed: js.Function1[js.Dynamic, Unit] = _ => done()
    Fs.close(fd, closed)
  }
}

private[node] object FifoReadEnd {

  /** The longest a waiting read goes between looks, in milliseconds. */
  inline val SlowestLook = 16

  /** The most one look reads. */
  inline val Chunk = 65536

  def flags(write: Boolean): Int = {
    val constants = Fs.constants
    (if write then constants.O_WRONLY else constants.O_RDONLY).asInstanceOf[Int] | constants.O_NONBLOCK
      .asInstanceOf[Int]
  }

  /** The FIFO as a source: what a look found, kept until a read takes it, and the looks made while a read waits. A look
    * is a call of Node's that runs to its end, so what it finds after its read was cancelled stays for the next, no
    * byte lost, and a close waits for it.
    */
  final class Looking(fd: Int) extends Source {
    private var found: Int8Array | Null = null
    private var atEnd = false
    private var failed: IOException | Null = null
    private var inFlight = false
    private var wanted = false
    private var pause = 1
    private var alarm: Clock.Alarm | Null = null
    private var closing: (() => Unit) | Null = null
    private var closed = false
    private var spare: Uint8Array | Null = null // a look's buffer that it found nothing for, for the next look

    def available: Int = if found == null then 0 else found.nn.length

    def take(max: Int): Int8Array = {
      val all = found.nn
      if all.length <= max then {
        found = null
        all
      } else {
        found = all.subarray(max)
        all.subarray(0, max)
      }
    }

    def ended: Boolean = atEnd
    def failure: IOException | Null = failed
    override def endTaken(): Unit = atEnd = false

    override def onChange(wake: (() => Unit) | Null): Unit = {
      super.onChange(wake)
      wanted = wake != null
      if wanted then { if !inFlight && alarm == null then look() }
      else {
        val waiting = alarm
        if waiting != null then waiting.cancel()
        alarm = null
        pause = 1
      }
    }

    private def look(): Unit = {
      alarm = null
      inFlight = true
      val chunk = if spare != null then spare.nn else new Uint8Array(Chunk)
      spare = null
      val done: js.Function2[js.Dynamic, js.Dynamic, Unit] = (failure, read) => {
        inFlight = false
        val close = closing
        if close != null then close()
        else if Errors.failed(failure) then
          if failure.code.asInstanceOf[Any] == "EAGAIN" then {
            spare = chunk
            if wanted then {
              alarm = Clock.at(System.nanoTime() + pause * 1_000_000L)(() => if wanted && !closed then look())
              pause = math.min(pause * 2, SlowestLook)
            }
          } else {
            failed = Errors.io(failure)
            wakeReader()
          }
        else {
          val count = read.asInstanceOf[Int]
          pause = 1
          if count > 0 then found = Buffers.signed(chunk).subarray(0, count)
          else {
            spare = chunk
            atEnd = wanted // an end no read waits for is not kept: the next read looks for itself
          }
          wakeReader()
        }
      }
      try Fs.read(fd, chunk, 0, Chunk, null, done)
      catch
        case js.JavaScriptException(failure) =>
          inFlight = false
          failed = Errors.io(failure)
          wakeReader()
    }

    /** Stops looking, and calls `done` once no look is under way. */
    def close(done: () => Unit): Unit = {
      closed = true
      val waiting = alarm
      if waiting != null then waiting.cancel()
      alarm = null
      if inFlight then closing = done else done()
    }
  }
}

/** A FIFO's write end: a stream of Node's over its descriptor. */
final class FifoWriteEnd private[node] (fd: Int) extends NodeHandle("FIFO's write end") with SinkEnd {
  private val stream = {
    val socket = new Net.Socket(js.Dynamic.literal(fd = fd, readable = false, writable = true))
    new SocketStream(socket.asInstanceOf[js.Dynamic], readable = false, writable = true)
  }
  private[node] def sink: Sink = stream

  protected def shut(done: () => Unit): Unit = {
    stream.close()
    done()
  }
}

/** Opens a FIFO for reading or for writing, without waiting for its other end, refusing anything that is not a FIFO
  * once the system has said what it is. Opened for writing, it fails while the FIFO has no reader, as the system fails
  * it.
  */
private[node] final class OpenFifoOp(
    path: String,
    write: Boolean,
    into: Slot[? >: FifoReadEnd & FifoWriteEnd <: NodeHandle]
) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(into)

  protected def start(run: Run): Unit = {
    run.hold()

    /** Closes the descriptor of what is not to be handed over, then fails the op. */
    def abandon(fd: Int, failure: Throwable): Unit = {
      val closed: js.Function1[js.Dynamic, Unit] = _ => run.fail(failure)
      Fs.close(fd, closed)
    }
    val opened: js.Function2[js.Dynamic, js.Dynamic, Unit] = (failure, descriptor) =>
      if Errors.failed(failure) then { if !run.failIfStopped() then run.fail(Errors.io(failure)) }
      else {
        val fd = descriptor.asInstanceOf[Int]
        val stat: js.Function2[js.Dynamic, js.Dynamic, Unit] = (failure, stats) => {
          val stopped = run.stopped
          if stopped != null then abandon(fd, stopped)
          else if Errors.failed(failure) then abandon(fd, Errors.io(failure))
          else if !stats.isFIFO().asInstanceOf[Boolean] then abandon(fd, new IOException(s"$path is not a FIFO"))
          else
            try {
              into.asInstanceOf[Slot[NodeHandle]].set(if write then new FifoWriteEnd(fd) else new FifoReadEnd(fd))
              run.succeed()
            } catch case failure: Throwable => abandon(fd, failure)
        }
        Fs.fstat(fd, stat)
      }
    Errors.guard(Fs.open(path, FifoReadEnd.flags(write), opened))
  }
}
