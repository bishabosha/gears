package gears.async.io
package node

import java.io.IOException
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array

/** A pipe of the Node.js backend. Node has no call that makes a pipe of the system's, only those it makes for a child
  * process, so a pipe here is the bytes themselves, held between its two ends within the program, which is all a pipe
  * opened by `Pipes.pipe` can be used for: at most `Capacity` of them, as a pipe of the system's holds 64 KiB, so that
  * a writer waits for its reader.
  */
private[node] final class Pipe extends Source with Sink {
  private val chunks = js.Array[Int8Array]()
  private var held = 0
  private var writerGone = false
  private var readerGone = false

  def available: Int = held

  def take(max: Int): Int8Array = {
    val first = chunks(0)
    val taken =
      if first.length <= max then {
        chunks.shift()
        first
      } else {
        chunks(0) = first.subarray(max)
        first.subarray(0, max)
      }
    held -= taken.length
    wakeWriter()
    taken
  }

  def ended: Boolean = writerGone && held == 0

  def failure: IOException | Null = null

  def room: Int = Pipe.Capacity - held

  def put(chunk: Int8Array): Unit = {
    chunks.push(chunk)
    held += chunk.length
    wakeReader()
  }

  def refusal: IOException | Null = if readerGone then new IOException("Broken pipe: its read end is closed") else null

  /** The read end has closed: what is held is dropped, and writes fail from here on. */
  def closeReader(): Unit = {
    readerGone = true
    chunks.length = 0
    held = 0
    wakeWriter()
  }

  /** The write end has closed: the stream ends after what is held. */
  def closeWriter(): Unit = {
    writerGone = true
    wakeReader()
  }
}

private[node] object Pipe {
  inline val Capacity = 65536
}

/** A pipe's read end. */
private[node] final class PipeReadEnd(pipe: Pipe) extends NodeHandle("pipe's read end") with SourceEnd {
  private[node] def source: Source = pipe
  protected def shut(done: () => Unit): Unit = {
    pipe.closeReader()
    done()
  }
}

/** A pipe's write end. */
private[node] final class PipeWriteEnd(pipe: Pipe) extends NodeHandle("pipe's write end") with SinkEnd {
  private[node] def sink: Sink = pipe
  protected def shut(done: () => Unit): Unit = {
    pipe.closeWriter()
    done()
  }
}

/** Opens a pipe. */
private[node] final class PipeOp(read: Slot[ReadEnd], write: Slot[WriteEnd]) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(read, write)

  protected def start(run: Run): Unit = {
    val pipe = new Pipe
    read.set(new PipeReadEnd(pipe))
    write.set(new PipeWriteEnd(pipe))
    run.succeed()
  }
}
