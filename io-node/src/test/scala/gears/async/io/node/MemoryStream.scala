package gears.async.io
package node

import java.io.IOException
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array

/** A stream held in memory, for the suites: bytes between a read end and a write end within the program, at most
  * `Capacity` of them, so that a writer waits for its reader. The backend has no `Pipes`: Node makes no pipe of the
  * system's but for a child process, and one that is only memory is nothing a program needs from a backend. As a
  * fixture, though, it puts the backend's stream ops, the same that read and write its sockets, over a source and a
  * sink with no system behind them, where every wait and every wake-up is the backend's own.
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

/** The read end. */
private[node] final class PipeReadEnd(pipe: Pipe) extends NodeHandle("memory stream's read end") with SourceEnd {
  private[node] def source: Source = pipe
  protected def shut(done: () => Unit): Unit = {
    pipe.closeReader()
    done()
  }
}

/** The write end. */
private[node] final class PipeWriteEnd(pipe: Pipe) extends NodeHandle("memory stream's write end") with SinkEnd {
  private[node] def sink: Sink = pipe
  protected def shut(done: () => Unit): Unit = {
    pipe.closeWriter()
    done()
  }
}

/** Supplies the stream fixture from memory, for a suite over the Node.js backend itself. */
trait MemoryStreamFixture extends gears.async.io.testkit.StreamFixture {
  val backend: NodeBackend.type

  protected def stream(
      io: Io[backend.Interface]
  )(using gears.async.Async.Spawn): (io.ops.Readable, io.ops.Writable) = {
    val pipe = new Pipe
    (new PipeReadEnd(pipe), new PipeWriteEnd(pipe))
  }
}
