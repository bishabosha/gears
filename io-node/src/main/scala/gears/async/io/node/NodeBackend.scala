package gears.async.io
package node

import gears.async.Async
import gears.async.js.UnsafeJsAsyncFromSync

import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.scalajs.js

/** The Node.js backend: the interface on Node's own I/O, for a Scala.js program linked to WebAssembly and run by
  * Node.js, which suspends with JSPI.
  *
  * A program runs on Node's one thread, its event loop, as gears' futures do on Scala.js. An op is performed by the
  * future performing it: begun at once, and if Node has not what completes it, left with Node as a listener or a
  * callback while the future is suspended, which the event loop resumes once it has. Nothing blocks, so that every
  * future, timer and socket goes on while one waits; and since there is one thread, what a program does between two
  * suspensions no other future sees half done.
  *
  * A run suspends its caller, so `Io.run` over this backend must be called where JSPI can suspend: within Scala.js's
  * `js.async`, as a gears program is on Scala.js. `start` does both, for a program started from JavaScript's side, a
  * main method or a callback.
  */
object NodeBackend extends Backend {
  type Interface = NodeOps.type

  def pool(bufferSize: Int, buffers: Int): BufferPool = new NodePool(bufferSize, buffers)

  private val io: Io[Interface] = new Io[Interface] {
    val backend: NodeBackend.type = NodeBackend
    val ops: NodeOps.type = NodeOps
    private[io] def performWithin(op: NodeOp, timeoutMillis: Long)(using Async): Unit = op.perform(timeoutMillis)
  }

  /** Runs `program` on the event loop, where its caller is, which is suspended until it returns. */
  private[io] def run[T](program: Io[Interface] ?=> Async.Spawn ?=> T): T =
    Async.fromSync(using UnsafeJsAsyncFromSync)(program(using io))

  /** Starts `program` as `Io.run` runs it, from where nothing can be suspended, and returns at once: the promise is
    * resolved with what the program returns, or rejected with its failure.
    */
  def start[T](program: Io[Interface] ?=> Async.Spawn ?=> T): js.Promise[T] = js.async(run(program))
}

/** The Node.js backend's ops, each checked as it is built. */
object NodeOps extends Pipes {
  type Op = NodeOp
  type Handle = NodeHandle
  type Readable = ReadEnd
  type Writable = WriteEnd

  private def fillable(buf: ByteBuffer): ByteBuffer = {
    if buf.isReadOnly() then throw new IllegalArgumentException(s"A read-only buffer cannot be read into: $buf")
    buf
  }

  private def several(bufs: Array[ByteBuffer]): Array[ByteBuffer] = {
    if bufs.isEmpty then throw new IllegalArgumentException("A vectored op takes at least one buffer")
    bufs
  }

  // ---- Every backend's

  def timer(duration: FiniteDuration): NodeOp & Repeatable = {
    if duration.length < 0 then throw new IllegalArgumentException(s"A timer's time is not negative: $duration")
    new TimerOp(duration.toNanos)
  }

  def deadlineTimer(nanoTime: Long): NodeOp & Repeatable & Deadline = new DeadlineOp(nanoTime)

  // ---- Streams

  def read(handle: ReadEnd, buf: ByteBuffer): NodeOp & Repeatable = new ReadOp(handle, Array(fillable(buf)))

  def write(handle: WriteEnd, buf: ByteBuffer): NodeOp & Repeatable = new WriteOp(handle, Array(buf))

  def read(handle: ReadEnd, bufs: Array[ByteBuffer]): NodeOp & Repeatable = {
    several(bufs).foreach(fillable)
    new ReadOp(handle, bufs)
  }

  def write(handle: WriteEnd, bufs: Array[ByteBuffer]): NodeOp & Repeatable = new WriteOp(handle, several(bufs))

  def read(handle: ReadEnd, pool: BufferPool, into: Slot[Received]): NodeOp & Repeatable = pool match {
    case ours: NodePool => new PooledReadOp(handle, ours, into)
    case other          => throw new IllegalArgumentException(s"Not a pool of this backend's: $other")
  }

  def close(handle: NodeHandle): NodeOp = new CloseOp(handle)

  // ---- Pipes

  def pipe(read: Slot[ReadEnd], write: Slot[WriteEnd]): NodeOp = new PipeOp(read, write)
}
