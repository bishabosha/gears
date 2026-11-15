package gears.async.io
package node

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.Uint8Array

/** A regular file, by its descriptor, read and written with Node's `fs` calls, which run on Node's own threads and call
  * back: as a stream at the position the handle carries, or at offsets.
  *
  * A call once made runs to its end, and Node cannot take it back, so its op is held until it calls back: stopped
  * meanwhile, by a cancel, its time limit or a close of the file, the op fails as what stopped it says, its outcome
  * dropped and its buffers untouched, which only a call that completes fills; and a close waits for it, so the
  * descriptor is never another file's under a call.
  */
final class FileHandle private[node] (fd: Int, path: String, writable: Boolean)
    extends NodeHandle(s"file $path")
    with ReadEnd
    with WriteEnd {
  private var position = 0L
  private var reading = false
  private var writing = false

  /** Reads at most `max` bytes at `offset` for `run`, calling `taken` with them, null at the end of the file. */
  private[node] def readAt(run: Run, max: Int, offset: Long, taken: (Int8Array | Null) => Unit): Unit = {
    if writable then throw new IOException(s"$this was opened for writing, not reading")
    val chunk = new Uint8Array(math.min(max, FileHandle.LargestCall))
    run.hold()
    // On a failure Node gives no count, so it is taken as a number only once there is none.
    val done: js.Function2[js.Dynamic, js.Dynamic, Unit] = (failure, read) =>
      if !run.failIfStopped() then
        if Errors.failed(failure) then run.fail(Errors.io(failure))
        else
          try {
            val count = read.asInstanceOf[Int]
            taken(if count == 0 then null else Buffers.signed(chunk).subarray(0, count))
            run.succeed()
          } catch case refused: Throwable => run.fail(refused)
    Errors.guard(Fs.read(fd, chunk, 0, chunk.length, offset.toDouble, done))
  }

  /** Writes what remains of `bufs`, in order, at `offset` for `run`, as much as one call takes, calling `written` with
    * how many bytes that was, once it has passed over them in `bufs`.
    */
  private[node] def writeAt(run: Run, bufs: Array[ByteBuffer], offset: Long, written: Int => Unit): Unit = {
    if !writable then throw new IOException(s"$this was opened for reading, not writing")
    // Copied without consuming: the buffers are passed over by what the call wrote, if it completes.
    val chunk = Buffers.drain(bufs.map(_.duplicate()), math.min(Buffers.room(bufs), FileHandle.LargestCall))
    run.hold()
    val done: js.Function2[js.Dynamic, js.Dynamic, Unit] = (failure, wrote) =>
      if !run.failIfStopped() then
        if Errors.failed(failure) then run.fail(Errors.io(failure))
        else {
          val count = wrote.asInstanceOf[Int]
          Buffers.skip(bufs, count)
          written(count)
          run.succeed()
        }
    Errors.guard(
      Fs.write(fd, new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.length), 0, chunk.length, offset.toDouble, done)
    )
  }

  private[node] def sync(run: Run): Unit = {
    run.hold()
    val done: js.Function1[js.Dynamic, Unit] = failure =>
      if !run.failIfStopped() then if Errors.failed(failure) then run.fail(Errors.io(failure)) else run.succeed()
    Errors.guard(Fs.fsync(fd, done))
  }

  // As a stream: at the handle's position, one read and one write at a time.

  private[node] def read(
      run: Run,
      op: NodeOp,
      max: Int,
      accept: () => Unit,
      deliver: (Int8Array | Null) => Unit
  ): Unit = {
    if reading then
      throw new IllegalArgumentException(s"An op reading, as $op does, is already pending on its handle, $this")
    readAt(
      run,
      max,
      position,
      chunk => {
        accept() // refused, the bytes stay the file's: the position is where it was
        deliver(chunk)
        if chunk != null then position += chunk.length
      }
    )
    reading = true
    run.release = () => reading = false
  }

  private[node] def write(run: Run, op: NodeOp, bufs: Array[ByteBuffer]): Unit = {
    if writing then
      throw new IllegalArgumentException(s"An op writing, as $op does, is already pending on its handle, $this")
    writeAt(run, bufs, position, count => position += count)
    writing = true
    run.release = () => writing = false
  }

  protected def shut(done: () => Unit): Unit = {
    val closed: js.Function1[js.Dynamic, Unit] = _ => done()
    Errors.guard(Fs.close(fd, closed))
  }
}

private[node] object FileHandle {

  /** The most one call reads or writes: a larger buffer takes several performances, as it may anywhere. */
  inline val LargestCall = 1 << 20
}

/** Opens a regular file, refusing anything else: without blocking even on a FIFO, which opened so is refused like the
  * rest once the system has said what it is.
  */
private[node] final class OpenFileOp(path: String, write: Boolean, into: Slot[FileHandle]) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(into)

  protected def start(run: Run): Unit = {
    val constants = Fs.constants
    val flags =
      (if write then constants.O_WRONLY else constants.O_RDONLY).asInstanceOf[Int] | constants.O_NONBLOCK
        .asInstanceOf[Int]
    run.hold()

    /** Closes the descriptor of a file that is not to be handed over, then fails the op. */
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
          else if !stats.isFile().asInstanceOf[Boolean] then
            abandon(fd, new IOException(s"$path is not a regular file"))
          else {
            into.set(new FileHandle(fd, path, write))
            run.succeed()
          }
        }
        Fs.fstat(fd, stat)
      }
    Errors.guard(Fs.open(path, flags, opened))
  }
}

/** A read at an offset, which goes on from where it left off when performed again. */
private[node] final class FileReadOp(on: FileHandle, buf: ByteBuffer, offset: Long) extends NodeOp with Repeatable {
  private var at = offset
  override protected def handle: NodeHandle = on

  override protected def check(): Unit =
    if !buf.hasRemaining() then
      throw new IllegalArgumentException(s"The buffer is full, so a read into it can read nothing: $buf")

  protected def start(run: Run): Unit =
    on.readAt(
      run,
      buf.remaining(),
      at,
      chunk =>
        if chunk != null then {
          Buffers.fill(Array(buf), chunk)
          at += chunk.length
        }
    )
}

/** A write at an offset, which goes on from where it left off when performed again. */
private[node] final class FileWriteOp(on: FileHandle, buf: ByteBuffer, offset: Long) extends NodeOp with Repeatable {
  private var at = offset
  override protected def handle: NodeHandle = on

  protected def start(run: Run): Unit =
    if !buf.hasRemaining() then run.succeed() else on.writeAt(run, Array(buf), at, count => at += count)
}

/** A sync of a file's written bytes to its device. */
private[node] final class SyncOp(on: FileHandle) extends NodeOp with Repeatable {
  override protected def handle: NodeHandle = on
  protected def start(run: Run): Unit = on.sync(run)
}
