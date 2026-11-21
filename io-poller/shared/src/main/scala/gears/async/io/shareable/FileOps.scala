package gears.async.io
package shareable

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException

/** What a file's handle keeps for the reads and writes of it as a stream (`ByteStreams.read` and `write` on a file):
  * the position they are at, which each moves on by what it read or wrote. Its positional ops leave it alone.
  */
trait FilePosition extends BaseHandle {
  @volatile var streamPosition: Long = 0L

  /** How many of the file's next reads and writes are done off the workers without being tried on the performer's
    * thread first: set when one that was tried there turned out slow (see `AbstractFileOp`). Counted down by whoever
    * performs them; a count that two performers both take one from loses nothing that matters.
    */
  @volatile var offThreadNext: Int = 0
}

/** An op on an open file, which the operating system does by blocking for the disk, and so is done off the workers
  * (`WorkOffThread`). Its call is not interrupted: a cancel or its time limit lets a call under way finish, its outcome
  * dropped, and returns once it has, as the call has the caller's buffer. The call is made with the file's work
  * claimed, so a closed file refuses it and a close waits for it; a file closed meanwhile fails the op, whatever the
  * call did (see `RegularFiles`).
  */
abstract class AbstractFileOp[F <: BaseHandle](file: F) extends RepeatableAbstractTimerHeapOp, WorkOffThread {

  /** The op's system call, which may block, the file's work claimed. */
  protected def call(): Unit

  /** The file whose reads and writes this is one of, which are tried where they are performed; null for an op that
    * is always done off the workers, a sync say.
    */
  protected def paced: FilePosition | Null = null

  /** A read or a write of a file whose pages the system has is done in the time of a copy, and handing it to another
    * thread and back costs twenty times that. So it is made on the performer's own thread, and timed: one that took
    * long, the disk having been waited for, has the file's next `OffThreadAfterSlow` done off the workers, after
    * which one is tried again. A worker is so kept by a file's disk once in a while, not for every call.
    */
  override protected final def workWithoutBlocking(): Boolean = {
    val pace = paced
    if pace == null then false
    else {
      val left = pace.offThreadNext
      if left > 0 then {
        pace.offThreadNext = left - 1
        false
      } else {
        val began = System.nanoTime()
        work()
        if System.nanoTime() - began > AbstractFileOp.SlowNanos then pace.offThreadNext = AbstractFileOp.OffThreadAfterSlow
        true
      }
    }
  }

  protected final def interruptible: Boolean = false
  protected final def waits: Boolean = true
  override protected final def worksOn: BaseHandle = file

  protected final def work(): Unit =
    file.claimWork()
    try
      call()
      if file.getState != HandleState.OpenState then throw new ClosedChannelException()
    finally file.releaseWork()
}

object AbstractFileOp {

  /** Longer than a copy of what a read holds, and shorter than a disk takes. */
  inline val SlowNanos = 100_000L

  inline val OffThreadAfterSlow = 64
}

/** Fills the first of `bufs` that has room from a file: at `start`, going on from where it left off when performed
  * again, or, if `start` is negative, at the file's stream position. Completes having read at least one byte, or none
  * at the end of the file. The buffer's position moves only once the op is claimed for what was read.
  */
abstract class AbstractFileReadOp[F <: FilePosition](file: F, bufs: Array[ByteBuffer], start: Long)
    extends AbstractFileOp[F](file) {
  Checks.checkBuffers(bufs)
  Checks.checkFillable(bufs)

  /** Reads into `buf`, moving its position, from `file` at `offset`: the bytes read, 0 at the end. May block. */
  def _readAt(file: F, buf: ByteBuffer, offset: Long): Int

  private var offset = start
  private var filling: ByteBuffer | Null = null
  private var read = 0

  override def onEntry(): Unit =
    super.onEntry()
    if !Checks.anyRemaining(bufs) then
      throw (if bufs.length == 1 then Checks.bufferFull(bufs(0)) else Checks.buffersFull(bufs))

  override protected final def paced: FilePosition = file

  protected final def call(): Unit =
    val buf = bufs.find(_.hasRemaining()).get
    filling = buf
    // Through a view of its own: the caller's buffer is as it was until `commit`.
    read = _readAt(file, buf.duplicate(), if start < 0 then file.streamPosition else offset)

  protected final def commit(): Unit =
    val buf = filling.nn
    buf.position(buf.position() + read)
    if start < 0 then file.streamPosition += read else offset += read
}

/** Drains the first of `bufs` that has bytes remaining into a file, at `start` or the file's stream position, as
  * `AbstractFileReadOp` fills one: at least one byte, or nothing if none remains.
  */
abstract class AbstractFileWriteOp[F <: FilePosition](file: F, bufs: Array[ByteBuffer], start: Long)
    extends AbstractFileOp[F](file) {
  Checks.checkBuffers(bufs)

  /** Writes from `buf`, moving its position, to `file` at `offset`: the bytes written. May block. */
  def _writeAt(file: F, buf: ByteBuffer, offset: Long): Int

  private var offset = start
  private var draining: ByteBuffer | Null = null
  private var written = 0

  override protected final def paced: FilePosition = file

  protected final def call(): Unit =
    draining = bufs.find(_.hasRemaining()).orNull
    written = 0
    val buf = draining
    if buf != null then written = _writeAt(file, buf.duplicate(), if start < 0 then file.streamPosition else offset)

  protected final def commit(): Unit =
    val buf = draining
    if buf != null then
      buf.position(buf.position() + written)
      if start < 0 then file.streamPosition += written else offset += written
}

/** A read of a file as a stream into a buffer of `pool`'s, handed over through `into` as a `Received`. */
abstract class AbstractFilePooledReadOp[F <: FilePosition](file: F, pool: LendingPool, into: Slot[Received])
    extends AbstractFileOp[F](file) {

  /** As `AbstractFileReadOp._readAt`. */
  def _readAt(file: F, buf: ByteBuffer, offset: Long): Int

  private var filled: ByteBuffer | Null = null
  private var read = 0

  override def onEntry(): Unit =
    super.onEntry()
    Checks.checkEmpty(into)

  override protected final def paced: FilePosition = file

  protected final def call(): Unit =
    val buf = pool.take()
    try read = _readAt(file, buf, file.streamPosition)
    catch
      case failure: Throwable =>
        pool.give(buf)
        throw failure
    filled = buf

  protected final def commit(): Unit =
    val buf = filled.nn
    filled = null
    buf.flip() // at the end of the file nothing was read, and the limit is 0
    into.set(pool.received(buf))
    file.streamPosition += read

  override protected def discard(): Unit =
    val buf = filled
    filled = null
    if buf != null then pool.give(buf)
}

/** Completes once a file's written bytes have been handed to the device (`RegularFiles.sync`). */
abstract class AbstractFileSyncOp[F <: BaseHandle](file: F) extends AbstractFileOp[F](file) {

  /** Hands `file`'s written bytes to the device. May block. */
  def _sync(file: F): Unit

  protected final def call(): Unit = _sync(file)
  protected final def commit(): Unit = ()
}

/** Opens a regular file, which blocks for the disk, and writes its handle into `into` once the op is claimed for it: an
  * op cancelled or timed out meanwhile closes what was opened, which never reached the caller. Performed once.
  */
abstract class AbstractOpenFileOp[F <: BaseHandle](path: String, write: Boolean, into: Slot[F])
    extends SingleUseAbstractTimerHeapOp,
      WorkOffThread {
  Checks.checkPath(path)

  /** Makes a handle of the regular file at `path`, opened for writing if `write`, as `owner`'s, or throws why it could
    * not be. May block. The op opens what this returns.
    */
  def _openFile(path: String, write: Boolean, owner: HandleOwner): F

  private var opened: F | Null = null

  override def onEntry(): Unit =
    super.onEntry()
    Checks.checkEmpty(into)

  protected final def interruptible: Boolean = false
  protected final def waits: Boolean = true
  protected final def work(): Unit =
    val made = _openFile(path, write, owner)
    made.open()
    opened = made
  protected final def commit(): Unit = into.set(opened.nn)
  override protected def discard(): Unit = opened.nn.requestClose(): Unit
}
