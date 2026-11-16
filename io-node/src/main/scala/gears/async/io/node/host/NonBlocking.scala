package gears.async.io.node.host

import java.nio.ByteBuffer
import scala.scalajs.js.typedarray.Int8Array

/** Where a stream's bytes come from, as Node has them, read without waiting: a read says at once what there is, and
  * would-block and the end of the stream are values, not failures. Whoever owns the source, its handle, is told when a
  * read that would have blocked may no longer (`onReadable`), once for the handle, not for each op that waits.
  */
trait ByteSource {

  /** At most `max` bytes that have arrived, as a chunk of Node's own; `ByteSource.Ended` at the end of the stream; or
    * null if a read would block. Fails with an `IOException` if the stream has failed.
    */
  def read(max: Int): Int8Array | Null

  /** Called when bytes, the end or a failure may have come: set by the source's handle. */
  var onReadable: (() => Unit) | Null = null

  /** An op begins to wait for the source, or stops: for a source that must do something to learn what has come. */
  def listening(on: Boolean): Unit = ()

  /** Closes the source: true if it is closed as this returns, else `done` is called once it is. */
  def close(done: () => Unit): Boolean
}

object ByteSource {

  /** What `read` gives at the end of the stream. */
  val Ended: Int8Array = new Int8Array(0)
}

/** Where a stream's bytes go, as Node takes them, written without waiting. */
trait ByteSink {

  /** Takes bytes from `bufs`, which have some, in order: how many, at least one, or `ByteSink.Blocked` if a write would
    * block, when none was taken. Fails with an `IOException` if the stream takes no more.
    */
  def write(bufs: Array[ByteBuffer]): Int

  /** Called when a write that would have blocked may no longer, or writes have begun to fail. */
  var onWritable: (() => Unit) | Null = null

  /** Closes the sink: true if it is closed as this returns, else `done` is called once it is. */
  def close(done: () => Unit): Boolean
}

object ByteSink {
  inline val Blocked = -1
}
