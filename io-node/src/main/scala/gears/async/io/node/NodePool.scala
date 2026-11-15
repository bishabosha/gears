package gears.async.io
package node

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.TypedArrayBuffer

/** The Node.js backend's pool. Node's streams hand the backend chunks of their own, so a pooled read hands over such a
  * chunk, of at most `bufferSize` bytes, rather than copy it into a buffer allocated here: the pool allocates nothing,
  * and counts the chunks held, at most `buffers`, which is what makes it a window on the stream.
  */
private[node] final class NodePool(val bufferSize: Int, val buffers: Int) extends BufferPool {
  require(bufferSize > 0 && buffers > 0, s"A pool has buffers of some size: $buffers of $bufferSize bytes")
  private var held = 0
  private var closed = false

  def available: Int = buffers - held

  /** Takes one of the pool's buffers for a read that has bytes to hand over; fails if there is none, or the pool is
    * closed.
    */
  private[node] def take(): Unit = {
    if closed then throw new IOException("The pool is closed")
    if held == buffers then throw new IOException(s"The pool has no buffer free: all $buffers are held")
    held += 1
  }

  /** Gives back a buffer taken with `take` that no `Received` was made of. */
  private[node] def give(): Unit = held -= 1

  /** What a read received as the buffer taken for it: `chunk`, or nothing at the end of the stream. */
  private[node] def received(chunk: Int8Array | Null): Received = new Received {
    private var released = false
    val buffer: ByteBuffer = if chunk == null then ByteBuffer.allocate(0) else TypedArrayBuffer.wrap(chunk)
    def release(): Unit = {
      if released then throw new IllegalStateException("Released already")
      released = true
      give()
    }
  }

  def close(): Unit =
    if !closed then {
      if held > 0 then throw new IllegalArgumentException(s"$held of the pool's buffers are still held")
      closed = true
    }
}
