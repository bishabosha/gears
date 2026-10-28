package gears.async.io
package internal

import java.nio.ByteBuffer
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Pointer-backed byte buffers over zone memory. `ByteBuffer.allocateDirect` on Scala Native is heap-backed, so
  * `PointerBuffer.wrap` is the supported way to get a buffer the kernel can address. The memory lives as long as the
  * zone it was allocated in.
  */
object NativeBuffer {

  def allocate(capacity: Int)(using Zone): ByteBuffer = {
    require(capacity > 0, "Buffer capacity must be positive")
    PointerBuffer.wrap(alloc[Byte](capacity), capacity)
  }

  /** A buffer holding `bytes`, flipped and ready to drain. Empty input still gets one byte of storage. */
  def of(bytes: Array[Byte])(using Zone): ByteBuffer = {
    val buf = allocate(math.max(bytes.length, 1))
    buf.put(bytes)
    buf.flip()
    buf
  }

  /** Checks, as an op is built, that the kernel can address `buf`: a buffer with native memory behind it, writable, as
    * `allocate` and `PointerBuffer.wrap` make, and not one on the heap, as `ByteBuffer.allocate` and, on Scala Native,
    * `ByteBuffer.allocateDirect` make, nor a read-only one; else an `IllegalArgumentException`, where the op would fail
    * later, in its system call, with an `UnsupportedOperationException`.
    */
  def check(buf: ByteBuffer): Unit =
    if !buf.hasPointer() then
      throw new IllegalArgumentException(
        "This backend needs a buffer with native memory behind it, from BufferArena or PointerBuffer.wrap, not one on " +
          s"the heap (ByteBuffer.allocate, or allocateDirect, which is on the heap on Scala Native) nor a read-only one: $buf"
      )

  /** `check` on each of `bufs`. */
  def check(bufs: Array[ByteBuffer]): Unit = bufs.foreach(check)

  /** The address of the buffer's first byte. */
  def pointer(buf: ByteBuffer): Ptr[Byte] =
    buf.pointer()

  /** The address of the buffer's current position. */
  def atPosition(buf: ByteBuffer): Ptr[Byte] = buf.pointer() + buf.position()

  /** Moves the position forward after the kernel filled or drained `n` bytes at it. */
  def advance(buf: ByteBuffer, n: Int): Unit = buf.position(buf.position() + n)
}
