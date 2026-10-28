package gears.async.io
package internal

import java.nio.ByteBuffer
import scala.scalanative.libc.stdlib
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

  /** Runs `body` with a buffer the kernel can address standing for `buf`, and returns what it returns: `buf` itself if
    * it has native memory behind it, as `allocate` and `PointerBuffer.wrap` make; else, for one on the heap, as
    * `ByteBuffer.allocate` and, on Scala Native, `allocateDirect` make, or a read-only one, a stand-in over memory of
    * its own, holding `buf`'s remaining bytes if `copyIn`, so that an op takes any buffer, at a copy's cost for such a
    * one. What `body` moves the stand-in's position over is then `buf`'s: copied back into it if `copyOut`, its
    * position advanced as far.
    */
  inline def staged[A](buf: ByteBuffer, inline copyIn: Boolean, inline copyOut: Boolean)(
      inline body: ByteBuffer => A
  ): A =
    if buf.hasPointer() then body(buf)
    else {
      val memory = stagingMemory(buf.remaining())
      try {
        val stand = standIn(buf, memory, copyIn)
        val result = body(stand)
        settle(buf, stand, copyOut)
        result
      } finally stdlib.free(memory)
    }

  /** As the other `staged`, for buffers taken in order: each with native memory stands for itself, the others each by a
    * stand-in of its own.
    */
  inline def staged[A](bufs: Array[ByteBuffer], inline copyIn: Boolean, inline copyOut: Boolean)(
      inline body: Array[ByteBuffer] => A
  ): A =
    if bufs.forall(_.hasPointer()) then body(bufs)
    else {
      val stands = bufs.clone()
      val memories = scala.collection.mutable.ArrayBuffer.empty[(Int, Ptr[Byte])]
      try {
        for i <- bufs.indices if !bufs(i).hasPointer() do {
          val memory = stagingMemory(bufs(i).remaining())
          memories += ((i, memory))
          stands(i) = standIn(bufs(i), memory, copyIn)
        }
        val result = body(stands)
        for (i, _) <- memories do settle(bufs(i), stands(i), copyOut)
        result
      } finally for (_, memory) <- memories do stdlib.free(memory)
    }

  /** Memory for a stand-in of `size` bytes, from `malloc`. */
  def stagingMemory(size: Int): Ptr[Byte] = {
    val memory = stdlib.malloc(math.max(size, 1).toCSize)
    if memory == null then throw new OutOfMemoryError(s"No native memory for a $size-byte staging buffer")
    memory
  }

  /** A stand-in for `buf` over `memory`: its remaining bytes long, holding them if `copyIn`, positioned at 0. */
  def standIn(buf: ByteBuffer, memory: Ptr[Byte], copyIn: Boolean): ByteBuffer = {
    val size = buf.remaining()
    val stand = PointerBuffer.wrap(memory, math.max(size, 1))
    stand.limit(size)
    if copyIn then {
      stand.put(buf.duplicate())
      stand.position(0)
    }
    stand
  }

  /** What an op did to `stand` made `buf`'s: the bytes it moved its position over copied into `buf` if `copyOut`, and
    * `buf`'s position advanced as far.
    */
  def settle(buf: ByteBuffer, stand: ByteBuffer, copyOut: Boolean): Unit = {
    val moved = stand.position()
    if copyOut then {
      val got = stand.duplicate()
      got.flip()
      buf.put(got)
    } else buf.position(buf.position() + moved)
  }

  /** The address of the buffer's first byte. */
  def pointer(buf: ByteBuffer): Ptr[Byte] =
    buf.pointer()

  /** The address of the buffer's current position. */
  def atPosition(buf: ByteBuffer): Ptr[Byte] = buf.pointer() + buf.position()

  /** Moves the position forward after the kernel filled or drained `n` bytes at it. */
  def advance(buf: ByteBuffer, n: Int): Unit = buf.position(buf.position() + n)
}
