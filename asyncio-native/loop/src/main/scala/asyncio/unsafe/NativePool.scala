package asyncio.unsafe

import java.nio.ByteBuffer
import scala.scalanative.libc.stdlib
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.BufferPool
import asyncio.Received

/** A `BufferPool` in C memory, for the Native backends: one `malloc` for all the buffers, a `ByteBuffer` and a
  * `Received` made once per buffer and reused, and a free list guarded by the pool's lock, since a `Received` may be
  * released from any thread. A buffer's address is `address(index)`, for a backend that receives straight into it.
  */
final class NativePool(val bufferSize: Int, val buffers: Int) extends BufferPool {
  require(bufferSize > 0 && buffers > 0, "A pool needs buffers of some size")
  private val memory: Ptr[Byte] = stdlib.malloc((bufferSize.toLong * buffers).toCSize)
  private val slots = Array.tabulate(buffers)(i => new Slot(i))
  private val free = new java.util.ArrayDeque[Slot](buffers)
  slots.foreach(free.add)
  private var closed = false

  /** One of the pool's buffers, as the `Received` of a read that filled it. */
  final class Slot(val index: Int) extends Received {
    val buffer: ByteBuffer = PointerBuffer.wrap(memory + index.toLong * bufferSize, bufferSize)
    def address: Ptr[Byte] = memory + index.toLong * bufferSize
    def release(): Unit = NativePool.this.release(this)
  }

  def available: Int = synchronized(free.size)

  /** Takes a free buffer, cleared, or null if none is. */
  def take(): Slot | Null = synchronized {
    if closed then throw new IllegalStateException("The pool is closed")
    val slot = free.poll()
    if slot != null then slot.buffer.clear()
    slot
  }

  private def release(slot: Slot): Unit = synchronized {
    if !closed then free.add(slot)
  }

  def close(): Unit = synchronized {
    if !closed then {
      closed = true
      require(free.size == buffers, s"${buffers - free.size} of the pool's buffers are still held")
      free.clear()
      stdlib.free(memory)
    }
  }
}
