package asyncio.nio

import java.nio.ByteBuffer

import asyncio.BufferPool
import asyncio.Received

/** A `BufferPool` of direct buffers, for NIO: one allocation sliced into `buffers` of `bufferSize` bytes, a `Received`
  * made once per buffer and reused, and a free list guarded by the pool's lock, since a `Received` may be released
  * from any thread.
  */
final class DirectPool(val bufferSize: Int, val buffers: Int) extends BufferPool {
  require(bufferSize > 0 && buffers > 0, "A pool needs buffers of some size")
  private val memory = ByteBuffer.allocateDirect(bufferSize * buffers)
  private val slots = Array.tabulate(buffers)(i => new Slot(i))
  private val free = new java.util.ArrayDeque[Slot](buffers)
  slots.foreach(free.add)
  private var closed = false

  final class Slot(val index: Int) extends Received {
    val buffer: ByteBuffer = memory.slice(index * bufferSize, bufferSize)
    def release(): Unit = DirectPool.this.release(this)
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
    }
  }
}
