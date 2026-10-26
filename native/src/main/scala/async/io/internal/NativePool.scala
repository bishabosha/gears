package gears.async.io
package internal

import gears.async.io.BufferPool
import gears.async.io.Received

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.scalanative.libc.stdlib
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** A `BufferPool` in C memory, for the Native backends: one `malloc` for all the buffers, a `ByteBuffer` and a
  * `Received` made once per buffer and reused, and a free list that takes no lock, since a `Received` may be released
  * from any thread and a readiness backend takes and releases a buffer on every read: a stack of slot indexes whose
  * head is a compare-and-set word, the index tagged with a count of changes so that a slot released and taken again
  * meanwhile cannot be mistaken for the one seen. A buffer's address is `address(index)`, for a backend that receives
  * straight into it.
  */
final class NativePool(val bufferSize: Int, val buffers: Int) extends BufferPool {
  require(bufferSize > 0 && buffers > 0, "A pool needs buffers of some size")
  private val memory: Ptr[Byte] = stdlib.malloc((bufferSize.toLong * buffers).toCSize)
  private val slots = Array.tabulate(buffers)(i => new Slot(i))

  // The free stack: `head` holds the top slot's index, -1 for none, in its low 32 bits and a change count above.
  private val head = new AtomicLong(NativePool.pack(-1, 0))
  private val free = new AtomicInteger(buffers)
  @volatile private var closed = false
  (buffers - 1 to 0 by -1).foreach(i => push(slots(i)))

  /** One of the pool's buffers, as the `Received` of a read that filled it. */
  final class Slot(val index: Int) extends Received {
    val buffer: ByteBuffer = PointerBuffer.wrap(memory + index.toLong * bufferSize, bufferSize)
    def address: Ptr[Byte] = memory + index.toLong * bufferSize

    /** The index of the slot below this one on the free stack, while it is on it. */
    @volatile private[NativePool] var next = -1

    /** Whether a read or a `Received` holds this buffer: set by `take`, cleared by `release`, which refuses a second.
      */
    private[NativePool] val held = new AtomicBoolean(false)

    def release(): Unit = NativePool.this.release(this)
  }

  def available: Int = free.get()

  /** Takes a free buffer, cleared, or null if none is. */
  def take(): Slot | Null = {
    if closed then throw new IllegalStateException("The pool is closed")
    var taken: Slot | Null = null
    var done = false
    while !done do {
      val seen = head.get()
      val index = NativePool.index(seen)
      if index < 0 then done = true
      else {
        val slot = slots(index)
        if head.compareAndSet(seen, NativePool.pack(slot.next, NativePool.tag(seen) + 1)) then {
          taken = slot
          done = true
        }
      }
    }
    if taken != null then {
      taken.held.set(true)
      free.decrementAndGet()
      taken.buffer.clear()
    }
    taken
  }

  private def release(slot: Slot): Unit = {
    // A second release would push the slot again, making a cycle of the free stack, so it fails instead.
    if !slot.held.compareAndSet(true, false) then
      throw new IllegalStateException(s"Buffer ${slot.index} of the pool was released already")
    push(slot)
    free.incrementAndGet()
  }

  private def push(slot: Slot): Unit = {
    var done = false
    while !done do {
      val seen = head.get()
      slot.next = NativePool.index(seen)
      done = head.compareAndSet(seen, NativePool.pack(slot.index, NativePool.tag(seen) + 1))
    }
  }

  def close(): Unit = synchronized {
    if !closed then {
      // Checked first, so that a pool closed too early stays open, for its buffers to be released and the close done.
      require(free.get() == buffers, s"${buffers - free.get()} of the pool's buffers are still held")
      closed = true
      stdlib.free(memory)
    }
  }
}

private object NativePool {
  private inline def pack(index: Int, tag: Int): Long = (tag.toLong << 32) | (index.toLong & 0xffffffffL)
  private inline def index(word: Long): Int = word.toInt
  private inline def tag(word: Long): Int = (word >>> 32).toInt
}
