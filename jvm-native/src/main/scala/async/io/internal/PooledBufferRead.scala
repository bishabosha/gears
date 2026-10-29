package gears.async.io.internal

import gears.async.io.{Received, Slot}

import java.nio.ByteBuffer

/** State local to a pooled read; neither the common operation nor an uncontended submission allocates a waiter. */
private[io] trait PooledRead {
  private[io] def bufferPool: FreeListPool
  private[io] def bufferSlot: Slot[Received]
  private[io] var needsBuffer = false
  private[io] var bufferWait: FreeListPool#Waiter | Null = null

  private[io] def stopBufferWait(): Unit = {
    val wait = bufferWait
    bufferWait = null
    needsBuffer = false
    if wait != null then wait.cancel()
  }
}

/** Buffer ownership for one synchronous read attempt, independent of readiness registration and operation completion. A
  * read returns 0 if it would block or no buffer is available, -1 at EOF, or the number of bytes transferred. Success
  * (including EOF) publishes a flipped buffer to the caller; every other exit returns it to the pool. The caller must
  * provide an empty slot.
  */
private[io] object PooledBufferRead {
  inline def attempt(pool: FreeListPool, into: Slot[Received])(inline read: ByteBuffer => Int): Int = {
    val buffer = pool.take()
    if buffer == null then 0 else borrowed(buffer, into)(read)
  }

  inline def attempt(op: PooledRead)(inline read: ByteBuffer => Int): Int = {
    val wait = op.bufferWait
    val buffer = if wait == null then op.bufferPool.take() else wait.consume()
    op.bufferWait = null
    op.needsBuffer = buffer == null
    if buffer == null then 0 else borrowed(buffer, op.bufferSlot)(read)
  }

  private inline def borrowed(buffer: Received, into: Slot[Received])(inline read: ByteBuffer => Int): Int = {
    var published = false
    try {
      val count = read(buffer.buffer)
      if count != 0 then {
        buffer.buffer.flip()
        into.set(buffer)
        published = true
      }
      count
    } finally if !published then buffer.release()
  }
}
