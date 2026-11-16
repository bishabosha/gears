package gears.async.io
package reference

import gears.async.Async

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

/** A handle's read window: direct buffers lent one at a time, at most `buffers` of them held at once, made as they are
  * first needed, so that a handle never read this way costs nothing. A read that finds every buffer held waits here for
  * one to be released; there is at most one such read, since a stream takes one read at a time.
  */
final class RefWindow extends ReadWindow {
  private var size = ReadWindow.DefaultBufferSize
  private var count = ReadWindow.DefaultBuffers
  private val free = scala.collection.mutable.Stack.empty[ByteBuffer]
  private var held = 0
  private var waiting: Wait | Null = null

  def bufferSize: Int = synchronized(size)
  def buffers: Int = synchronized(count)
  def available: Int = synchronized(count - held)

  /** A buffer if one is not held, cleared; else null, with `wait` to be signalled when one is released. */
  private def tryTake(wait: Wait): ByteBuffer | Null = synchronized {
    if held == count then {
      waiting = wait
      null
    } else {
      held += 1
      val buffer = if free.isEmpty then ByteBuffer.allocateDirect(size) else free.pop()
      buffer.clear()
      buffer
    }
  }

  /** Takes a buffer for a read of `op`, on `handle`, waiting for one to be released if all are held: until then the
    * read has taken nothing, and fails as it would waiting for bytes, if `handle` is closed, or the op is cancelled or
    * out of time.
    */
  private[reference] def take(ctx: Ctx, handle: RefHandle, op: AnyRef)(using Async): ByteBuffer = {
    val wait = ctx.waiter
    var buffer: ByteBuffer | Null = null
    try
      while {
        wait.reset()
        buffer = tryTake(wait)
        buffer == null
      } do {
        ctx.park()
        handle.checkOpen()
        ctx.checkStop(op)
      }
    finally synchronized { if waiting eq wait then waiting = null }
    buffer.nn
  }

  /** Gives back a buffer `take` gave, and lets the read waiting for one go on. One of a size the window no longer has
    * is dropped.
    */
  private[reference] def give(buffer: ByteBuffer): Unit = {
    val woken = synchronized {
      held -= 1
      if buffer.capacity() == size then free.push(buffer)
      val wait = waiting
      waiting = null
      wait
    }
    if woken != null then woken.signal()
  }

  /** `filled`, a buffer `take` gave, as the caller's until released. */
  private[reference] def received(filled: ByteBuffer): Received = new Received {
    private val released = new AtomicBoolean()
    def buffer: ByteBuffer = filled
    def release(): Unit = {
      if !released.compareAndSet(false, true) then throw new IllegalStateException("Released already")
      give(filled)
    }
  }

  /** Changes the window's size, which it may only while none of its buffers is held. */
  private[reference] def resize(bufferSize: Int, buffers: Int): Unit = synchronized {
    if held > 0 then
      throw new IllegalStateException(s"$held of the window's buffers are still held, and would be of no window")
    free.clear()
    size = bufferSize
    count = buffers
  }
}
