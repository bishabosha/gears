package gears.async.io.node.host

import java.nio.ByteBuffer
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.TypedArrayBuffer
import scala.scalajs.js.typedarray.Uint8Array

/** Bytes between the caller's buffers and Node's chunks. Node keeps a chunk it is given until it has sent it, and hands
  * over chunks of its own, so bytes are copied between a chunk and a buffer, of any kind: on the heap, direct or
  * read-only.
  */
private[node] object NodeBuffer {

  /** The bytes remaining in `bufs`, together. */
  def remaining(bufs: Array[ByteBuffer]): Long = {
    var total = 0L
    var i = 0
    while i < bufs.length do {
      total += bufs(i).remaining()
      i += 1
    }
    total
  }

  /** `remaining(bufs)`, or `Int.MaxValue` if it is more. */
  def room(bufs: Array[ByteBuffer]): Int = math.min(remaining(bufs), Int.MaxValue.toLong).toInt

  /** A chunk of Node's, a `Buffer`, as the signed bytes a `ByteBuffer` is over: the same memory. */
  def signed(chunk: Uint8Array): Int8Array = new Int8Array(chunk.buffer, chunk.byteOffset, chunk.length)

  /** Copies `chunk` into `bufs` in order, each filled before the next is begun; `chunk` is no longer than their room.
    */
  def fill(bufs: Array[ByteBuffer], chunk: Int8Array): Unit = {
    val from = TypedArrayBuffer.wrap(chunk)
    var i = 0
    while from.hasRemaining() do {
      val buf = bufs(i)
      val n = math.min(buf.remaining(), from.remaining())
      if n > 0 then {
        val part = from.duplicate()
        part.limit(part.position() + n)
        buf.put(part)
        from.position(from.position() + n)
      }
      i += 1
    }
  }

  /** Takes `n` bytes out of `bufs` in order, each drained before the next is begun, as a chunk for Node to keep. */
  def drain(bufs: Array[ByteBuffer], n: Int): Int8Array = {
    val chunk = new Int8Array(n)
    val into = TypedArrayBuffer.wrap(chunk)
    var i = 0
    while into.hasRemaining() do {
      val buf = bufs(i)
      val take = math.min(buf.remaining(), into.remaining())
      if take > 0 then {
        val part = buf.duplicate()
        part.limit(part.position() + take)
        into.put(part)
        buf.position(buf.position() + take)
      }
      i += 1
    }
    chunk
  }

  /** Passes over `n` bytes of `bufs` in order, as a write that took them does. */
  def skip(bufs: Array[ByteBuffer], n: Int): Unit = {
    var left = n
    var i = 0
    while left > 0 do {
      val buf = bufs(i)
      val step = math.min(buf.remaining(), left)
      buf.position(buf.position() + step)
      left -= step
      i += 1
    }
  }
}
