package gears.async.io.node.host

import java.nio.ByteBuffer
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.TypedArrayBuffer
import scala.scalajs.js.typedarray.TypedArrayBufferOps.*
import scala.scalajs.js.typedarray.Uint8Array
import scala.scalajs.js.typedarray.byteArray2Int8Array
import scala.scalajs.js.typedarray.int8Array2ByteArray

/** Bytes between the caller's buffers and Node's chunks. Node keeps a chunk it is given until it has sent it, and hands
  * over chunks of its own, so bytes are copied between a chunk and a buffer, of any kind: on the heap, direct or
  * read-only. A direct buffer is the one that costs a single copy, of the block; the others cost two.
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

  /** Copies `count` bytes of `chunk`, from `from`, into `buf` at its position, which they advance. A direct buffer is
    * memory JavaScript has, so the bytes are copied as a block; one on the heap is an array of WebAssembly's, which
    * JavaScript cannot reach, so they go by way of an array, converted as a block too. Byte by byte, each crossing
    * between the two, is what is left for a buffer that is neither.
    */
  private def put(buf: ByteBuffer, chunk: Int8Array, from: Int, count: Int): Unit =
    if buf.hasArrayBuffer() then {
      new Int8Array(buf.arrayBuffer(), buf.arrayBufferOffset() + buf.position(), count)
        .set(chunk.subarray(from, from + count))
      buf.position(buf.position() + count)
    } else if buf.hasArray() then buf.put(int8Array2ByteArray(chunk.subarray(from, from + count))): Unit
    else {
      val part = TypedArrayBuffer.wrap(chunk)
      part.position(from).limit(from + count)
      buf.put(part): Unit
    }

  /** Copies `count` bytes of `buf`, from its position, which they advance, into `chunk` at `at`: as `put` does. A
    * read-only buffer hides its memory, and is copied by way of an array.
    */
  private def get(buf: ByteBuffer, chunk: Int8Array, at: Int, count: Int): Unit =
    if buf.hasArrayBuffer() then {
      chunk.set(new Int8Array(buf.arrayBuffer(), buf.arrayBufferOffset() + buf.position(), count), at)
      buf.position(buf.position() + count)
    } else {
      val bytes = new Array[Byte](count)
      buf.get(bytes)
      chunk.set(byteArray2Int8Array(bytes), at)
    }

  /** Copies `chunk` into `bufs` in order, each filled before the next is begun; `chunk` is no longer than their room.
    */
  def fill(bufs: Array[ByteBuffer], chunk: Int8Array): Unit = {
    var from = 0
    var i = 0
    while from < chunk.length do {
      val buf = bufs(i)
      val count = math.min(buf.remaining(), chunk.length - from)
      if count > 0 then {
        put(buf, chunk, from, count)
        from += count
      }
      i += 1
    }
  }

  /** Takes `n` bytes out of `bufs` in order, each drained before the next is begun, as a chunk for Node to keep. */
  def drain(bufs: Array[ByteBuffer], n: Int): Int8Array = {
    val chunk = new Int8Array(n)
    var at = 0
    var i = 0
    while at < n do {
      val buf = bufs(i)
      val count = math.min(buf.remaining(), n - at)
      if count > 0 then {
        get(buf, chunk, at, count)
        at += count
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
