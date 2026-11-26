package gears.async.streams

import java.nio.ByteBuffer

/** An immutable run of elements: what a stream hands on at a time, so that the cost of a stage is paid once for many
  * elements rather than once for each. Bytes are held unboxed (`Chunk.Bytes`), anything else as references.
  */
sealed abstract class Chunk[+A] {
  def size: Int

  def apply(i: Int): A

  final def isEmpty: Boolean = size == 0

  final def nonEmpty: Boolean = size != 0

  def foreach(f: A => Unit): Unit = {
    var i = 0
    while i < size do {
      f(apply(i))
      i += 1
    }
  }

  def map[B](f: A => B): Chunk[B] = {
    val out = new Array[AnyRef](size)
    var i = 0
    while i < out.length do {
      out(i) = f(apply(i)).asInstanceOf[AnyRef]
      i += 1
    }
    Chunk.Boxed(out, 0, out.length)
  }

  def filter(p: A => Boolean): Chunk[A] = {
    val out = new Array[AnyRef](size)
    var kept = 0
    var i = 0
    while i < out.length do {
      val a = apply(i)
      if p(a) then {
        out(kept) = a.asInstanceOf[AnyRef]
        kept += 1
      }
      i += 1
    }
    if kept == out.length then this else Chunk.Boxed(out, 0, kept)
  }

  /** The first `n` elements, sharing this chunk's storage. */
  def take(n: Int): Chunk[A]

  /** All but the first `n` elements, sharing this chunk's storage. */
  def drop(n: Int): Chunk[A]

  def foldLeft[B](init: B)(f: (B, A) => B): B = {
    var acc = init
    var i = 0
    while i < size do {
      acc = f(acc, apply(i))
      i += 1
    }
    acc
  }

  def toList: List[A] = {
    val out = List.newBuilder[A]
    foreach(out += _)
    out.result()
  }

  override def toString: String = toList.mkString("Chunk(", ", ", ")")
}

object Chunk {
  private val Empty: Chunk[Nothing] = Boxed(new Array[AnyRef](0), 0, 0)

  def empty[A]: Chunk[A] = Empty

  def single[A](a: A): Chunk[A] = Boxed(Array[AnyRef](a.asInstanceOf[AnyRef]), 0, 1)

  def apply[A](as: A*): Chunk[A] = from(as)

  def from[A](as: Iterable[A]): Chunk[A] = {
    val array = as.asInstanceOf[Iterable[AnyRef]].toArray
    Boxed(array, 0, array.length)
  }

  /** A chunk of the whole of `array`, which is not copied: it is the chunk's from now on, and not to be written to. */
  def bytes(array: Array[Byte]): Bytes = Bytes(array, 0, array.length)

  /** A chunk of `length` bytes of `array` from `offset`, not copied, as the other `bytes`. */
  def bytes(array: Array[Byte], offset: Int, length: Int): Bytes = {
    java.util.Objects.checkFromIndexSize(offset, length, array.length)
    Bytes(array, offset, length)
  }

  /** A chunk of references, `size` of `array`'s from `offset`: the array is the chunk's, never written again. */
  private[streams] final class Boxed[+A](array: Array[AnyRef], offset: Int, val size: Int) extends Chunk[A] {
    def apply(i: Int): A = array(offset + java.util.Objects.checkIndex(i, size)).asInstanceOf[A]

    def take(n: Int): Chunk[A] = if n >= size then this else Boxed(array, offset, math.max(n, 0))

    def drop(n: Int): Chunk[A] =
      if n <= 0 then this else if n >= size then Chunk.empty else Boxed(array, offset + n, size - n)
  }

  /** A chunk of bytes, unboxed. */
  final class Bytes private[Chunk] (array: Array[Byte], offset: Int, val size: Int) extends Chunk[Byte] {
    def apply(i: Int): Byte = array(offset + java.util.Objects.checkIndex(i, size))

    def take(n: Int): Bytes = if n >= size then this else Bytes(array, offset, math.max(n, 0))

    def drop(n: Int): Bytes =
      if n <= 0 then this else if n >= size then Bytes(array, offset, 0) else Bytes(array, offset + n, size - n)

    override def filter(p: Byte => Boolean): Chunk[Byte] = {
      val out = new Array[Byte](size)
      var kept = 0
      var i = 0
      while i < size do {
        val b = array(offset + i)
        if p(b) then {
          out(kept) = b
          kept += 1
        }
        i += 1
      }
      if kept == size then this else Bytes(out, 0, kept)
    }

    /** Copies `length` of these bytes, from the `from`th, into `buf`, which must have room for them. */
    def copyTo(buf: ByteBuffer, from: Int, length: Int): Unit = {
      java.util.Objects.checkFromIndexSize(from, length, size)
      buf.put(array, offset + from, length)
    }

    /** These bytes as a buffer that cannot be written to, not copied. */
    def toByteBuffer: ByteBuffer = ByteBuffer.wrap(array, offset, size).slice().asReadOnlyBuffer()

    def toArray: Array[Byte] = java.util.Arrays.copyOfRange(array, offset, offset + size)
  }
}
