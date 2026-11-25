package gears.async.io.flow

/** Elements that travel a flow together: what one step of a source produced, a read's worth of requests say, which the
  * stages after it take in one call, and a sink writes in one write. It does not change once made.
  */
final class Chunk[+A] private (items: Array[AnyRef], val size: Int) {
  def isEmpty: Boolean = size == 0

  def apply(i: Int): A = {
    if i < 0 || i >= size then throw new IndexOutOfBoundsException(s"$i of $size")
    items(i).asInstanceOf[A]
  }

  def foreach(f: A => Unit): Unit = {
    var i = 0
    while i < size do {
      f(items(i).asInstanceOf[A])
      i += 1
    }
  }

  def map[B](f: A => B): Chunk[B] = {
    val out = new Array[AnyRef](size)
    var i = 0
    while i < size do {
      out(i) = f(items(i).asInstanceOf[A]).asInstanceOf[AnyRef]
      i += 1
    }
    new Chunk[B](out, size)
  }

  def filter(keep: A => Boolean): Chunk[A] = {
    val out = new Array[AnyRef](size)
    var kept = 0
    var i = 0
    while i < size do {
      if keep(items(i).asInstanceOf[A]) then {
        out(kept) = items(i)
        kept += 1
      }
      i += 1
    }
    if kept == size then this else new Chunk[A](out, kept)
  }

  /** The first `n` elements, or all of them if there are fewer. */
  def take(n: Int): Chunk[A] = if n >= size then this else new Chunk[A](items, math.max(n, 0))

  /** All but the first `n` elements. */
  def drop(n: Int): Chunk[A] =
    if n <= 0 then this
    else if n >= size then Chunk.empty
    else new Chunk[A](java.util.Arrays.copyOfRange(items, n, size), size - n)

  def toList: List[A] = {
    var out: List[A] = Nil
    var i = size - 1
    while i >= 0 do {
      out = items(i).asInstanceOf[A] :: out
      i -= 1
    }
    out
  }

  override def toString: String = toList.mkString("Chunk(", ", ", ")")
}

object Chunk {
  val empty: Chunk[Nothing] = new Chunk[Nothing](new Array[AnyRef](0), 0)

  def one[A](a: A): Chunk[A] = {
    val items = new Array[AnyRef](1)
    items(0) = a.asInstanceOf[AnyRef]
    new Chunk[A](items, 1)
  }

  def from[A](elements: Iterable[A]): Chunk[A] = {
    val builder = new Builder[A]
    elements.foreach(builder += _)
    builder.result()
  }

  /** Gathers elements for a chunk: `result` gives what was added since the last one. */
  final class Builder[A] {
    private var items = new Array[AnyRef](8)
    private var count = 0

    def size: Int = count
    def isEmpty: Boolean = count == 0

    def +=(a: A): Unit = {
      if count == items.length then items = java.util.Arrays.copyOf(items, count * 2)
      items(count) = a.asInstanceOf[AnyRef]
      count += 1
    }

    def result(): Chunk[A] =
      if count == 0 then empty
      else {
        val chunk = new Chunk[A](items, count)
        items = new Array[AnyRef](math.max(8, count))
        count = 0
        chunk
      }
  }
}
