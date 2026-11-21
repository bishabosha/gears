package gears.async.io

/** A queue, first in first out, of one thread's: an array that is gone round, and doubled when it is full. In place of
  * `java.util.ArrayDeque`, which on Scala.js loses what it holds when it grows after something has been taken from it.
  */
private[io] final class Ring[A <: AnyRef] {
  private var slots = new Array[AnyRef | Null](16)
  private var head = 0
  private var count = 0

  def size: Int = count
  def isEmpty: Boolean = count == 0

  def add(element: A): Unit = {
    if count == slots.length then {
      val grown = new Array[AnyRef | Null](count * 2)
      var i = 0
      while i < count do {
        grown(i) = slots((head + i) & (slots.length - 1))
        i += 1
      }
      slots = grown
      head = 0
    }
    slots((head + count) & (slots.length - 1)) = element
    count += 1
  }

  /** The first element, left where it is; null if there is none. */
  def peek(): A | Null = if count == 0 then null else slots(head).asInstanceOf[A]

  /** The first element, taken; null if there is none. */
  def poll(): A | Null =
    if count == 0 then null
    else {
      val element = slots(head)
      slots(head) = null
      head = (head + 1) & (slots.length - 1)
      count -= 1
      element.asInstanceOf[A]
    }
}
