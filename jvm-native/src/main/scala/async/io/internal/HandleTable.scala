package gears.async.io
package internal

import java.io.IOException
import java.util.concurrent.atomic.AtomicReferenceArray

/** A process-wide table of a backend's I/O objects by number, for a backend whose foreign side refers to them by
  * number, as Network.framework's C side tags each of its handles with one, so that an event it delivers finds its
  * object without a lock. Handles themselves are objects (see `Backend.Handle`); the number is the backend's own.
  * Numbers freed by `remove` are reused, lowest last freed first. Looking a number up takes no lock; adding and
  * removing take the table's.
  */
final class HandleTable[E <: AnyRef](initialCapacity: Int = 64) {
  private val lock = new Object
  @volatile private var table = new AtomicReferenceArray[E | Null](initialCapacity)
  private var free = new Array[Int](16) // a stack of freed numbers, guarded by `lock`
  private var freed = 0 // how many are on it
  private var next = 0 // the next number never used, guarded by `lock`

  /** Gives `entry` a number. */
  def add(entry: E): Int = lock.synchronized {
    val number =
      if freed > 0 then { freed -= 1; free(freed) }
      else { next += 1; next - 1 }
    var current = table
    if number >= current.length() then {
      val grown = new AtomicReferenceArray[E | Null](current.length() * 2)
      var i = 0
      while i < current.length() do {
        grown.set(i, current.get(i))
        i += 1
      }
      table = grown
      current = grown
    }
    current.set(number, entry)
    number
  }

  /** What `number` names, or null if nothing does. */
  def find(number: Int): E | Null = {
    val current = table
    if number >= 0 && number < current.length() then current.get(number) else null
  }

  /** What `number` names; throws if nothing does. */
  def apply(number: Int): E = {
    val found = find(number)
    if found == null then throw new IOException(s"Not an open handle: $number")
    found.nn
  }

  /** Frees `number` and returns what it named; throws if nothing does. */
  def remove(number: Int): E = {
    val removed = lock.synchronized {
      val current = table
      val found = if number >= 0 && number < current.length() then current.get(number) else null
      if found != null then {
        current.set(number, null)
        if freed == free.length then free = java.util.Arrays.copyOf(free, free.length * 2)
        free(freed) = number
        freed += 1
      }
      found
    }
    if removed == null then throw new IOException(s"Not an open handle: $number")
    removed.nn
  }
}
