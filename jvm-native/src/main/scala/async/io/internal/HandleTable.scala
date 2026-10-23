package gears.async.io
package internal

import java.io.IOException
import java.util.concurrent.atomic.AtomicReferenceArray

import gears.async.io.Handle

/** A process-wide table of what a backend's handles name, for a backend whose I/O objects are not numbers, such as
  * NIO's channels or Network.framework's connections: a handle is an index into it, so that, like a descriptor, it
  * means the same on every thread. Numbers freed by `remove` are reused, lowest last freed first. Looking a handle up
  * takes no lock; adding and removing take the table's.
  */
final class HandleTable[E <: AnyRef](initialCapacity: Int = 64) {
  private val lock = new Object
  @volatile private var table = new AtomicReferenceArray[E | Null](initialCapacity)
  private var free = new Array[Int](16) // a stack of freed numbers, guarded by `lock`
  private var freed = 0 // how many are on it
  private var next = 0 // the next number never used, guarded by `lock`

  /** Gives `entry` a handle. */
  def add(entry: E): Handle = lock.synchronized {
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
    Handle.wrap(number)
  }

  /** What `handle` names, or null if it is not open. */
  def find(handle: Handle): E | Null = {
    val number = Handle.unwrap(handle)
    val current = table
    if number >= 0 && number < current.length() then current.get(number) else null
  }

  /** What `handle` names; throws if it is not open. */
  def apply(handle: Handle): E = {
    val found = find(handle)
    if found == null then throw new IOException(s"Not an open handle: $handle")
    found.nn
  }

  /** Frees `handle` and returns what it named; throws if it is not open. */
  def remove(handle: Handle): E = {
    val number = Handle.unwrap(handle)
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
    if removed == null then throw new IOException(s"Not an open handle: $handle")
    removed.nn
  }
}
