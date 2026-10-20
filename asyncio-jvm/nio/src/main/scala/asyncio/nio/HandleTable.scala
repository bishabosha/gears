package asyncio.nio

import java.io.IOException
import java.util.concurrent.atomic.AtomicReferenceArray

/** A process-wide table of what handles name, indexed by handle, so that, like descriptors, a handle means the same on
  * every thread. Numbers freed by `remove` are reused. Looking a handle up takes no lock.
  */
private[nio] final class HandleTable[E <: AnyRef] {
  private val lock = new Object
  @volatile private var table = new AtomicReferenceArray[E | Null](64)
  private val free = new java.util.ArrayDeque[Integer]() // guarded by `lock`
  private var next = 0 // guarded by `lock`

  /** Gives `entry` a handle. */
  def add(entry: E): Int = lock.synchronized {
    val handle = if free.isEmpty then { next += 1; next - 1 }
    else free.pop().intValue
    var current = table
    if handle >= current.length() then {
      val grown = new AtomicReferenceArray[E | Null](current.length() * 2)
      var i = 0
      while i < current.length() do {
        grown.set(i, current.get(i))
        i += 1
      }
      table = grown
      current = grown
    }
    current.set(handle, entry)
    handle
  }

  /** What `handle` names; throws if it is not open. */
  def apply(handle: Int): E = {
    val current = table
    val found = if handle >= 0 && handle < current.length() then current.get(handle) else null
    if found == null then throw new IOException(s"Not an open handle: $handle")
    found.nn
  }

  /** Frees `handle` and returns what it named; throws if it is not open. */
  def remove(handle: Int): E = {
    val removed = lock.synchronized {
      val current = table
      val found = if handle >= 0 && handle < current.length() then current.get(handle) else null
      if found != null then {
        current.set(handle, null)
        free.push(handle)
      }
      found
    }
    if removed == null then throw new IOException(s"Not an open handle: $handle")
    removed.nn
  }
}
