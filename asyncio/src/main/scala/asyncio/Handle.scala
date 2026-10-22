package asyncio

import java.io.IOException
import java.util.concurrent.atomic.AtomicReferenceArray

/** Names an open file, socket, or other I/O object of a backend. Underneath it is a number whose meaning belongs to
  * the backend: the I/O object itself where that is an integer, such as a POSIX descriptor, and otherwise an index
  * into a table the backend keeps of its I/O objects, such as JVM channels (`Handle.Table`). The number is hidden by
  * the type: a handle cannot be mistaken for an `Int`, nor an `Int` passed as a handle, and only the backends convert
  * between the two (`wrap` and `unwrap`, visible in the `asyncio` packages; packages being open, that guards against
  * mistakes, not intent). Handles belong to the caller, who closes them with `Ops.close`; the backend never closes
  * them. A handle is unboxed, an `Int` at runtime, and so are `Handle.Opt` and `Handle.Slot`, which hold one or none.
  */
opaque type Handle = Int

object Handle {

  /** The handle with `number`, for a backend. */
  private[asyncio] def wrap(number: Int): Handle = number

  /** `handle`'s number, for a backend. */
  private[asyncio] def unwrap(handle: Handle): Int = handle

  /** A handle or none, unboxed: handles are never negative, so -1 is none. */
  opaque type Opt = Int

  object Opt {
    val empty: Opt = -1

    def apply(handle: Handle): Opt = handle

    extension (opt: Opt) {
      def isEmpty: Boolean = opt < 0
      def nonEmpty: Boolean = opt >= 0

      /** The handle; throws `NoSuchElementException` if there is none. */
      def get: Handle = {
        if opt < 0 then throw new NoSuchElementException("No handle")
        opt
      }
    }
  }

  /** A process-wide table of what a backend's handles name, for a backend whose I/O objects are not numbers: a handle
    * is an index into it, so that, like a descriptor, it means the same on every thread. Numbers freed by `remove` are
    * reused, lowest last freed first. Looking a handle up takes no lock; adding and removing take the table's.
    */
  final class Table[E <: AnyRef](initialCapacity: Int = 64) {
    private val lock = new Object
    @volatile private var table = new AtomicReferenceArray[E | Null](initialCapacity)
    private var free = new Array[Int](16) // a stack of freed numbers, guarded by `lock`
    private var freed = 0 // how many are on it
    private var next = 0 // the next number never used, guarded by `lock`

    /** Gives `entry` a handle. */
    def add(entry: E): Handle = lock.synchronized {
      val handle =
        if freed > 0 then { freed -= 1; free(freed) }
        else { next += 1; next - 1 }
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

    /** What `handle` names, or null if it is not open. */
    def find(handle: Handle): E | Null = {
      val current = table
      if handle >= 0 && handle < current.length() then current.get(handle) else null
    }

    /** What `handle` names; throws if it is not open. */
    def apply(handle: Handle): E = {
      val found = find(handle)
      if found == null then throw new IOException(s"Not an open handle: $handle")
      found.nn
    }

    /** Frees `handle` and returns what it named; throws if it is not open. */
    def remove(handle: Handle): E = {
      val removed = lock.synchronized {
        val current = table
        val found = if handle >= 0 && handle < current.length() then current.get(handle) else null
        if found != null then {
          current.set(handle, null)
          if freed == free.length then free = java.util.Arrays.copyOf(free, free.length * 2)
          free(freed) = handle
          freed += 1
        }
        found
      }
      if removed == null then throw new IOException(s"Not an open handle: $handle")
      removed.nn
    }
  }

  /** A `Slot` for a handle, which holds it unboxed, as an `Opt`; otherwise it has the same contract as `Slot`. */
  final class Slot {
    private var handle: Opt = Opt.empty

    def clear(): Handle = {
      val h = handle
      if h < 0 then throw new IllegalStateException("Nothing has been written to this slot yet")
      handle = Opt.empty
      h
    }

    private[asyncio] def set(handle: Handle): Unit = {
      require(handle >= 0, s"Not a handle: $handle")
      if this.handle >= 0 then throw new IllegalStateException("Slot was already set")
      this.handle = handle
    }
  }
}
