package asyncio

/** Names an open file, socket, or other I/O object of a backend. Underneath it is a number whose meaning belongs to
  * the backend: the I/O object itself where that is an integer, such as a POSIX descriptor, and otherwise an index
  * into a table the backend keeps of its I/O objects, such as JVM channels (`asyncio.unsafe.HandleTable`). The number is hidden by
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
