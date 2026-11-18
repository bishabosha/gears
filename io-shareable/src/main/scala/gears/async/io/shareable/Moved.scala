package gears.async.io.shareable

/** What one attempt at an op's work did, which must not block: it moved some bytes, or took some other count of what
  * the op takes; it found the end of a stream, having moved nothing; or the handle was not ready, and nothing was done.
  * One convention for every backend's calls and every op, in place of each call's own sentinels.
  *
  * The count is kept, not only that something was moved: a handle that learns from its event source how much there was
  * knows from it when it has been drained, and so that the next op need not try (see `HandleReadiness.report`).
  */
opaque type Moved = Int

object Moved:

  /** The handle was not ready: the attempt did nothing, and the op waits. */
  inline def Blocked: Moved = -1

  /** The end of a stream: the attempt is done, having moved nothing. */
  inline def EndOfStream: Moved = 0

  /** `count` bytes, or things, moved: not negative. Of a read, none is the end of the stream. */
  inline def apply(count: Int): Moved = count

  /** As `apply`, of a count a call gives in more than an `Int` holds, as a vectored call does: held at the most an
    * `Int` says, since wrapped it would say that nothing was moved, or that the handle was not ready. The bytes are
    * moved whatever is said of them.
    */
  def of(count: Long): Moved = if count > Int.MaxValue then Int.MaxValue else count.toInt

  extension (moved: Moved)

    /** Whether the handle was not ready. */
    inline def isBlocked: Boolean = moved < 0

    /** How much was moved, by an attempt that was not blocked. */
    inline def count: Int = moved
