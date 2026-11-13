package gears.async.io.shareable

/** What an event source holds of an op waiting for its handle to be ready: the ops waiting on one handle are a list
  * through `nextWaiting`, which the event source alone reads and writes, under its own lock.
  */
trait ReadinessOp:
  private[io] var nextWaiting: ReadinessOp | Null = null

  /** Claims the op for the worker that found its handle ready. The event source calls it while it still holds the op as
    * waiting, and under the lock its `unregister` takes: a performer can't leave, and the op be performed again,
    * between the two.
    */
  def claimAtReady(): Boolean

  /** Worker thread, after `claimAtReady`: tries the op's work again. True if that settled the op; false if the handle
    * was not ready after all, which leaves the op claimed, for the event source to hold as waiting again and then
    * `releaseReady`.
    */
  def readyWakeup(): Boolean

  /** Gives the claim back: whether the op still waits. */
  def releaseReady(): Boolean

/** The ops one worker holds as waiting for descriptors to be ready, in one direction: a list for each descriptor, in
  * the order they came, through `ReadinessOp.nextWaiting`, in an array the descriptor indexes. It has no lock of its
  * own: its event source guards it with the lock that also guards what it registers with the kernel.
  */
final class ReadinessWaiters {
  private var heads = new Array[ReadinessOp | Null](64)

  def first(fd: Int): ReadinessOp | Null =
    if fd < heads.length then heads(fd) else null

  def isEmpty(fd: Int): Boolean = first(fd) == null

  private def ensure(fd: Int): Unit =
    if fd >= heads.length then heads = java.util.Arrays.copyOf(heads, Math.max(fd + 1, 2 * heads.length))

  /** Last in line. */
  def append(fd: Int, op: ReadinessOp): Unit = {
    ensure(fd)
    op.nextWaiting = null
    var last = heads(fd)
    if last == null then heads(fd) = op
    else
      while last.nextWaiting != null do last = last.nextWaiting
      last.nextWaiting = op
  }

  /** First in line again: for an op taken with `takeFirst` whose descriptor was not ready after all. */
  def prepend(fd: Int, op: ReadinessOp): Unit = {
    ensure(fd)
    op.nextWaiting = heads(fd)
    heads(fd) = op
  }

  def takeFirst(fd: Int): ReadinessOp | Null = {
    val op = first(fd)
    if op != null then
      heads(fd) = op.nextWaiting
      op.nextWaiting = null
    op
  }

  /** Whether `op` was waiting on `fd`, and no longer is. */
  def remove(fd: Int, op: ReadinessOp): Boolean = {
    var previous: ReadinessOp | Null = null
    var current = first(fd)
    while current != null && (current ne op) do
      previous = current
      current = current.nextWaiting
    if current == null then false
    else
      if previous == null then heads(fd) = op.nextWaiting else previous.nextWaiting = op.nextWaiting
      op.nextWaiting = null
      true
  }
}
