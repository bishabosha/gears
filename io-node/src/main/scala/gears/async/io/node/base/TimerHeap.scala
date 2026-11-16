package gears.async.io.node.base

/** What a heap of timers holds: an op with a deadline, which the heap fires once it has come. */
trait TimerOp {

  /** The deadline as it stands, on `System.nanoTime`'s clock. */
  def deadlineNow: Long

  /** Asked as the timer is taken from the heap, its deadline come: whether the heap may fire it. A timer that says no
    * is dropped.
    */
  def claimAtDeadline(): Boolean

  /** Whether there is a deadline at all. If not, `deadlineNow` means nothing, and the op stays off the heap. */
  def hasDeadline: Boolean = true

  /** Fires the timer. Precondition: `claimAtDeadline` returned true. */
  def timerWakeup(now: Long): Unit

  /** Where the heap holds this timer, or -1. */
  private[base] var heapIndex = -1

  /** The deadline the heap orders this timer by: at most `deadlineNow`, unless a `notifyDeadlineChange` is to come. */
  private[base] var heapKey = 0L
}

/** Where timers are held, as an op knows it. */
trait TimerSource {
  def addTimer(timer: TimerOp): Unit

  /** Whether `timer` was taken out, and so will not fire; false if it was not held. */
  def removeTimer(timer: TimerOp): Boolean

  /** `timer` has a deadline earlier than the heap knows of. A later one needs no call: see `nextReadyTimer`. */
  def notifyDeadlineChange(timer: TimerOp): Unit
}

object TimerHeap {

  /** A deadline that never comes. */
  inline val Never = Long.MaxValue

  /** `nanos` after `from`, or `Never` if that is beyond what the clock counts. */
  def deadlineAfter(from: Long, nanos: Long): Long =
    if nanos >= Never - from then Never else from + nanos
}

/** A binary heap of timers, each knowing its place, so that removing a timer or bringing it forward takes logarithmic
  * time. Its order is by `heapKey` and never by a deadline read afresh, as a deadline can move at any moment. One heap
  * holds every deadline of the backend, an op's own and its time limit as one entry, so that the event loop is asked
  * for one timer, the earliest: `wake` is called when that changes, when a timer becomes the earliest or the heap
  * empties, and at no other time, so a time limit that comes and goes with its op, as most do, costs the event loop
  * nothing.
  */
final class TimerHeap(wake: () => Unit) extends TimerSource {
  private var heap = new Array[TimerOp | Null](16)
  private var size = 0

  def isEmpty: Boolean = size == 0

  private def place(timer: TimerOp, index: Int): Unit = {
    heap(index) = timer
    timer.heapIndex = index
  }

  private def siftUp(timer: TimerOp, start: Int): Unit = {
    var index = start
    var moving = index > 0
    while moving do {
      val parent = (index - 1) / 2
      if timer.heapKey < heap(parent).nn.heapKey then {
        place(heap(parent).nn, index)
        index = parent
        moving = index > 0
      } else moving = false
    }
    place(timer, index)
  }

  private def siftDown(timer: TimerOp, start: Int): Unit = {
    var index = start
    var moving = true
    while moving do {
      val left = 2 * index + 1
      if left >= size then moving = false
      else {
        val right = left + 1
        val child = if right < size && heap(right).nn.heapKey < heap(left).nn.heapKey then right else left
        if heap(child).nn.heapKey < timer.heapKey then {
          place(heap(child).nn, index)
          index = child
        } else moving = false
      }
    }
    place(timer, index)
  }

  private def takeAt(index: Int): Unit = {
    heap(index).nn.heapIndex = -1
    size -= 1
    val last = heap(size).nn
    heap(size) = null
    if index < size then {
      siftDown(last, index)
      if last.heapIndex == index then siftUp(last, index)
    }
  }

  private def holds(timer: TimerOp): Boolean = {
    val index = timer.heapIndex
    index >= 0 && index < size && (heap(index) eq timer)
  }

  private def rekey(timer: TimerOp): Unit = {
    val previous = timer.heapKey
    timer.heapKey = timer.deadlineNow
    if timer.heapKey < previous then siftUp(timer, timer.heapIndex) else siftDown(timer, timer.heapIndex)
  }

  /** Takes the next timer due by `now` out of the heap, claimed (`claimAtDeadline`), or gives null if none is. A timer
    * moved later is still at its old place: it is put right here, once that place comes up.
    */
  def nextReadyTimer(now: Long): TimerOp | Null = {
    var next: TimerOp | Null = null
    var looking = true
    while looking do
      if size == 0 then looking = false
      else {
        val head = heap(0).nn
        if head.heapKey > now then looking = false
        else if head.deadlineNow > now then rekey(head)
        else {
          takeAt(0)
          if head.claimAtDeadline() then {
            next = head
            looking = false
          }
        }
      }
    next
  }

  /** When to look at the heap next, which may be before anything is due: meaningless if the heap is empty. */
  def peekNextDeadline(): Long = heap(0).nn.heapKey

  def addTimer(timer: TimerOp): Unit = {
    if !timer.hasDeadline then throw new IllegalArgumentException("Only an op with a deadline goes on the heap")
    if size == heap.length then heap = java.util.Arrays.copyOf(heap, 2 * size)
    timer.heapKey = timer.deadlineNow
    size += 1
    siftUp(timer, size - 1)
    if heap(0) eq timer then wake()
  }

  /** Tells the event loop only if nothing is left to wait for: otherwise it wakes at worst for a timer no longer there,
    * and finds nothing to do.
    */
  def removeTimer(timer: TimerOp): Boolean = {
    val held = holds(timer)
    if held then {
      takeAt(timer.heapIndex)
      if size == 0 then wake()
    }
    held
  }

  def notifyDeadlineChange(timer: TimerOp): Unit =
    if holds(timer) then {
      rekey(timer)
      if heap(0) eq timer then wake()
    }
}

/** A call to make once a deadline has come, as a timer on a heap. */
final class DoAfter(val deadlineNow: Long, body: () => Unit) extends TimerOp {
  private var cancelled = false

  def claimAtDeadline(): Boolean = !cancelled

  def timerWakeup(now: Long): Unit = body()

  /** The call is not made, wherever the timer is: taking it off its heap is the canceller's. */
  def cancel(): Unit = cancelled = true
}
