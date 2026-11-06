package gears.async.io.kqueue

/** An indexed binary min-heap owned by one worker. Cached primitive keys avoid chasing task/op pointers in comparisons.
  * Insert, remove and rekey are O(log n); only capacity growth allocates. Producers never access this object.
  */
private[kqueue] final class TimerQueue {
  private var tasks = new Array[TimerTask | Null](16)
  private var deadlines = new Array[Long](16)
  private var size = 0

  def isEmpty: Boolean = size == 0
  def first: TimerTask = tasks(0).nn
  def firstDeadline: Long = deadlines(0)

  def update(task: TimerTask, deadline: Long): Unit = {
    var index = task.heapIndex
    if index < 0 then {
      if size == tasks.length then grow()
      index = size
      size += 1
      tasks(index) = task
      task.heapIndex = index
    }
    deadlines(index) = deadline
    if siftUp(index) == index then siftDown(index)
  }

  def remove(task: TimerTask): Unit = {
    val index = task.heapIndex
    if index < 0 then return
    size -= 1
    task.heapIndex = -1
    if index != size then {
      tasks(index) = tasks(size)
      deadlines(index) = deadlines(size)
      tasks(index).nn.heapIndex = index
    }
    tasks(size) = null
    if index < size && siftUp(index) == index then siftDown(index)
  }

  private def siftUp(from: Int): Int = {
    var index = from
    while index > 0 do {
      val parent = (index - 1) >>> 1
      if deadlines(index) - deadlines(parent) >= 0 then return index
      swap(index, parent)
      index = parent
    }
    index
  }

  private def siftDown(from: Int): Unit = {
    var index = from
    var child = index * 2 + 1
    while child < size do {
      if child + 1 < size && deadlines(child + 1) - deadlines(child) < 0 then child += 1
      if deadlines(index) - deadlines(child) <= 0 then return
      swap(index, child)
      index = child
      child = index * 2 + 1
    }
  }

  private def swap(a: Int, b: Int): Unit = {
    val task = tasks(a)
    val deadline = deadlines(a)
    tasks(a) = tasks(b)
    deadlines(a) = deadlines(b)
    tasks(b) = task
    deadlines(b) = deadline
    tasks(a).nn.heapIndex = a
    tasks(b).nn.heapIndex = b
  }

  private def grow(): Unit = {
    val capacity = tasks.length * 2
    val nextTasks = new Array[TimerTask | Null](capacity)
    val nextDeadlines = new Array[Long](capacity)
    System.arraycopy(tasks, 0, nextTasks, 0, size)
    System.arraycopy(deadlines, 0, nextDeadlines, 0, size)
    tasks = nextTasks
    deadlines = nextDeadlines
  }
}
