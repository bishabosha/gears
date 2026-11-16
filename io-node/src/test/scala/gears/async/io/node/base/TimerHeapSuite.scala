package gears.async.io.node.base

/** The heap of timers on its own: its order, what it fires, and when it asks the event loop to be woken. */
class TimerHeapSuite extends munit.FunSuite {

  private final class Timer(var deadline: Long, claimable: Boolean = true) extends TimerOp {
    var firedAt = -1L
    def deadlineNow: Long = deadline
    def claimAtDeadline(): Boolean = claimable
    def timerWakeup(now: Long): Unit = firedAt = now
  }

  private final class Fixture {
    var wakes = 0
    val heap = new TimerHeap(() => wakes += 1)

    /** Fires everything due by `now`, returning it in the order fired. */
    def fire(now: Long): List[TimerOp] = {
      val fired = List.newBuilder[TimerOp]
      var next = heap.nextReadyTimer(now)
      while next != null do {
        next.timerWakeup(now)
        fired += next
        next = heap.nextReadyTimer(now)
      }
      fired.result()
    }
  }

  test("timers fire in deadline order, and only those that are due") {
    val f = new Fixture
    val timers = List(50L, 10L, 40L, 20L, 30L).map(new Timer(_))
    timers.foreach(f.heap.addTimer)
    assertEquals(f.fire(25).map(_.deadlineNow), List(10L, 20L))
    assertEquals(f.heap.peekNextDeadline(), 30L)
    assertEquals(f.fire(100).map(_.deadlineNow), List(30L, 40L, 50L))
    assert(f.heap.isEmpty)
  }

  test("the event loop is woken for a new earliest deadline and for an emptied heap, and for nothing else") {
    val f = new Fixture
    val first = new Timer(100)
    f.heap.addTimer(first)
    assertEquals(f.wakes, 1, "the first timer is the earliest")
    val later = new Timer(200)
    f.heap.addTimer(later)
    assertEquals(f.wakes, 1, "a later one changes nothing")
    val earlier = new Timer(50)
    f.heap.addTimer(earlier)
    assertEquals(f.wakes, 2, "an earlier one does")
    assert(f.heap.removeTimer(earlier))
    assertEquals(f.wakes, 2, "a timer removed leaves the loop to wake for nothing, at worst")
    assert(f.heap.removeTimer(first))
    assert(f.heap.removeTimer(later))
    assertEquals(f.wakes, 3, "with nothing left there is nothing to wake for")
    assert(!f.heap.removeTimer(later), "it was not held")
  }

  test("a timer removed does not fire, wherever it was in the heap") {
    val f = new Fixture
    val timers = (1 to 20).map(i => new Timer(i * 10L))
    timers.foreach(f.heap.addTimer)
    val removed = timers.filter(t => t.deadline % 30 == 0)
    removed.foreach(t => assert(f.heap.removeTimer(t)))
    val fired = f.fire(1000)
    assertEquals(fired.map(_.deadlineNow), timers.filterNot(removed.contains).map(_.deadline).toList)
    assert(removed.forall(_.firedAt == -1L))
  }

  test("a deadline moved later costs nothing until its old place comes up, and then fires at the new one") {
    val f = new Fixture
    val moved = new Timer(10)
    val other = new Timer(20)
    f.heap.addTimer(moved)
    f.heap.addTimer(other)
    val wakes = f.wakes
    moved.deadline = 30 // no call to the heap
    assertEquals(f.wakes, wakes)
    assertEquals(f.fire(25), List(other), "at its old deadline it is put right, not fired")
    assertEquals(f.fire(30), List(moved))
  }

  test("a deadline brought forward is told to the heap, which wakes the loop if it is now the earliest") {
    val f = new Fixture
    val first = new Timer(100)
    val moved = new Timer(200)
    f.heap.addTimer(first)
    f.heap.addTimer(moved)
    val wakes = f.wakes
    moved.deadline = 150
    f.heap.notifyDeadlineChange(moved)
    assertEquals(f.wakes, wakes, "still behind the first")
    moved.deadline = 50
    f.heap.notifyDeadlineChange(moved)
    assertEquals(f.wakes, wakes + 1)
    assertEquals(f.heap.peekNextDeadline(), 50L)
    assertEquals(f.fire(60), List(moved))
  }

  test("a timer that refuses its claim is dropped, and the next one due is fired") {
    val f = new Fixture
    val refused = new Timer(10, claimable = false)
    val next = new Timer(20)
    f.heap.addTimer(refused)
    f.heap.addTimer(next)
    assertEquals(f.fire(30), List(next))
    assertEquals(refused.firedAt, -1L)
    assert(f.heap.isEmpty)
  }

  test("many timers added, moved and removed in no order each fire when their deadline comes, and not before") {
    val f = new Fixture
    val random = new scala.util.Random(42)
    val timers = Vector.fill(500)(new Timer(random.nextInt(10_000).toLong))
    timers.foreach(f.heap.addTimer)
    val removed = timers.filter(_ => random.nextInt(4) == 0).toSet
    removed.foreach(f.heap.removeTimer)
    for timer <- timers if !removed(timer) && random.nextBoolean() do {
      val to = random.nextInt(10_000).toLong
      val earlier = to < timer.deadline
      timer.deadline = to
      if earlier then f.heap.notifyDeadlineChange(timer)
    }
    val kept = timers.filterNot(removed)
    // Time passes a deadline at a time: among timers due at once the heap has no order, as one moved later keeps
    // its old place until that comes up.
    for now <- kept.map(_.deadline).distinct.sorted do f.fire(now): Unit
    for timer <- kept do assertEquals(timer.firedAt, timer.deadline)
    assert(removed.forall(_.firedAt == -1L))
    assert(f.heap.isEmpty)
  }
}
