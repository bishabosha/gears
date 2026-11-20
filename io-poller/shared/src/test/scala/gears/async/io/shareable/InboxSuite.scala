package gears.async.io.shareable

/** What a thief may take from a worker's inbox, and when: at times the checks give, not the clock's. */
class InboxSuite extends munit.FunSuite {
  private val start = 1_000_000_000L
  private val later = start + Inbox.StealDelayNanos

  private def filled(tasks: Int): Inbox = {
    val inbox = new Inbox
    for _ <- 1 to tasks do inbox.add(() => ()): Unit
    inbox
  }

  test("nothing is a thief's until the worker says it has tasks to spare") {
    val inbox = filled(4)
    assertEquals(inbox.spare, 0)
    assertEquals(inbox.takeHalfInto(new Inbox, later), null)
    assertEquals(inbox.size, 4)
  }

  test("what a worker has just come to have to spare is its own for a moment, and a thief's after it") {
    val inbox = filled(4)
    inbox.declareSurplus(start)
    assertEquals(inbox.spare, 4)
    assertEquals(inbox.stealableIn(start), Inbox.StealDelayNanos)
    assertEquals(inbox.takeHalfInto(new Inbox, start), null, "taken at once")
    assertEquals(inbox.takeHalfInto(new Inbox, later - 1), null, "taken a nanosecond early")
    assertEquals(inbox.size, 4)
    assertEquals(inbox.stealableIn(later), 0L)
    val thief = new Inbox
    assert(inbox.takeHalfInto(thief, later) != null, "not taken once the moment had passed")
    assertEquals(thief.size, 1, "half of the four: one to run now, and one for the thief's inbox")
    assertEquals(inbox.size, 2)
  }

  test("tasks that are still to spare later are a thief's at once: the moment is counted from when they first were") {
    val inbox = filled(4)
    inbox.declareSurplus(start)
    inbox.declareSurplus(later) // again, as a worker does each turn it is behind
    inbox.add(() => ()): Unit
    assert(inbox.takeHalfInto(new Inbox, later) != null)
  }

  test("once a worker has nothing to spare, what it next has is its own for a moment again") {
    val inbox = filled(4)
    inbox.declareSurplus(start)
    inbox.clearSurplus()
    assertEquals(inbox.spare, 0)
    inbox.declareSurplus(later)
    assertEquals(inbox.takeHalfInto(new Inbox, later), null)
    assert(inbox.takeHalfInto(new Inbox, later + Inbox.StealDelayNanos) != null)
  }

  test("a backlog is a thief's at once, and makes what was only just to spare a thief's too") {
    val inbox = filled(4)
    inbox.declareSurplus(start)
    assertEquals(inbox.takeHalfInto(new Inbox, start), null, "taken at once")
    inbox.declareBacklog()
    assert(inbox.stealableIn(System.nanoTime()) <= 0)
    assert(inbox.takeHalfInto(new Inbox) != null)
  }
}
