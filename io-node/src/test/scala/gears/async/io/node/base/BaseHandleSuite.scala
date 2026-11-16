package gears.async.io.node.base

import java.io.IOException

/** A handle's state on its own: work claimed, waiters, and a close with one owner. */
class BaseHandleSuite extends munit.FunSuite {

  private final class Closed extends IOException("closed")

  private final class Handle(closesAtOnce: Boolean = true) extends BaseHandle with OneWaiterEachWay {
    var events = List.empty[String]
    protected def closedFailure(): IOException = new Closed
    protected def cleanup(): Boolean = {
      events ::= "cleanup"
      closesAtOnce
    }
    override protected def failWaiters(): Unit = {
      events ::= "waiters failed"
      super.failWaiters()
    }
    def cleanedUp(): Unit = completeClose()
    def ready(read: Boolean): Unit = readyWaiter(read)
  }

  private final class Owner extends CloseListener {
    var told = 0
    def handleClosed(): Unit = told += 1
  }

  /** A waiter that finds its handle ready after `blocks` wake-ups that did not. */
  private final class Waiter(var blocks: Int = 0) extends ReadinessOp {
    var done = false
    var closed = false
    var claimed = false
    def settled: Boolean = done || closed
    def claimAtReady(): Boolean = {
      val free = !settled && !claimed
      if free then claimed = true
      free
    }
    def readyWakeup(): Boolean = {
      if blocks == 0 then done = true else blocks -= 1
      done
    }
    def releaseReady(): Boolean = {
      claimed = false
      true
    }
    def handleClosing(): Unit = closed = true
  }

  test("a handle with no work claimed closes at once, and a second close fails as any op on it does") {
    val handle = new Handle
    assert(handle.isOpen)
    assert(handle.requestClose())
    assert(!handle.isOpen)
    intercept[Closed](handle.requestClose())
    intercept[Closed](handle.claimWork())
    intercept[Closed](handle.checkOpen())
    assert(handle.failureIfClosed.isInstanceOf[Closed])
    assertEquals(handle.events.reverse, List("waiters failed", "cleanup"))
  }

  test("work claimed keeps the handle's object open: the last of it released closes it, and tells the owner once") {
    val handle = new Handle
    val owner = new Owner
    handle.claimWork()
    handle.claimWork()
    assert(!handle.requestClose(), "work is still under way")
    handle.onClosed(owner)
    intercept[Closed](handle.claimWork()) // no new work once it is closing
    handle.releaseWork()
    assertEquals(owner.told, 0)
    assert(!handle.events.contains("cleanup"))
    handle.releaseWork()
    assertEquals(owner.told, 1)
    assertEquals(handle.events.reverse, List("waiters failed", "cleanup"))
    intercept[IllegalStateException](handle.releaseWork())
  }

  test("an object that closes in its own time tells the owner when it has, at once if it already had") {
    val handle = new Handle(closesAtOnce = false)
    val owner = new Owner
    assert(!handle.requestClose())
    handle.onClosed(owner)
    intercept[IllegalStateException](handle.onClosed(new Owner)) // a close has one owner
    assertEquals(owner.told, 0)
    handle.cleanedUp()
    handle.cleanedUp()
    assertEquals(owner.told, 1)
    val late = new Owner
    handle.onClosed(late)
    assertEquals(late.told, 1)
  }

  test("one op waits each way: a second is refused, one that has settled gives way, and a close fails both") {
    val handle = new Handle
    val reader = new Waiter
    val writer = new Waiter
    handle.awaitReadiness(read = true, reader)
    handle.awaitReadiness(read = false, writer)
    intercept[IllegalArgumentException](handle.awaitReadiness(read = true, new Waiter))
    reader.done = true // settled, not yet cleared by its performer
    val next = new Waiter
    handle.awaitReadiness(read = true, next)
    assert(!handle.clearWaiter(read = true, reader), "it is no longer the waiter")
    handle.requestClose()
    assert(next.closed && writer.closed)
  }

  test("a handle found ready wakes its waiter, which waits on if it was not ready after all") {
    val handle = new Handle
    val waiter = new Waiter(blocks = 2)
    handle.awaitReadiness(read = true, waiter)
    handle.ready(read = false) // nobody waits to write
    handle.ready(read = true)
    handle.ready(read = true)
    assert(!waiter.done && !waiter.claimed, "its claim was given back each time")
    handle.ready(read = true)
    assert(waiter.done)
    assert(handle.clearWaiter(read = true, waiter))
  }

  test("ops waiting in a queue are woken in order, for as long as each finds the handle ready") {
    val queue = new WaitQueue
    val first = new Waiter
    val gone = new Waiter
    val blocked = new Waiter(blocks = 1)
    val last = new Waiter
    List(first, gone, blocked, last).foreach(queue.add)
    gone.done = true // settled some other way, and not yet taken out
    queue.ready()
    assert(first.done, "the first was ready")
    assert(!blocked.done && !last.done, "the next was not, so those behind it wait their turn")
    queue.ready()
    assert(blocked.done && last.done)
    val late = new Waiter
    queue.add(late)
    assert(queue.remove(late))
    assert(!queue.remove(late))
    val failed = new Waiter
    queue.add(failed)
    queue.failAll()
    assert(failed.closed)
  }
}
