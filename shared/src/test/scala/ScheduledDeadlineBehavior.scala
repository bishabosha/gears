import gears.async.*
import gears.async.default.given

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class ScheduledDeadlineBehavior extends munit.FunSuite {
  private final class Entry(val body: Runnable) extends Cancellable {
    var cancelled = false
    def cancel(): Unit = cancelled = true
  }

  // Deliberately permits firing a cancelled callback, as a scheduler that already dispatched it may do.
  private final class ManualScheduler(delegate: Scheduler) extends Scheduler {
    val entries = UnboundedChannel[Entry]()
    val scheduled = new AtomicInteger()
    def execute(body: Runnable): Unit = delegate.execute(body)
    def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
      assert(delay.length >= 0)
      val entry = new Entry(body)
      scheduled.incrementAndGet()
      entries.sendImmediately(entry)
      entry
    }
  }

  test("later moves coalesce without cancellation or new registrations; the old callback rechecks") {
    Async.fromSync {
      val scheduler = new ManualScheduler(summon[Async].scheduler)
      val timer = new ScheduledDeadline(System.nanoTime() - 1, scheduler)
      val waiter = Future(timer.await())
      val first = scheduler.entries.read().toOption.get
      val later = System.nanoTime() + 60.seconds.toNanos
      for i <- 0 until 1000 do timer.moveTo(later + i)
      assertEquals(scheduler.scheduled.get(), 1)
      assert(!first.cancelled)
      first.body.run()
      val second = scheduler.entries.read().toOption.get
      assertEquals(scheduler.scheduled.get(), 2)
      assert(waiter.poll().isEmpty)
      timer.moveTo(System.nanoTime() - 1)
      val last = scheduler.entries.read().toOption.get
      assert(second.cancelled)
      last.body.run()
      waiter.await
    }
  }

  test("earlier moves replace the registration and a dispatched stale callback cannot complete the wait") {
    Async.fromSync {
      val scheduler = new ManualScheduler(summon[Async].scheduler)
      val timer = new ScheduledDeadline(System.nanoTime() + 120.seconds.toNanos, scheduler)
      val waiter = Future(timer.await())
      val first = scheduler.entries.read().toOption.get
      timer.moveTo(System.nanoTime() + 60.seconds.toNanos)
      val second = scheduler.entries.read().toOption.get
      assert(first.cancelled)
      first.body.run()
      assertEquals(scheduler.scheduled.get(), 2, "stale callback scheduled more work")
      assert(waiter.poll().isEmpty)
      timer.moveTo(System.nanoTime() - 1)
      val last = scheduler.entries.read().toOption.get
      second.body.run()
      last.body.run()
      waiter.await
    }
  }

  test("cancellation retracts the registration before return; stale callbacks cannot affect the next wait") {
    Async.fromSync {
      val scheduler = new ManualScheduler(summon[Async].scheduler)
      val timer = new ScheduledDeadline(System.nanoTime() + 60.seconds.toNanos, scheduler)
      val firstWait = Future(timer.await())
      val first = scheduler.entries.read().toOption.get
      firstWait.cancel()
      assert(firstWait.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException]))
      assert(first.cancelled)
      timer.moveTo(System.nanoTime() - 1)
      val secondWait = Future(timer.await())
      val second = scheduler.entries.read().toOption.get
      first.body.run()
      assert(secondWait.poll().isEmpty)
      second.body.run()
      secondWait.await
    }
  }

  test("a second waiter is refused without changing the first wait") {
    Async.fromSync {
      val scheduler = new ManualScheduler(summon[Async].scheduler)
      val timer = new ScheduledDeadline(System.nanoTime() + 60.seconds.toNanos, scheduler)
      val waiter = Future(timer.await())
      scheduler.entries.read()
      val duplicate = Future(timer.await())
      assert(duplicate.awaitResult.failed.toOption.exists(_.isInstanceOf[IllegalArgumentException]))
      timer.moveTo(System.nanoTime() - 1)
      scheduler.entries.read().toOption.get.body.run()
      waiter.await
    }
  }
}
