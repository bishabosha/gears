package gears.async.io.kqueue

import gears.async.io.testkit.DeadlineTimers.Mode
import gears.async.{Async, AsyncOperations, AsyncSupport, Cancellable, Future, ScheduledDeadline}
import gears.io.demos.GearsQuicTimers

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class ScheduledDeadlineSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 30.seconds

  private def onPool[T](program: KqueueScheduler => Async.Spawn ?=> T): T = {
    onScheduler(new KqueueScheduler(4))(program)
  }

  private def onScheduler[T](pool: KqueueScheduler)(program: KqueueScheduler => Async.Spawn ?=> T): T = {
    given support: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given scheduler: pool.Scheduler = pool
    try Async.blocking(Future(program(pool)).await)
    finally pool.shutdown()
  }

  test("AsyncOperations dispatches to the I/O primitive while the default uses cancellable scheduler registrations") {
    val registrations = new AtomicInteger()
    val pool = new KqueueScheduler(4) {
      override def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
        registrations.incrementAndGet()
        super.schedule(delay, body)
      }
    }
    onScheduler(pool) { _ =>
      val primitive = AsyncOperations.deadlineTimer(System.nanoTime() + 5.millis.toNanos)(using pool, summon[Async])
      assertEquals(registrations.get(), 0, "building the timer started a scheduler registration")
      primitive.await()
      assertEquals(registrations.get(), 0, "the I/O override fell back to the scheduler")

      val operations: AsyncOperations = new AsyncOperations {}
      val scheduled =
        AsyncOperations.deadlineTimer(System.nanoTime() + 5.millis.toNanos)(using operations, summon[Async])
      assertEquals(registrations.get(), 0, "building the default timer started a registration")
      scheduled.await()
      assertEquals(registrations.get(), 1)
    }
  }

  for mode <- Mode.values do {
    test(s"$mode: the QUIC timer state machine acknowledges every packet and closes every idle connection") {
      onPool { pool =>
        val result = GearsQuicTimers.runInMemory(8, 101, 100.micros, mode)(using pool, summon[Async.Spawn])
        assertEquals(result.counts.packets, 808L)
        assertEquals(result.counts.closedIdle, 8)
        assert(result.counts.earlierMoves > 0)
        assert(result.counts.laterMoves > 0)
        assert(result.counts.acksOnTimer > 0)
      }
    }

    test(s"$mode: later moves survive the old deadline; earlier moves release the same wait") {
      onPool { pool =>
        val timer = mode.timer(System.nanoTime() + 30.millis.toNanos)(using pool, summon[Async])
        val waiter = Future { timer.await(); System.nanoTime() }
        pool.sleep(5)
        val later = System.nanoTime() + 100.millis.toNanos
        timer.moveTo(later)
        pool.sleep(50)
        assert(waiter.poll().isEmpty, "completed at a superseded deadline")
        val due = System.nanoTime()
        timer.moveTo(due)
        val returned = waiter.await
        assert(returned >= due)
        assert(returned - due < 5.seconds.toNanos)
        timer.moveTo(System.nanoTime() - 1)
        timer.await()
      }
    }

    test(s"$mode: a cancelled wait can be reused while old callbacks become due") {
      onPool { pool =>
        val timer = mode.timer(System.nanoTime() + 30.millis.toNanos)(using pool, summon[Async])
        val waiter = Future(timer.await())
        pool.sleep(5)
        waiter.cancel()
        assert(waiter.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException]))
        val later = System.nanoTime() + 80.millis.toNanos
        timer.moveTo(later)
        val next = Future { timer.await(); System.nanoTime() }
        val returned = next.await
        assert(returned >= later)
      }
    }
  }

  test("concurrent external movers preserve the last deadline and release the pending scheduled wait") {
    onPool { pool =>
      val timer = new ScheduledDeadline(System.nanoTime() + 120.seconds.toNanos, pool)
      val waiting = Future(timer.await())
      pool.sleep(5)
      val movers = (0 until 4).map { n =>
        new Thread(() => {
          for i <- 0 until 2000 do timer.moveTo(System.nanoTime() + (60.seconds.toNanos + n * 2000 + i))
        })
      }
      movers.foreach(_.start())
      movers.foreach(_.join())
      assert(waiting.poll().isEmpty)
      val last = System.nanoTime()
      timer.moveTo(last)
      waiting.await
      assertEquals(timer.deadline, last)
    }
  }
}
