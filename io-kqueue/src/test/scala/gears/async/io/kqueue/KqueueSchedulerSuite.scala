package gears.async.io.kqueue

import gears.async.Listener

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicIntegerArray
import scala.concurrent.duration.*

class KqueueSchedulerSuite extends munit.FunSuite {
  private def register(worker: Worker, op: KqueueTimerOp, timeout: Int = -1): TimerWait = {
    val wait = new TimerWait(worker, op, timeout)
    op.attach(wait)
    wait.requestUpdate()
    wait
  }

  test("a single worker keeps every runnable across inbox batch boundaries") {
    val worker = new Worker("test-inbox")
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val finished = new CountDownLatch(100)
    try {
      worker.execute(() => {
        entered.countDown()
        release.await()
      })
      assert(entered.await(5, TimeUnit.SECONDS))
      for _ <- 1 to 100 do worker.execute(() => finished.countDown())
      release.countDown()
      assert(finished.await(5, TimeUnit.SECONDS), finished.getCount().toString + " runnables were lost")
    } finally {
      release.countDown()
      worker.close()
    }
  }

  test("concurrent producers deliver every command exactly once while the owner drains batches") {
    val worker = new Worker("test-producers")
    val start = new CountDownLatch(1)
    val finished = new CountDownLatch(4000)
    val counts = new AtomicIntegerArray(4000)
    val producers = Array.tabulate(4) { producer =>
      new Thread(() => {
        start.await()
        for i <- 0 until 1000 do {
          val id = producer * 1000 + i
          worker.execute(() => {
            counts.incrementAndGet(id)
            finished.countDown()
          })
        }
      })
    }
    try {
      producers.foreach(_.start())
      start.countDown()
      producers.foreach(_.join(5000))
      assert(producers.forall(!_.isAlive))
      assert(finished.await(5, TimeUnit.SECONDS), finished.getCount().toString + " commands were lost")
      for i <- 0 until 4000 do assertEquals(counts.get(i), 1, s"command $i")
    } finally {
      start.countDown()
      producers.foreach(_.join())
      worker.close()
    }
  }

  test("moving a queued deadline earlier repairs its position behind another timer") {
    val worker = new Worker("test-rekey")
    val first = KqueueMakeDeadline(System.nanoTime() + 30.seconds.toNanos)
    val moved = KqueueMakeDeadline(System.nanoTime() + 60.seconds.toNanos)
    val firstWait = register(worker, first)
    val movedWait = register(worker, moved)
    val registered = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    movedWait.onComplete(Listener[TimerResult]((_, _) => completed.countDown()))
    worker.execute(() => registered.countDown()) // FIFO publication: both registrations are now in the heap.
    try {
      assert(registered.await(5, TimeUnit.SECONDS))
      moved.moveTo(System.nanoTime() - 1)
      assert(completed.await(5, TimeUnit.SECONDS), "the moved timer stayed behind the original heap head")
      assertEquals(movedWait.poll(), Some(TimerResult.Succeeded))
      assert(firstWait.poll().isEmpty)
    } finally worker.close()
  }

  test("a late worker selects the earlier deadline and lets timer completion win an exact tie") {
    for (limit, expected) <- List(
        1 -> TimerResult.TimedOut,
        20 -> TimerResult.Succeeded,
        100 -> TimerResult.Succeeded
      )
    do {
      val worker = new Worker("test-late-wakeup")
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val completed = new CountDownLatch(1)
      try {
        worker.execute(() => {
          entered.countDown()
          release.await()
        })
        assert(entered.await(5, TimeUnit.SECONDS))
        val start = System.nanoTime()
        val wait = register(worker, KqueueMakeTimer(20.millis), limit)
        wait.onComplete(Listener[TimerResult]((_, _) => completed.countDown()))
        while System.nanoTime() - start < 30.millis.toNanos do Thread.sleep(1)
        release.countDown()
        assert(completed.await(5, TimeUnit.SECONDS))
        assertEquals(wait.poll(), Some(expected))
      } finally {
        release.countDown()
        worker.close()
      }
    }
  }

  test("the owner acknowledges cancellation by removing the registration before completing it") {
    val worker = new Worker("test-cancel")
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    try {
      worker.execute(() => {
        entered.countDown()
        release.await()
      })
      assert(entered.await(5, TimeUnit.SECONDS))
      val wait = register(worker, KqueueMakeTimer(60.seconds))
      wait.onComplete(Listener[TimerResult]((_, _) => {
        assertEquals(wait.heapIndex, -1)
        completed.countDown()
      }))
      wait.cancel()
      assert(wait.poll().isEmpty, "the submitting thread acknowledged cancellation before the owner processed it")
      release.countDown()
      assert(completed.await(5, TimeUnit.SECONDS))
      assertEquals(wait.poll(), Some(TimerResult.Cancelled))
    } finally {
      release.countDown()
      worker.close()
    }
  }

  test("the indexed heap agrees with a model through growth, rekeys, removals and clock wrap") {
    val worker = new Worker("test-heap")
    val heap = new TimerQueue()
    val tasks = Array.fill(64)(new ScheduledTask(worker, 0L, () => ()))
    val model = collection.mutable.Map.empty[TimerTask, Long]
    val random = new scala.util.Random(0)
    val epoch = Long.MaxValue - 5000
    try {
      for _ <- 0 until 5000 do {
        val task = tasks(random.nextInt(tasks.length))
        if random.nextInt(3) == 0 then {
          heap.remove(task)
          model.remove(task)
          assertEquals(task.heapIndex, -1)
        } else {
          val offset = random.nextInt(10000).toLong
          heap.update(task, epoch + offset)
          model(task) = offset
        }
        assertEquals(heap.isEmpty, model.isEmpty)
        if model.nonEmpty then {
          assertEquals(heap.firstDeadline, epoch + model.values.min)
          assertEquals(model(heap.first), model.values.min)
        }
      }
      while !heap.isEmpty do {
        val first = heap.first
        heap.remove(first)
        assertEquals(first.heapIndex, -1)
      }
      assert(tasks.forall(_.heapIndex == -1))
    } finally worker.close()
  }

  test("a cleanup subscription survives a late cancellation unregistration") {
    val worker = new Worker("test-subscription")
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    try {
      worker.execute(() => {
        entered.countDown()
        release.await()
      })
      assert(entered.await(5, TimeUnit.SECONDS))
      val wait = register(worker, KqueueMakeTimer(60.seconds))
      val cancelled = Listener[TimerResult]((_, _) => fail("the cancelled subscription was retained"))
      wait.onComplete(cancelled)
      wait.cancel()
      wait.onComplete(Listener[TimerResult]((_, _) => completed.countDown()))
      wait.dropListener(cancelled)
      release.countDown()
      assert(completed.await(5, TimeUnit.SECONDS))
      assertEquals(wait.poll(), Some(TimerResult.Cancelled))
    } finally {
      release.countDown()
      worker.close()
    }
  }
}
