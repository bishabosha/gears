package gears.async.io.internal

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}

class PoolWaitSuite extends munit.FunSuite {
  private class Pool extends FreeListPool(8, 1) {
    type S = Slot
    fill(i => new Slot(i, ByteBuffer.allocate(bufferSize)))
    protected def freeMemory(): Unit = ()
  }

  test("registration observes a buffer returned after failed acquisition") {
    val pool = new Pool
    val held = pool.take().nn
    val calls = new AtomicInteger
    val wait = pool.newWaiter(() => { calls.incrementAndGet(); () })
    assertEquals(pool.take(), null)
    held.release()
    pool.enqueue(wait)
    assertEquals(calls.get(), 1)
    assertEquals(pool.available, 0)
    wait.cancel()
    assertEquals(pool.available, 1)
    pool.close()
  }

  test("FIFO handoff skips cancelled waiters and returns a cancelled grant") {
    val pool = new Pool
    val held = pool.take().nn
    val notified = new java.util.ArrayList[Int]()
    val a = pool.newWaiter(() => { notified.add(1); () })
    val b = pool.newWaiter(() => { notified.add(2); () })
    val c = pool.newWaiter(() => { notified.add(3); () })
    pool.enqueue(a)
    pool.enqueue(b)
    pool.enqueue(c)
    b.cancel()
    held.release()
    assertEquals(notified.toString, "[1]")
    assertEquals(pool.take(), null)
    a.cancel()
    assertEquals(notified.toString, "[1, 3]")
    assertEquals(pool.available, 0)
    val received = c.consume().nn
    c.cancel() // consumed ownership has passed to the reader
    assertEquals(pool.available, 0)
    received.release()
    assertEquals(pool.available, 1)
    pool.close()
  }

  test("cancellation before registration cannot retain a buffer") {
    val pool = new Pool
    val wait = pool.newWaiter(() => fail("cancelled registration was notified"))
    wait.cancel()
    pool.enqueue(wait)
    assertEquals(pool.available, 1)
    pool.close()
  }

  for cancel <- List(false, true) do
    test(s"concurrent release and registration have no lost grants (cancel: $cancel)") {
      val pool = new Pool
      val executor = Executors.newSingleThreadExecutor()
      try {
        for _ <- 0 until 1000 do {
          val held = pool.take().nn
          val gate = new CyclicBarrier(2)
          val calls = new AtomicInteger
          val wait = pool.newWaiter(() => { calls.incrementAndGet(); () })
          val release = executor.submit(new Callable[Unit] {
            def call(): Unit = { gate.await(5, TimeUnit.SECONDS); held.release() }
          })
          gate.await(5, TimeUnit.SECONDS)
          pool.enqueue(wait)
          if cancel then wait.cancel()
          release.get(5, TimeUnit.SECONDS)
          if !cancel then {
            assertEquals(calls.get(), 1)
            val granted = wait.consume()
            assert(granted != null)
            granted.nn.release()
          }
          assertEquals(pool.available, 1)
        }
      } finally {
        executor.shutdownNow()
        pool.close()
      }
    }
}
