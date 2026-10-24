package gears.async.io
package kqueue

import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import scala.scalanative.unsafe.Zone

import gears.async.io.internal.NativeBuffer

/** Closing a kqueue handle while another thread uses its descriptor: the close waits for the use in progress, and no
  * use starts after it, so that nothing a thread still writing through the stale handle sends reaches the descriptors
  * opened since, which the kernel gives the lowest numbers free, the closed ones among them.
  */
class HandleCloseSuite extends munit.FunSuite {

  test("a handle closed while another thread writes through it: nothing reaches what reuses its descriptor") {
    Zone.acquire { implicit z =>
      val rounds = 200
      var leaked = 0L
      for _ <- 1 to rounds do {
        val (in, out) = KqueueHandles.pipe()
        val started = new CountDownLatch(1)
        val stopped = new AtomicBoolean(false)
        val writes = new AtomicLong(0)
        val writer = new Thread(() => {
          val one = NativeBuffer.allocate(1)
          started.countDown()
          try
            while !stopped.get() do {
              one.clear()
              KqueueHandles.writeNow(out, one) // the stale handle's write fails once it is closed
              writes.incrementAndGet(): Unit
            }
          catch case _: IOException => () // not an open handle any more
        })
        writer.start()
        started.await()
        while writes.get() < 10 do Thread.onSpinWait() // writing for a while, the pipe filling
        KqueueHandles.close(in)
        KqueueHandles.close(out)
        // What reuses the numbers: opened while the writer may still be trying.
        val fresh = Seq.fill(4)(KqueueHandles.pipe())
        Thread.sleep(1)
        stopped.set(true)
        writer.join()
        val buf = NativeBuffer.allocate(64)
        for (freshIn, freshOut) <- fresh do {
          buf.clear()
          val got = KqueueHandles.readNow(freshIn, buf) // 0 if nothing arrived, as it should be
          if got > 0 then leaked += got
          KqueueHandles.close(freshIn)
          KqueueHandles.close(freshOut)
        }
      }
      assertEquals(leaked, 0L, s"bytes written through closed handles reached reused descriptors in $rounds rounds")
    }
  }
}
