package gears.async.io.internal

import gears.async.io.{Pipes, Slot}
import gears.async.{AsyncOperations, Future}

/** Diagnostic only: compare warm medians on the same machine; keep measurements outside Git. */
private[io] trait PoolCostContract extends munit.FunSuite {
  val backend: ReactorBackend & Pipes

  test("pooled EOF acquisition and release costs") {
    val count = 200000
    for workers <- List(1, 4) do {
      val pool = backend.pool(8, workers)
      try {
        val samples = (0 until 7)
          .map { _ =>
            ReactorRuns.run(workers)(using backend) { io ?=>
              val ready = new java.util.concurrent.atomic.AtomicInteger()
              val go = new java.util.concurrent.atomic.AtomicBoolean()
              val tasks = List.fill(workers)(Future {
                val readEnd = Slot[backend.Readable]()
                val writeEnd = Slot[backend.Writable]()
                io.perform(backend.ops.pipe(readEnd, writeEnd))
                val in = readEnd.clear()
                io.perform(backend.ops.close(writeEnd.clear()))
                try {
                  val into = Slot[gears.async.io.Received]()
                  val read = backend.ops.read(in, pool, into)
                  ready.incrementAndGet()
                  while !go.get() do summon[AsyncOperations].`yield`()
                  var n = 0
                  var received = 0L
                  while n < count do {
                    io.perform(read)
                    val chunk = into.clear()
                    received += chunk.buffer.remaining()
                    chunk.release()
                    n += 1
                  }
                  assertEquals(received, 0L)
                } finally io.perform(backend.ops.close(in))
              })
              while ready.get() < workers do summon[AsyncOperations].`yield`()
              val start = System.nanoTime()
              go.set(true)
              tasks.foreach(_.await)
              (System.nanoTime() - start).toDouble / (count.toLong * workers)
            }
          }
          .drop(2)
          .sorted
        println(f"pool-cost eof workers=$workers: ${samples(samples.size / 2)}%.1f ns/op")
        assertEquals(pool.available, pool.buffers)
      } finally pool.close()
    }
  }

}
