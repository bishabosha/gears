package gears.async.io.internal

import gears.async.io.{Pipes, Repeatable, Slot}
import gears.async.{AsyncOperations, Future}

import java.nio.ByteBuffer
import scala.concurrent.duration.*

/** Diagnostic experiment, deliberately without timing thresholds. The three representations execute the same immediate
  * reads and relative timers. Samples alternate order; warm-up samples are excluded. Keep output outside Git.
  */
private[io] trait RequestCostContract extends munit.FunSuite {
  val backend: ReactorBackend & Pipes
  protected def prepareRead(request: ReadRequest[backend.Readable]): backend.Op & Repeatable
  protected def prepareTimer(request: TimerRequest): backend.Op & Repeatable

  test("compare intrusive, prepared-description and fresh-execution costs") {
    for workers <- List(1, 4); timer <- List(false, true) do {
      val count = if timer then 10000 else 200000
      val samples = Array.fill(3)(scala.collection.mutable.ArrayBuffer.empty[Double])
      for round <- 0 until 7 do {
        val modes = if round % 2 == 0 then List(0, 1, 2) else List(2, 1, 0)
        for mode <- modes do {
          val elapsed = ReactorRuns.run(workers)(using backend) { io ?=>
            val ready = new java.util.concurrent.atomic.AtomicInteger()
            val go = new java.util.concurrent.atomic.AtomicBoolean()
            val tasks = List.fill(workers)(Future {
              val readEnd = Slot[backend.Readable]()
              val writeEnd = Slot[backend.Writable]()
              io.perform(backend.ops.pipe(readEnd, writeEnd))
              val in = readEnd.clear()
              io.perform(backend.ops.close(writeEnd.clear()))
              try {
                val buffer = ByteBuffer.allocateDirect(1)
                val request = ReadRequest(in, buffer)
                val delay = TimerRequest(Duration.Zero)
                val prepared =
                  if mode == 0 then {
                    if timer then backend.ops.timer(Duration.Zero) else backend.ops.read(in, buffer)
                  } else if timer then prepareTimer(delay)
                  else prepareRead(request)
                ready.incrementAndGet()
                while !go.get() do summon[AsyncOperations].`yield`()
                var i = 0
                while i < count do {
                  val op =
                    if mode != 2 then prepared
                    else if timer then prepareTimer(delay)
                    else prepareRead(request)
                  io.perform(op)
                  i += 1
                }
                assertEquals(buffer.position(), 0)
              } finally io.perform(backend.ops.close(in))
            })
            while ready.get() < workers do summon[AsyncOperations].`yield`()
            val start = System.nanoTime()
            go.set(true)
            tasks.foreach(_.await)
            System.nanoTime() - start
          }
          if round >= 2 then samples(mode) += elapsed.toDouble / (count.toLong * workers)
        }
      }
      val names = List("intrusive", "prepared", "fresh")
      for mode <- 0 until 3 do {
        val values = samples(mode).sorted
        println(
          f"request-cost ${if timer then "timer" else "eof"} workers=$workers ${names(mode)}: ${values(values.size / 2)}%.1f ns/op"
        )
      }
    }
  }
}
