package gears.async.io.kqueue

import gears.async.Future.Promise
import gears.async.io.testkit.DeadlineTimers.Mode
import gears.async.{Async, AsyncSupport, Future}
import gears.io.demos.GearsQuicTimers

import scala.concurrent.duration.*
import scala.util.Success

/** Release-mode, paired measurements of pending deadline updates and the QUIC demo's server state machine. Arguments:
  * moves per sample (100000), measured samples (7), packets per QUIC connection (1001).
  */
object DeadlineTimerBench {
  private def onPool[T](workers: Int)(program: KqueueScheduler => Async.Spawn ?=> T): T = {
    val pool = new KqueueScheduler(workers)
    given support: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given scheduler: pool.Scheduler = pool
    try {
      val result = Async.blocking {
        Async.withNewCompletionGroup(pool.rootGroup)(Future(program(pool)).awaitResult)
      }
      val fatal = pool.fatalFailure
      if fatal != null then throw fatal
      result.get
    } finally pool.shutdown()
  }

  // Each worker processes a FIFO sentinel after the timer futures already submitted to it. After all sentinels
  // complete, every timer future has installed its wait, even with one worker. No guessed settling sleep is needed.
  private def barrier(workers: Int)(using async: Async): Unit = {
    val sentinels = (0 until workers).map { _ =>
      val done = Promise[Unit]()
      async.scheduler.execute(() => done.complete(Success(())))
      done
    }
    sentinels.foreach(_.await)
  }

  private def moves(pool: KqueueScheduler, workers: Int, mode: Mode, earlier: Boolean, size: Int, count: Int)(using
      Async.Spawn
  ): Double = {
    val base = System.nanoTime() + 1.hour.toNanos
    val deadlines = Array.tabulate(size)(i => base + i * 1000L)
    val timers = deadlines.map(at => mode.timer(at)(using pool, summon[Async]))
    val waiters = timers.map(timer => Future(timer.await()))
    barrier(workers)
    require(waiters.forall(_.poll().isEmpty), "a benchmark timer completed before measurement")
    val step = if earlier then -1000L else 1000L
    val start = System.nanoTime()
    var i = 0
    while i < count do {
      val index = i % size
      deadlines(index) += step
      timers(index).moveTo(deadlines(index))
      i += 1
    }
    val elapsed = System.nanoTime() - start
    timers.indices.foreach(i => require(timers(i).deadline == deadlines(i)))
    require(waiters.forall(_.poll().isEmpty), "a benchmark timer completed during measurement")
    waiters.foreach(_.cancel())
    waiters.foreach(_.awaitResult)
    elapsed.toDouble / count
  }

  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    val n = sorted.size
    if n % 2 == 0 then (sorted(n / 2 - 1) + sorted(n / 2)) / 2 else sorted(n / 2)
  }

  def main(args: Array[String]): Unit = {
    val count = args.headOption.fold(100000)(_.toInt)
    val samples = args.lift(1).fold(7)(_.toInt)
    val packets = args.lift(2).fold(1001)(_.toInt)
    require(count > 0 && samples > 0 && packets > 0)
    println(
      s"deadline-bench: moves=$count samples=$samples warmups=2 quicPackets=$packets processors=${Runtime.getRuntime.availableProcessors()}"
    )
    println("kind,workers,resident,direction,mode,sample,ns_per_move")
    for workers <- Seq(1, 4) do
      onPool(workers) { pool =>
        for size <- Seq(1, 256); earlier <- Seq(false, true) do {
          val direction = if earlier then "earlier" else "later"
          val results = Mode.values.map(_ => scala.collection.mutable.ArrayBuffer.empty[Double])
          for sample <- -2 until samples do {
            val order = if sample % 2 == 0 then Mode.values.toSeq else Mode.values.reverse.toSeq
            for mode <- order do {
              val ns = moves(pool, workers, mode, earlier, size, count)
              if sample >= 0 then {
                results(mode.ordinal) += ns
                println(f"moves,$workers,$size,$direction,$mode,$sample,$ns%.3f")
              }
            }
          }
          val primitive = median(results(Mode.Primitive.ordinal).toSeq)
          val scheduled = median(results(Mode.Scheduled.ordinal).toSeq)
          println(f"summary,$workers,$size,$direction,Primitive,median,$primitive%.3f")
          println(f"summary,$workers,$size,$direction,Scheduled,median,$scheduled%.3f")
          println(f"ratio,$workers,$size,$direction,Scheduled/Primitive,median,${scheduled / primitive}%.3f")
        }
        println(
          "kind,workers,connections,interval_us,mode,sample,active_ms,total_ms,moves,fires,acks_on_timer,earlier_moves,later_moves"
        )
        for interval <- Seq(Duration.Zero, 100.micros) do {
          for sample <- -1 until math.min(samples, 5) do {
            val order = if sample % 2 == 0 then Mode.values.toSeq else Mode.values.reverse.toSeq
            for mode <- order do {
              val result = GearsQuicTimers.runInMemory(32, packets, interval, mode)(using pool, summon[Async.Spawn])
              val counts = result.counts
              if sample >= 0 then
                println(
                  f"quic,$workers,32,${interval.toMicros},$mode,$sample,${result.activeNanos / 1e6}%.3f,${result.totalNanos / 1e6}%.3f,${counts.moves},${counts.fires},${counts.acksOnTimer},${counts.earlierMoves},${counts.laterMoves}"
                )
            }
          }
        }
      }
  }
}
