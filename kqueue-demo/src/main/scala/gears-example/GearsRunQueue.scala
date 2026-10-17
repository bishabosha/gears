package gearsexample

import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicLong

/** Checks `RunQueue` under contention, run as `kqueuedemo gears runqueue`. One owner thread pushes tasks and takes some
  * itself, while other threads, the takers, move tasks from it into queues of their own and run them. Every task must
  * run exactly once, and whichever thread runs it must see the field the owner wrote before queueing it.
  */
object GearsRunQueue {

  final class Task(val id: Int, counts: AtomicIntegerArray, failures: AtomicLong) extends Runnable {
    // Written by the owner before it queues the task, read by whichever thread runs it; plain on purpose.
    var payload = 0L

    def run(): Unit = {
      if payload != id * 31L + 7 then failures.incrementAndGet()
      counts.incrementAndGet(id)
    }
  }

  def run(tasks: Int, takers: Int): Unit = {
    val counts = new AtomicIntegerArray(tasks)
    val failures = new AtomicLong
    val ran = new AtomicLong
    val moved = new AtomicLong
    val owner = new RunQueue
    @volatile var done = false

    val takerThreads = (1 to takers).map { _ =>
      val body: Runnable = () => {
        val mine = new RunQueue
        var took = 0L
        while !done do {
          took += owner.moveHalfTo(mine)
          var task = mine.pop()
          while task != null do {
            task.run()
            ran.incrementAndGet()
            task = mine.pop()
          }
        }
        moved.addAndGet(took)
      }
      new Thread(body)
    }
    takerThreads.foreach(_.start())

    val start = System.nanoTime()
    var next = 0
    // The owner queues tasks three at a time and runs one, so the queue stays busy without overflowing for long.
    while next < tasks || !owner.isEmpty do {
      var pushed = 0
      while pushed < 3 && next < tasks do {
        val task = new Task(next, counts, failures)
        task.payload = next * 31L + 7
        if owner.push(task) then {
          next += 1
          pushed += 1
        } else pushed = 3 // full: run one before trying again
      }
      val task = owner.pop()
      if task != null then {
        task.run()
        ran.incrementAndGet()
      }
    }
    while ran.get() < tasks do Thread.onSpinWait() // the takers finish what they took
    done = true
    takerThreads.foreach(_.join())
    val seconds = (System.nanoTime() - start) / 1e9

    var wrong = 0
    var i = 0
    while i < tasks do {
      if counts.get(i) != 1 then {
        if wrong < 10 then System.err.println(s"runqueue: task $i ran ${counts.get(i)} times")
        wrong += 1
      }
      i += 1
    }
    println(
      f"runqueue: $tasks tasks, ${moved.get()} moved by $takers takers, in $seconds%.2f s; " +
        s"${wrong + failures.get()} failures"
    )
    if wrong != 0 || failures.get() != 0 then sys.exit(1)
    if moved.get() == 0 then {
      System.err.println("runqueue: nothing was moved, so the check proves little")
      sys.exit(1)
    }
  }
}
