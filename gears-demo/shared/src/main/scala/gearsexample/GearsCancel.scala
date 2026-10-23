package gearsexample

import gears.async.Future
import gears.async.Future.Promise

import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Failure

import asyncio.BlockingBackend
import asyncio.PipeBackend
import asyncio.ReactorBackend
import asyncio.ReadinessBackend
import asyncio.Completion
import asyncio.Interest
import asyncio.Slot
import asyncio.testkit.BufferArena
import DemoIO.text
import asyncio.gears.ReactorFutures

/** Mirror of `example.KQueueExampleCancel`. Cancellation is just `Future.cancel()`: the bridge asks the reactor to
  * cancel the op, and the future is rejected when the reactor confirms through the completion's `onCancel`. Cancelling
  * in the reactor is also what frees the read slot for the fresh read afterwards. The reactor has no promise op here:
  * code written against `Reactor` uses gears promises, which do not involve the reactor.
  */
object GearsCancel {

  /** A blocking body that sleeps for `millis`, recording whether cancellation interrupted it. */
  private def sleeper(millis: Long, interrupted: AtomicBoolean): () => Unit = () => {
    try Thread.sleep(millis)
    catch {
      case e: InterruptedException =>
        interrupted.set(true)
        throw e
    }
  }

  /** A blocking body that keeps running for `millis` whatever happens, like a native call that cannot be interrupted.
    */
  private def stubborn(millis: Long, finished: AtomicBoolean): () => String = () => {
    val deadline = System.nanoTime() + millis * 1_000_000L
    while System.nanoTime() < deadline do ()
    finished.set(true)
    "finished"
  }

  /** Waits up to two seconds for `interrupted` to be set. */
  private def interruptedSoon(interrupted: AtomicBoolean): Boolean = {
    val deadline = System.nanoTime() + 2_000_000_000L
    while !interrupted.get() && System.nanoTime() < deadline do Thread.sleep(10)
    interrupted.get()
  }

  private def isCancelled(future: Future[?]): Boolean =
    future.poll().exists {
      case Failure(_: CancellationException) => true
      case _                                 => false
    }

  def run()(using ReactorBackend & ReadinessBackend & BlockingBackend & PipeBackend): Unit = {
    val leftoverInterrupted = new AtomicBoolean() // its task is still running when the program ends
    val leftoverNotified = new AtomicBoolean() // set by its completion's onCancel when the program's group is cancelled
    val leftoverStopped = new AtomicBoolean() // whether its task had stopped by then
    BufferArena.scoped { arena =>
      val buf = arena.allocate(64)
      ReactorFutures.run { r =>
        // Everything below runs on the reactor's thread, so ops go straight to the reactor.
        val (readFd, writeFd) = r.handles.pipe()
        try {
          val sleeperInterrupted = new AtomicBoolean()
          val read = r.ops.ready(readFd, Interest.Read)
          val blocking = r.ops.blocking(sleeper(1000, sleeperInterrupted))
          val pending = Seq(
            "read" -> Future(r.perform(read)), // nothing has been written yet
            "blocking" -> Future(r.perform(blocking)),
            "timer" -> Future(r.perform(r.ops.timer(300)))
          )
          // Submitted straight to the reactor, with no future: the submission is a member of the program's group all
          // the same, so the program's end cancels it, as it cancels the futures still running, and waits for it.
          r.submit(
            r.ops.blocking(sleeper(10000, leftoverInterrupted)),
            new Completion[r.Op] {
              def onComplete(op: r.Op): Unit = println("unexpected completion of the leftover task")
              override def onCancel(op: r.Op): Unit = {
                leftoverNotified.set(true)
                leftoverStopped.set(leftoverInterrupted.get())
              }
            }
          )
          // Cancelled while it runs; it ignores the interrupt, so the future is rejected only once it has finished, and
          // the value it produces after the cancel must not reach its slot.
          val stubbornFinished = new AtomicBoolean()
          val stubbornResult = Slot[String]()
          val stubbornTask = Future(r.perform(r.ops.blocking(stubborn(300, stubbornFinished), stubbornResult)))
          println("submitted a read, a blocking task, and a timer")

          r.perform(r.ops.timer(100))
          stubbornTask.cancel()
          for (name, future) <- pending do {
            future.cancel()
            future.awaitResult // rejected once the reactor confirms the cancel, through the completion's onCancel
            println(s"$name cancelled: ${isCancelled(future)}")
          }

          // A blocking op is not Repeatable, so the reactor refuses it a second time, which `perform` throws.
          val refused =
            try {
              r.perform(blocking)
              false
            } catch case _: IllegalStateException => true
          println(s"resubmitting the cancelled blocking task: ${if refused then "refused" else "accepted"}")

          // Closing a handle fails the ops pending on it, on whichever reactor (`Handles.close`), so a future waiting on
          // one is rejected by that failure, with nothing left for a cancel to do.
          val (orphanRead, orphanWrite) = r.handles.pipe()
          val reading = Promise[Unit]()
          val orphan = Future {
            reading.complete(scala.util.Success(()))
            r.perform(r.ops.read(orphanRead, arena.allocate(16)))
          }
          // Lets the future start and submit its read before the pipe is closed: on a pool's worker, the read is pending
          // once the future has started; on the JVM's own virtual threads, it is handed to the reactor's thread first,
          // which the timer gives time for.
          reading.await
          r.perform(r.ops.timer(20))
          r.perform(r.ops.close(orphanRead))
          r.perform(r.ops.close(orphanWrite))
          val closedFailed = orphan.awaitResult.failed.toOption.exists(_.isInstanceOf[java.io.IOException])
          orphan.cancel() // nothing left to cancel
          println(s"closing fails a pending read: $closedFailed")

          stubbornTask.awaitResult
          println(s"a running blocking task had stopped before onCancel: ${stubbornFinished.get()}")
          println(s"the cancelled task left its slot empty: ${DemoIO.isEmpty(stubbornResult)}")

          // Data arrives only now; the cancelled read must not see it, and its slot takes a fresh read.
          r.handles.writeNow(writeFd, arena.of("hello".getBytes(StandardCharsets.UTF_8)))
          buf.clear()
          r.perform(r.ops.read(readFd, buf))
          buf.flip()
          println(s"read after cancel: `${text(buf)}`")

          // Past the cancelled timer's deadline, with the interrupted sleeper long stopped.
          r.perform(r.ops.timer(500))
          val late = pending.count((_, future) => !isCancelled(future))
          println(s"blocking interrupted: ${sleeperInterrupted.get()}")
          println(s"cancelled completions run: $late")
          // Each future can only have been rejected by its completion's onCancel.
          println(s"cancel notifications: ${pending.count((_, future) => isCancelled(future))}")
        } finally {
          r.perform(r.ops.close(readFd))
          r.perform(r.ops.close(writeFd))
        }
      }
    }
    println(s"the program's end notified the leftover: ${leftoverNotified.get()}")
    println(s"the leftover had stopped when notified: ${leftoverStopped.get()}")
    println(s"blocking interrupted by the program's end: ${interruptedSoon(leftoverInterrupted)}")
  }
}
