package gearsexample

import gears.async.Future

import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.scalanative.unsafe.Zone
import scala.util.Failure

import asyncio.Completion
import asyncio.Interest
import asyncio.Reactor
import asyncio.unsafe.NativeBuffer
import example.KQueueExampleIO.text
import ReactorFutures.*

/** Mirror of `example.KQueueExampleCancel`. Cancellation is just `Future.cancel()`: the bridge asks the reactor to
  * cancel the op, and the future is rejected when the reactor confirms through the completion's `onCancel`. Cancelling
  * in the reactor is also what frees the read slot for the fresh read afterwards.
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

  def run()(using Reactor.Factory[Reactor]): Unit = {
    val leftoverInterrupted = new AtomicBoolean() // its task is still running when the reactor closes
    val leftoverNotified = new AtomicBoolean() // set by its completion's onCancel when the reactor closes
    Reactor.scoped { r =>
      val (readFd, writeFd) = r.handles.pipe()
      try {
        Zone.acquire { implicit z =>
          val buf = NativeBuffer.allocate(64)
          val promise = r.ops.promise() // nobody will complete it before it is cancelled
          val sleeperInterrupted = new AtomicBoolean()
          val readPerforms = new AtomicInteger() // shows the cancelled read is never performed
          val read = r.ops.whenReady(readFd, Interest.Read) { () =>
            readPerforms.incrementAndGet()
            r.handles.readNow(readFd, buf) != 0
          }
          ReactorFutures.run(r) {
            val pending = Seq(
              "read" -> submit(r, read), // nothing has been written yet
              "blocking" -> submit(r, r.ops.blocking(sleeper(1000, sleeperInterrupted))),
              "promise" -> submit(r, promise),
              "timer" -> submit(r, r.ops.timer(300))
            )
            // Submitted straight to the reactor, with no future to cancel it: only closing the reactor can stop it.
            val leftover = r.ops.blocking(sleeper(10000, leftoverInterrupted))
            onLoop(r)(
              r.submit(
                leftover,
                new Completion[r.Op] {
                  def onComplete(op: r.Op): Unit = println("unexpected completion of the leftover task")
                  override def onCancel(op: r.Op): Unit = leftoverNotified.set(true)
                }
              )
            )
            println("submitted a read, a blocking task, a promise, and a timer")

            submit(r, r.ops.timer(100)).await
            for (name, future) <- pending do {
              future.cancel()
              future.awaitResult // rejected once the reactor confirms the cancel, through the completion's onCancel
              println(s"$name cancelled: ${isCancelled(future)}")
            }

            // A promise is not Repeatable, so the reactor refuses it a second time, failing the future.
            val refused = submit(r, promise).awaitResult.failed.toOption.exists(_.isInstanceOf[IllegalStateException])
            println(s"resubmitting the cancelled promise: ${if refused then "refused" else "accepted"}")

            // Closing a handle may remove what the reactor watches, so a read still pending on it must stay cancellable.
            // The close goes through the reactor thread too, so it lands after the read has been armed.
            val (orphanRead, orphanWrite) = r.handles.pipe()
            val orphan = submit(r, r.ops.read(orphanRead, NativeBuffer.allocate(16)))
            onLoop(r) {
              r.handles.close(orphanRead)
              r.handles.close(orphanWrite)
            }
            orphan.cancel()
            orphan.awaitResult
            println(s"cancelling after close: ${isCancelled(orphan)}")

            // Completing the cancelled promise from another thread must not run its completion.
            val completer = new Thread(() => promise.complete(()))
            completer.start()
            completer.join()

            // Data arrives only now; the cancelled read must not see it, and its slot takes a fresh read.
            r.handles.writeNow(writeFd, NativeBuffer.of("hello".getBytes(StandardCharsets.UTF_8)))
            buf.clear()
            submit(r, r.ops.read(readFd, buf)).await
            buf.flip()
            println(s"read after cancel: `${text(buf)}`")

            // Past the cancelled timer's deadline, with the interrupted sleeper long stopped.
            submit(r, r.ops.timer(500)).await
            val late = readPerforms.get() + pending.count((_, future) => !isCancelled(future))
            println(s"blocking interrupted: ${sleeperInterrupted.get()}")
            println(s"cancelled completions run: $late")
            // Each future can only have been rejected by its completion's onCancel.
            println(s"cancel notifications: ${pending.count((_, future) => isCancelled(future))}")
          }
        }
      } finally {
        r.handles.close(readFd)
        r.handles.close(writeFd)
      }
    }
    println(s"close notified the leftover: ${leftoverNotified.get()}")
    println(s"blocking interrupted by close: ${interruptedSoon(leftoverInterrupted)}")
  }
}
