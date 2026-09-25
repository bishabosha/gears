package gearsexample

import gears.async.Async
import gears.async.Future
import gears.async.default.given

import java.util.concurrent.atomic.AtomicReference

import asyncio.Completion
import asyncio.Reactor

/** Deliberately thin glue between gears futures and a reactor, written only against the `asyncio` interface. Each
  * completion step becomes a gears future for one op's outcome.
  *
  * A reactor is single-threaded, while gears futures run on a fork-join pool, so every call into the reactor is hopped
  * onto its thread with one of its promises, which may be completed from any thread.
  */
object ReactorFutures {

  /** Runs `body` on the reactor thread. */
  def onLoop(r: Reactor)(body: => Unit): Unit = {
    val hop = r.ops.promise()
    r.submit(hop, _ => body)
    hop.complete(())
  }

  /** Submits `op`, which must have been built by `r.ops`, and returns a future that completes when the op does. The
    * future is linked to the caller's group, so cancelling the caller, or the future itself, asks the reactor to cancel
    * the op. The future is rejected as cancelled when the reactor confirms it, which also happens if the reactor closes
    * first; if the op finished in the meantime, the future gets its result instead.
    */
  def submit(r: Reactor, op: r.Op)(using Async): Future[Unit] =
    Future
      .withResolver[Unit] { resolver =>
        resolver.onCancel(() => onLoop(r)(r.cancel(op)))
        onLoop(r) {
          try
            r.submit(
              op,
              new Completion[r.Op] {
                def onComplete(op: r.Op): Unit = resolver.resolve(())
                override def onFailure(op: r.Op, failure: Throwable): Unit = resolver.reject(failure)
                override def onCancel(op: r.Op): Unit = resolver.rejectAsCancelled()
              }
            )
          catch case t: Throwable => resolver.reject(t)
        }
      }
      .link()

  /** Runs `program` in gears on its own thread while the calling thread runs the reactor. The reactor stops when the
    * program ends, and a failure of the program is rethrown here.
    */
  def run(r: Reactor)(program: Async.Spawn ?=> Unit): Unit = {
    val failure = new AtomicReference[Throwable | Null](null)
    val thread = new Thread(
      () =>
        try Async.blocking(program)
        catch case t: Throwable => failure.set(t)
        finally onLoop(r)(r.stop()),
      "gears-program"
    )
    thread.setDaemon(true) // If the reactor itself fails, the process must still be able to exit.
    thread.start()
    r.run()
    thread.join()
    val t = failure.get()
    if t != null then throw t
  }
}
