package gears.async

import java.util.concurrent.atomic.AtomicReference

/** What `Async.park` waits on where an `Async` has nothing cheaper: a source with one item, there once it has been
  * run.
  */
private[async] final class ParkGate extends Async.Source[Unit], Runnable {
  private val state = new AtomicReference[AnyRef | Null](null)

  def run(): Unit = state.getAndSet(ParkGate.Open) match {
    case waiting: Listener[?] => waiting.asInstanceOf[Listener[Unit]].completeNow((), this): Unit
    case _                    => ()
  }

  def poll(k: Listener[Unit]): Boolean = (state.get() eq ParkGate.Open) && k.completeNow((), this)

  def onComplete(k: Listener[Unit]): Unit =
    if !state.compareAndSet(null, k) then k.completeNow((), this): Unit

  def dropListener(k: Listener[Unit]): Unit = state.compareAndSet(k, null): Unit
}

private[async] object ParkGate {
  private val Open = new AnyRef
}
