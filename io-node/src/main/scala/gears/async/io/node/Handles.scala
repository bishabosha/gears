package gears.async.io
package node

import java.nio.channels.ClosedChannelException

/** The failure of an op on a closed handle, naming it: a closed channel's, as `Handles.close` has it. */
final class HandleClosedException(handle: AnyRef) extends ClosedChannelException {
  override def getMessage: String = s"$handle is closed"
}

/** A handle of the Node.js backend: an I/O object of Node's, and the ops in progress on it, which a close fails before
  * it closes the object, waiting for those Node cannot take back, so that no op uses the object once closed.
  */
abstract class NodeHandle private[node] (kind: String) {
  private val id = NodeHandle.next()
  private var closing = false
  private val runs = scala.collection.mutable.LinkedHashSet.empty[Run]

  /** Takes up an op performed on this handle, which a close stops; fails if the handle is closed. */
  private[node] def enter(run: Run): Unit = {
    checkOpen()
    runs += run
  }

  /** Lets go of an op taken up by `enter`. */
  private[node] def leave(run: Run): Unit = runs -= run

  /** Throws the failure of an op on this handle if it has been closed. */
  private[node] def checkOpen(): Unit = if closing then throw new HandleClosedException(this)

  /** Closes this handle as `closer`'s op: fails the ops in progress on it, waits for those that are held, then closes
    * its I/O object, and finishes `closer` once that is closed. Fails if the handle was closed already. A close once
    * begun finishes, whatever stops its caller.
    */
  private[node] def close(closer: Run): Unit = {
    checkOpen()
    closing = true
    closer.hold()
    var pending = 0
    def proceed(): Unit =
      try shut(() => closer.succeed())
      catch case failure: Throwable => closer.fail(failure)
    for run <- runs.toList if !run.done do {
      run.stop(new HandleClosedException(this))
      if !run.done then {
        pending += 1
        run.finishing = () => {
          pending -= 1
          if pending == 0 then proceed()
        }
      }
    }
    if pending == 0 then proceed()
  }

  /** Closes the I/O object, calling `done` once it is closed. */
  protected def shut(done: () => Unit): Unit

  override def toString: String = s"$kind #$id"
}

private[node] object NodeHandle {
  private var ids = 0L

  private def next(): Long = {
    ids += 1
    ids
  }
}
