package gears.async.io
package node

/** A handle of the Node.js backend: an I/O object of Node's. */
abstract class NodeHandle private[node] (kind: String) {
  private val id = NodeHandle.next()

  /** Takes up an op performed on this handle; fails if the handle is closed. */
  private[node] def enter(run: Run): Unit = ()

  /** Lets go of an op taken up by `enter`. */
  private[node] def leave(run: Run): Unit = ()

  override def toString: String = s"$kind #$id"
}

private[node] object NodeHandle {
  private var ids = 0L

  private def next(): Long = {
    ids += 1
    ids
  }
}
