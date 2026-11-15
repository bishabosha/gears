package gears.async.io
package node

import scala.scalajs.js

/** Node's globals that Scala.js has no facade for. */
private[node] object Node {

  /** Calls `body` on the event loop's next turn, after the I/O events of this one, and returns what cancels it. */
  def setImmediate(body: js.Function0[Unit]): js.Dynamic = js.Dynamic.global.setImmediate(body)

  def clearImmediate(immediate: js.Dynamic): Unit = js.Dynamic.global.clearImmediate(immediate): Unit
}
