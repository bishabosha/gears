package gears.async.io
package node

/** The Node.js backend's pool: Node's streams hand the backend chunks of their own, so a pool allocates nothing, and
  * counts the chunks held.
  */
private[node] final class NodePool(val bufferSize: Int, val buffers: Int) extends BufferPool {
  require(bufferSize > 0 && buffers > 0, s"A pool has buffers of some size: $buffers of $bufferSize bytes")
  private var held = 0
  private var closed = false

  def available: Int = buffers - held

  def close(): Unit =
    if !closed then {
      if held > 0 then throw new IllegalArgumentException(s"$held of the pool's buffers are still held")
      closed = true
    }
}
