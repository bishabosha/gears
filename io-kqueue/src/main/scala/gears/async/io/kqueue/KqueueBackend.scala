package gears.async.io
package kqueue

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Future

/** Native backend under construction. Each run has a scheduler whose workers own their timer heaps. */
object KqueueBackend extends Backend with Pipes {
  type Op = KqueueOp
  type Handle = KqueueHandle
  type Readable = KqueueReadable
  type Writable = KqueueWritable

  val ops: KqueueOps.type = KqueueOps

  def pool(bufferSize: Int, buffers: Int): BufferPool = new NativePool(bufferSize, buffers)

  private[io] def run[T](program: Io[this.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T = {
    val pool = KqueueScheduler(Runtime.getRuntime.availableProcessors)
    given support: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given pool.Scheduler = pool
    given AsyncOperations = pool
    try Async.blocking(Future(program(using pool)).await)
    finally pool.close()
  }
}
