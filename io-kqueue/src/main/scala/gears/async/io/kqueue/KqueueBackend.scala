package gears.async.io
package kqueue

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Future
import gears.async.default.DefaultSupport

import java.nio.ByteBuffer
import java.nio.channels.NetworkChannel
import scala.annotation.targetName
import scala.concurrent.duration.FiniteDuration

/** The kqueue backend
  *
  * A program runs on gears' forkjoin threadpool, and each op is attempted by the thread performing it: tried at once
  * with a non-blocking operation, and if it would block, waited for on a polling thread, which runs kqueue, and tried
  * again once woken. Timers are managed by a heap and gate how long polling blocks for. Files are managed on a blocking
  * thread-pool.
  */
object KqueueBackend
    extends Backend
    // with IpStreams
    // with IpDatagrams
    // with UnixStreams
    // with RegularFiles
    with Pipes
    // with Resolving
    // with Blocking
    {
  type Op = KqueueOp
  type Handle = KqueueHandle
  type Readable = KqueueReadable
  type Writable = KqueueWritable
  // type Connection = ConnectionHandle
  // type Listener = ListenerHandle
  // type BoundDatagram = BoundDatagramHandle
  // type ConnectedDatagram = ConnectedDatagramHandle
  // type File = FileHandle

  val ops: KqueueOps.type = KqueueOps

  def pool(bufferSize: Int, buffers: Int): BufferPool = new NativePool(bufferSize, buffers)

  /** Runs `program` on gears' virtual threads, each op performed by the future performing it. */
  private[io] def run[T](program: Io[this.type] ?=> Async.Spawn ?=> T): T = {
    val pool = KqueueScheduler(Runtime.getRuntime.availableProcessors)
    given poolSupport: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given poolScheduler: pool.Scheduler = pool
    // `Async.blocking` runs its body on the calling thread.
    try Async.blocking(Future(program(using pool)).await)
    finally pool.shutdown()
  }
}
