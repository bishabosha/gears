package asyncio.nio

import gears.async.Async

import java.util.concurrent.CancellationException

import asyncio.ReadinessBackend
import asyncio.UnixStreamBackend

/** NIO's non-blocking channels with no reactor: `perform` runs the op on the caller's thread, which must be a virtual
  * thread, as gears' futures on the JVM are. An op tries its operation, and while that would block, yields and tries
  * again, then waits on a selector of its handle's own, selecting in the virtual thread, which the JDK parks until the
  * selector is ready (see `VThreadOp`). The handles are `NioHandles`', so a handle may be
  * used with either backend.
  *
  * Cancelling the caller wakes the op's wait with `Selector.wakeup`, or unparks a timer, and the op throws
  * `CancellationException` with the handle left open, as on a reactor; nothing is interrupted.
  *
  * Besides what every backend has, it has readiness, its ops waiting on the handles' readiness themselves, and Unix
  * stream sockets. It has no blocking ops: a caller that may block calls the blocking code itself.
  */
object VThreadBackend extends ReadinessBackend with UnixStreamBackend {

  type Op = VThreadOp

  def ops: VThreadOps.type = VThreadOps

  def handles: NioHandles.type = NioHandles

  /** Performs `op` on the caller's thread until it is done. Throws `IllegalStateException` on a platform thread, which
    * a wait would block outright.
    */
  def perform(op: Op)(using ac: Async): Unit = {
    val thread = Thread.currentThread()
    if !thread.isVirtual then
      throw new IllegalStateException(s"VThreadBackend performs ops only on virtual threads, not on $thread")
    if ac.group.isCancelled then throw new CancellationException()
    op.perform()
  }
}
