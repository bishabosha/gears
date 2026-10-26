package gears.async.io
package kqueue

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ReadinessContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.UnixDatagramContract
import gears.async.io.testkit.UnixStreamContract

class KqueueContractSuite
    extends ReadinessContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract {
  val backend: KqueueBackend.type = KqueueBackend

  // macOS tells neither kqueue nor Dispatch, nor poll, that a FIFO's writer closed: only select does, or a read (docs/io/kqueue.md).
  override def knownFailures: Set[String] =
    Set("a read waiting on a FIFO when its writer closes completes at the end of the stream")
}
