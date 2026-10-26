package gears.async.io
package nwframework

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.SocketContract

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class NWFrameworkContractSuite extends FifoContract with SocketContract with ResolveContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend

  // macOS tells neither kqueue nor Dispatch, nor poll, that a FIFO's writer closed: only select does, or a read (docs/io/kqueue.md).
  override def knownFailures: Set[String] =
    Set("a read waiting on a FIFO when its writer closes completes at the end of the stream")
}
