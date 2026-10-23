package gears.async.io
package nwframework

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.SocketContract

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class NWFrameworkContractSuite extends FifoContract with SocketContract with ResolveContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
}
