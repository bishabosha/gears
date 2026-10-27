package gears.async.io
package nwframework

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.SocketContract
import gears.async.io.testkit.UnixDatagramContract
import gears.async.io.testkit.UnixStreamContract

/** The reactor contract over the backend as programs get it: kqueue's reactors, with Network.framework for IP sockets
  * that are not on loopback, and kqueue's own code for the rest. Its checks' sockets are on loopback, so they run on
  * kqueue's; `NWFrameworkEverywhereContractSuite` runs them on Network.framework's.
  */
class NWFrameworkContractSuite
    extends UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
}

/** The socket contract with every IP socket on Network.framework, loopback's too (`NWFrameworkBackend.Everywhere`). */
class NWFrameworkEverywhereContractSuite extends SocketContract {
  val backend: NWFrameworkBackend.Everywhere.type = NWFrameworkBackend.Everywhere
}
