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
    extends SocketContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract
    with HostAddressChecks {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
  def hostAddressesOnKqueue: Boolean = true
}

/** The socket contract with every IP socket on Network.framework, loopback's too (`NWFrameworkBackend.Everywhere`). */
class NWFrameworkEverywhereContractSuite extends HostAddressChecks {
  val backend: NWFrameworkBackend.Everywhere.type = NWFrameworkBackend.Everywhere
  def hostAddressesOnKqueue: Boolean = false

  test("a Network.framework handle shows its kind and number, as in the failure of an op on it once closed") {
    withReactor { r =>
      val (server, _) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      assert(server.toString.matches("""Handle\(nw listener \d+\)"""), server.toString)
      close(r, server)
      val failure = run(r, backend.ops.accept(server, Slot[Connection]()))
      assert(failure != null && failure.getMessage.contains(server.toString), String.valueOf(failure))
    }
  }
}
