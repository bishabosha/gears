package gears.async.io
package nwframework

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.SocketContract

import java.nio.charset.StandardCharsets

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class NWFrameworkContractSuite extends FifoContract with SocketContract with ResolveContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend

  test("a bound datagram socket lets go a flow idle for FlowIdleNanos, and hears its peer again after") {
    val before = NWFrameworkHandle.FlowIdleNanos
    NWFrameworkHandle.FlowIdleNanos = 100_000_000L
    try
      withReactor { r =>
        val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4("127.0.0.1", port), null))
        try {
          val first = datagram(r, null, IpAddress.IPv4("127.0.0.1", port))
          val second = datagram(r, null, IpAddress.IPv4("127.0.0.1", port))
          try {
            def heard(from: Datagram, text: String)(using munit.Location): Unit = {
              assertEquals(run(r, backend.ops.send(from, bytes(text))), null)
              val buf = allocate(64)
              assertEquals(run(r, backend.ops.receive(server, buf, Slot[IpAddress]())), null)
              assertEquals(StandardCharsets.UTF_8.decode(buf).toString, text)
            }
            def flows = NWFrameworkHandles.get(server).flowCount
            heard(first, "first")
            assertEquals(flows, 1)
            Thread.sleep(1200) // past the idle time, and the second between looks for idle flows
            heard(second, "second") // its packet finds the first's flow idle
            assertEquals(flows, 1, "the idle flow was let go")
            // A packet that came while the framework cancels the flow would be lost: once it has, the peer is heard.
            Thread.sleep(300)
            heard(first, "again")
            assertEquals(flows, 2)
          } finally {
            close(r, first)
            close(r, second)
          }
        } finally close(r, server)
      }
    finally NWFrameworkHandle.FlowIdleNanos = before
  }
}
