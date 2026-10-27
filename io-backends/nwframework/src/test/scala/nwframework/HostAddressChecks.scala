package gears.async.io
package nwframework

import gears.async.io.testkit.SocketContract

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*

/** Sockets between two addresses of this host that are not loopback, which the kernel carries over loopback: kqueue's
  * on `NWFrameworkBackend`, as loopback's are, and Network.framework's on `Everywhere`. Skipped on a host with no such
  * address on an Ethernet or Wi-Fi interface (`en`).
  */
trait HostAddressChecks extends SocketContract {
  val backend: NWFrameworkBackends

  /** Whether sockets to this host's own addresses are kqueue's. */
  def hostAddressesOnKqueue: Boolean

  /** An IPv4 address of this host's `en` interfaces, if any. */
  private def hostAddress: String | Null =
    java.net.NetworkInterface.getNetworkInterfaces.asScala
      .filter(i => i.isUp && !i.isLoopback && i.getName.startsWith("en"))
      .flatMap(_.getInetAddresses.asScala)
      .collectFirst { case v4: java.net.Inet4Address => v4.getHostAddress }
      .orNull

  test("a datagram socket bound to an address of this host, connected to another socket at one, sends and receives") {
    val host = hostAddress
    assume(host != null, "this host has no IPv4 address on an en interface")
    withReactor { r =>
      val (server, port) = onFreePort(port => datagram(r, IpAddress.IPv4(host.nn, port), null))
      try {
        val client = datagram(r, IpAddress.IPv4(host.nn, 0), IpAddress.IPv4(host.nn, port))
        try {
          assertEquals(NWFrameworkHandles.owns(client), !hostAddressesOnKqueue, "which facility has it")
          assertEquals(run(r, backend.ops.send(client, bytes("across"))), null)
          val buf = allocate(64)
          assertEquals(run(r, backend.ops.receive(server, buf, Slot[IpAddress]())), null)
          assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "across")
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("a connection to a listener at an address of this host is kqueue's, the listener Network.framework's") {
    val host = hostAddress
    assume(host != null, "this host has no IPv4 address on an en interface")
    withReactor { r =>
      val (server, port) = onFreePort(port => listen(r, IpAddress.IPv4(host.nn, port)))
      try {
        assert(NWFrameworkHandles.owns(server), "a listener there is reached from other hosts")
        val (connected, into) = startConnect(r, IpAddress.IPv4(host.nn, port))
        val accepted = Slot[Connection]()
        assertEquals(run(r, backend.ops.accept(server, accepted)), null)
        turnUntil(r)(connected.finished)
        assertEquals(connected.failure, null)
        val client = into.clear()
        val peer = accepted.clear()
        try {
          assertEquals(NWFrameworkHandles.owns(client), !hostAddressesOnKqueue, "which facility has it")
          assertEquals(run(r, backend.ops.write(client, bytes("hello"))), null)
          val buf = allocate(16)
          assertEquals(run(r, backend.ops.read(peer, buf)), null)
          buf.flip()
          assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "hello")
        } finally {
          close(r, client)
          close(r, peer)
        }
      } finally close(r, server)
    }
  }
}
