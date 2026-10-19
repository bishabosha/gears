package gearsexample

import mainargs.{ParserForMethods, arg, main}
import asyncio.IpAddress
import asyncio.UnixAddress
import asyncio.nio.NioBackend
import asyncio.gears.TakePolicy

/** The gears demos on the JVM, run as `sbt "gearsDemoJVM/run <command>"`, with the commands and flags of
  * `kqueuedemo gears` that NIO can run: the FIFO and Unix datagram demos need capabilities NIO lacks, so they would not
  * compile here, and `move-bench` is kqueue's own.
  *
  * This is the edge, the one place that chooses NIO. The futures are gears' own virtual threads, beside the reactors;
  * `-Dasyncio.gears.carriers=reactors` carries them on the reactor pool instead (see `asyncio.gears.Carriers`).
  */
object Main {
  given NioBackend.type = NioBackend

  @main
  def timer(): Unit = {
    GearsTimer.run()
  }
  @main
  def sockServe(@arg sock: String): Unit = {
    GearsServerSocket.run(sock)
  }
  @main
  def sock(@arg sock: String): Unit = {
    GearsSocket.run(sock)
  }
  @main(name = "sock4-serve")
  def sock4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsServerSocket.run(IpAddress.IPv4(host, port))
  }
  @main(name = "sock4")
  def sock4(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsSocket.run(IpAddress.IPv4(host, port))
  }
  @main(name = "sock6-serve")
  def sock6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsServerSocket.run(IpAddress.IPv6(host, port))
  }
  @main(name = "sock6")
  def sock6(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsSocket.run(IpAddress.IPv6(host, port))
  }
  @main
  def kvServe(@arg sock: String): Unit = {
    GearsKeyValue.serve(UnixAddress(sock))
  }
  @main
  def kv(@arg sock: String, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(UnixAddress(sock), count)
  }
  @main(name = "kv4-serve")
  def kv4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsKeyValue.serve(IpAddress.IPv4(host, port))
  }
  @main(name = "kv4")
  def kv4(@arg host: String = "127.0.0.1", @arg port: Int = 9999, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(IpAddress.IPv4(host, port), count)
  }
  @main(name = "kv6-serve")
  def kv6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsKeyValue.serve(IpAddress.IPv6(host, port))
  }
  @main(name = "kv6")
  def kv6(@arg host: String = "::1", @arg port: Int = 9999, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(IpAddress.IPv6(host, port), count)
  }
  @main(name = "datagram4-serve")
  def datagram4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsDatagram.serve(IpAddress.IPv4(host, port))
  }
  @main(name = "datagram4")
  def datagram4(
      @arg host: String = "127.0.0.1",
      @arg port: Int = 9999,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(IpAddress.IPv4(host, port), message)
  }
  @main(name = "datagram6-serve")
  def datagram6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsDatagram.serve(IpAddress.IPv6(host, port))
  }
  @main(name = "datagram6")
  def datagram6(
      @arg host: String = "::1",
      @arg port: Int = 9999,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(IpAddress.IPv6(host, port), message)
  }
  @main
  def whois(
      @arg host: String = "whois.iana.org",
      @arg port: Int = 43,
      @arg query: String = "example.com"
  ): Unit = {
    GearsWhois.run(host, port, query)
  }
  @main
  def cancel(): Unit = {
    GearsCancel.run()
  }
  @main
  def resolve(@arg host: String = "localhost"): Unit = {
    GearsWhois.resolve(host)
  }
  @main(doc = "Stress tests to measure reactor changes against; see kqueue-demo/bench/compare.py")
  def bench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc = "use TCP on 127.0.0.1 at --port instead of a Unix socket") tcp: mainargs.Flag,
      @arg port: Int = 19876,
      @arg(doc = "worker threads for the clients, and as many again for the server") workers: Int = 1,
      @arg(doc = "which tasks idle workers take from others: free, affine or hybrid") policy: String = "affine"
  ): Unit = {
    val address =
      if tcp.value then IpAddress.IPv4("127.0.0.1", port)
      else UnixAddress(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock")
    val takePolicy = TakePolicy.values
      .find(_.toString.equalsIgnoreCase(policy))
      .getOrElse(throw new IllegalArgumentException(s"Unknown policy `$policy`; try free, affine or hybrid"))
    GearsBench.run(scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty), scale, connections, address, workers, takePolicy)
  }
  def main(args: Array[String]): Unit = {
    ParserForMethods(this).runOrExit(args.toIndexedSeq)
  }
}
