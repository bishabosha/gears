package gearsexample

import mainargs.{ParserForMethods, arg, main}
import asyncio.IpAddress
import asyncio.UnixAddress
import asyncio.kqueue.KqueueBackend
import asyncio.gears.TakePolicy

/** The gears mirrors, run as `kqueuedemo gears <command>` with the same commands and flags as the plain demos.
  *
  * This is the edge: the demos only know the `asyncio` interface, and this is the one place that chooses kqueue. Each
  * demo asks for the capabilities it needs (Unix sockets, FIFOs), and kqueue has them all.
  */
object Main {
  given KqueueBackend.type = KqueueBackend

  @main
  def fileRead(@arg fifo: String): Unit = {
    GearsFileRead.run(fifo)
  }
  @main
  def fileWrite(@arg fifo: String, @arg(short = 'm') message: String): Unit = {
    GearsFileWrite.run(fifo, message)
  }
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
  @main
  def datagramServe(@arg sock: String): Unit = {
    GearsDatagram.serve(UnixAddress(sock))
  }
  @main
  def datagram(
      @arg sock: String,
      @arg localSock: String,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(sock, localSock, message)
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
  @main(name = "move-bench", doc = "Measures what moving a descriptor between kqueue reactors costs")
  def moveBench(
      @arg(name = "round-trips") roundTrips: Int = 100_000,
      @arg(doc = "stay, hand-off or move alone, instead of every way") only: String = "",
      @arg(doc = "with --only, how many round trips between turns") every: Int = 1
  ): Unit = {
    GearsMoveBench.run(roundTrips, only, every)
  }
  @main(doc = "Stress tests to measure reactor changes against; see kqueue-demo/bench/compare.py")
  def bench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc = "use TCP on --host at --port instead of a Unix socket") tcp: mainargs.Flag,
      @arg(doc = "the TCP address: 127.0.0.1, or this machine's, or with --no-server another machine's") host: String =
        "127.0.0.1",
      @arg port: Int = 19876,
      @arg(doc = "connect to an echo server already listening (`bench-serve`), instead of starting one") noServer: mainargs.Flag,
      @arg(doc = "worker threads for the clients, and as many again for the server") workers: Int = 1,
      @arg(doc = "which tasks idle workers take from others: free, affine or hybrid") policy: String = "affine",
      @arg(doc = "kqueue, or nwframework: Network.framework, which needs --tcp") backend: String = "kqueue"
  ): Unit = {
    val address =
      if tcp.value then IpAddress.IPv4(host, port)
      else UnixAddress(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock")
    val names = scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty)
    withBackend(backend) { b =>
      GearsBench.run(names, scale, connections, address, workers, takePolicy(policy), server = !noServer.value)(using b)
    }
  }
  @main(name = "bench-serve", doc = "The bench's echo server alone, for a bench on another machine with --no-server")
  def benchServe(
      @arg(doc = "the address to listen on: this machine's, or 0.0.0.0") host: String = "0.0.0.0",
      @arg port: Int = 19876,
      @arg(doc = "worker threads for the server") workers: Int = 1,
      @arg(doc = "which tasks idle workers take from others: free, affine or hybrid") policy: String = "affine",
      @arg(doc = "kqueue, or nwframework: Network.framework") backend: String = "kqueue"
  ): Unit =
    withBackend(backend) { b =>
      GearsBench.serveForever(IpAddress.IPv4(host, port), workers, takePolicy(policy))(using b)
    }

  private def takePolicy(policy: String): TakePolicy = TakePolicy.values
    .find(_.toString.equalsIgnoreCase(policy))
    .getOrElse(throw new IllegalArgumentException(s"Unknown policy `$policy`; try free, affine or hybrid"))

  /** Runs `body` with the backend named: kqueue, or nwframework, Network.framework. */
  private def withBackend(backend: String)(body: asyncio.SocketBackend => Unit): Unit = backend match {
    case "kqueue" => body(KqueueBackend)
    case "nwframework" => body(asyncio.nwframework.NWFrameworkBackend)
    case other    => throw new IllegalArgumentException(s"Unknown backend `$other`; try kqueue or nwframework")
  }
  def main(args: Array[String]): Unit = {
    ParserForMethods(this).runOrExit(args.toIndexedSeq)
  }
}
