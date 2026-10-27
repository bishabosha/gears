package gearsexample

import gears.async.io.IpAddress
import gears.async.io.IpSockets
import gears.async.io.TakePolicy
import gears.async.io.UnixAddress
import gears.async.io.kqueue.KqueueBackend

import mainargs.{ParserForMethods, arg, main}

/** The gears mirrors, run as `iokqueuedemos gears <command>` with the same commands and flags as the plain demos.
  *
  * This is the edge: the demos only know the `gears.async.io` interface, and this is the one place that chooses kqueue.
  * Each demo asks for the capabilities it needs (Unix sockets, FIFOs), and kqueue has them all.
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
  @main(name = "reactor-sock-serve", doc = "sock-serve as completions on one reactor (`ReactorServerSocket`)")
  def reactorSockServe(@arg sock: String): Unit = {
    ReactorServerSocket.run(sock)
  }
  @main(name = "reactor-sock4-serve", doc = "sock4-serve as completions on one reactor (`ReactorServerSocket`)")
  def reactorSock4Serve(
      @arg host: String = "127.0.0.1",
      @arg port: Int = 9999,
      @arg(doc =
        "kqueue, nwframework (Network.framework, loopback on kqueue), or nwframework-everywhere (loopback too)"
      ) backend: String = "kqueue"
  ): Unit = {
    withReactorBackend(backend)(ReactorServerSocket.run(IpAddress.IPv4(host, port))(using _))
  }
  @main(name = "reactor-sock6-serve", doc = "sock6-serve as completions on one reactor (`ReactorServerSocket`)")
  def reactorSock6Serve(
      @arg host: String = "::1",
      @arg port: Int = 9999,
      @arg(doc =
        "kqueue, nwframework (Network.framework, loopback on kqueue), or nwframework-everywhere (loopback too)"
      ) backend: String = "kqueue"
  ): Unit = {
    withReactorBackend(backend)(ReactorServerSocket.run(IpAddress.IPv6(host, port))(using _))
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
  @main(doc = "Stress tests to measure reactor changes against; see bench/compare.sc")
  def bench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc = "use TCP on --host at --port instead of a Unix socket") tcp: mainargs.Flag,
      @arg(doc = "the TCP address: 127.0.0.1, or this machine's, or with --no-server another machine's") host: String =
        "127.0.0.1",
      @arg port: Int = 19876,
      @arg(doc =
        "connect to an echo server already listening (`bench-serve`), instead of starting one"
      ) noServer: mainargs.Flag,
      @arg(doc = "worker threads for the clients, and as many again for the server") workers: Int = 1,
      @arg(doc = "which tasks idle workers take from others: free, affine or hybrid") policy: String = "affine",
      @arg(doc =
        "kqueue, nwframework (Network.framework, loopback on kqueue), or nwframework-everywhere (loopback too); Network.framework's own sockets need --tcp, which on loopback needs nwframework-everywhere"
      )
      backend: String = "kqueue"
  ): Unit = {
    val names = scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty)
    def run[B <: gears.async.io.StreamSockets](endpoint: Endpoint[B])(using B): Unit =
      GearsBench.run(names, scale, connections, endpoint, workers, takePolicy(policy), server = !noServer.value)
    withBackend(backend) { b =>
      if tcp.value then run(Endpoint.Ip(IpAddress.IPv4(host, port)))(using b)
      else run(Endpoint.Unix(UnixAddress(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock")))(using b)
    }
  }
  @main(name = "bench-serve", doc = "The bench's echo server alone, for a bench on another machine with --no-server")
  def benchServe(
      @arg(doc = "the address to listen on: this machine's, or 0.0.0.0") host: String = "0.0.0.0",
      @arg port: Int = 19876,
      @arg(doc = "worker threads for the server") workers: Int = 1,
      @arg(doc = "which tasks idle workers take from others: free, affine or hybrid") policy: String = "affine",
      @arg(doc =
        "kqueue, nwframework (Network.framework, loopback on kqueue), or nwframework-everywhere (loopback too)"
      ) backend: String = "kqueue"
  ): Unit =
    withBackend(backend) { b =>
      GearsBench.serveForever(Endpoint.Ip(IpAddress.IPv4(host, port)), workers, takePolicy(policy))(using b)
    }

  private def takePolicy(policy: String): TakePolicy = TakePolicy.values
    .find(_.toString.equalsIgnoreCase(policy))
    .getOrElse(throw new IllegalArgumentException(s"Unknown policy `$policy`; try free, affine or hybrid"))

  /** Runs `body` with the backend named: kqueue; nwframework, Network.framework for IP sockets not on loopback,
    * kqueue's code for the rest; or nwframework-everywhere, Network.framework for loopback's too, to measure it.
    */
  private def withBackend(
      backend: String
  )(body: gears.async.io.ReactorBackend & IpSockets & gears.async.io.UnixStreams => Unit): Unit =
    backend match {
      case "kqueue"                 => body(KqueueBackend)
      case "nwframework"            => body(gears.async.io.nwframework.NWFrameworkBackend)
      case "nwframework-everywhere" => body(gears.async.io.nwframework.NWFrameworkBackend.Everywhere)
      case other                    =>
        throw new IllegalArgumentException(
          s"Unknown backend `$other`; try kqueue, nwframework or nwframework-everywhere"
        )
    }

  private def withReactorBackend(backend: String)(
      body: gears.async.io.ReactorBackend & gears.async.io.IpStreams => Unit
  ): Unit =
    withBackend(backend)(body)
  def main(args: Array[String]): Unit = {
    ParserForMethods(this).runOrExit(args.toIndexedSeq)
  }
}
