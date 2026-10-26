package gearsexample

import gears.async.io.BlockingBackend
import gears.async.io.FileBackend
import gears.async.io.IpAddress
import gears.async.io.PipeBackend
import gears.async.io.ResolveBackend
import gears.async.io.TakePolicy
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamBackend
import gears.async.io.nio.NioBackend
import gears.async.io.nio.NioOnCaller

import mainargs.{ParserForMethods, arg, main}

/** The gears demos on the JVM, run as `sbt "ioDemosJVM/run <command>"`, with the commands and flags of
  * `iokqueuedemos gears` that NIO can run: the FIFO and Unix datagram demos need capabilities NIO lacks, so they would
  * not compile here, and `move-bench` is kqueue's own.
  *
  * This is the edge, the one place that chooses NIO: `NioOnCaller`, with no reactor, each op performed on the future's
  * own virtual thread; or, with `-Dgears.io.nio.backend=reactors`, `NioBackend`, its futures carried by the reactor
  * pool (see `gears.async.io.ReactorFutures`).
  */
object Main {
  given (UnixStreamBackend & BlockingBackend & FileBackend & ResolveBackend & PipeBackend) =
    sys.props.get("gears.io.nio.backend") match {
      case Some("reactors")      => NioBackend
      case None | Some("caller") => NioOnCaller
      case Some(other)           =>
        throw new IllegalArgumentException(s"gears.io.nio.backend is caller or reactors, not `$other`")
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
  def resolve(@arg host: String = "localhost"): Unit = {
    GearsWhois.resolve(host)
  }
  @main(doc = "Stress tests to measure reactor changes against; see bench/compare.sc")
  def bench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc = "use TCP on 127.0.0.1 at --port instead of a Unix socket") tcp: mainargs.Flag,
      @arg port: Int = 19876,
      @arg(doc = "worker threads for the clients, and as many again for the server") workers: Int = 1,
      @arg(doc = "which tasks idle workers take from others: free, affine or hybrid") policy: String = "affine",
      @arg(doc =
        "run every scenario once at a fifth of its count first, so the JIT has compiled it"
      ) warmup: mainargs.Flag
  ): Unit = {
    val endpoint =
      if tcp.value then Endpoint.Ip(IpAddress.IPv4("127.0.0.1", port))
      else Endpoint.Unix(UnixAddress(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock"))
    val takePolicy = TakePolicy.values
      .find(_.toString.equalsIgnoreCase(policy))
      .getOrElse(throw new IllegalArgumentException(s"Unknown policy `$policy`; try free, affine or hybrid"))
    GearsBench.run(
      scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty),
      scale,
      connections,
      endpoint,
      workers,
      takePolicy,
      warmup.value
    )
  }
  @main(
    name = "jdk-bench",
    doc = "The bench scenarios with the JDK alone: blocking channels on virtual threads, in structured task scopes " +
      "(needs --enable-preview), over a Unix socket"
  )
  def jdkBench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc =
        "run every scenario once at a fifth of its count first, so the JIT has compiled it"
      ) warmup: mainargs.Flag,
      @arg(doc =
        "ignored, so the same flags as bench's can be passed: the JDK's virtual thread scheduler decides; set "
          + "-Djdk.virtualThreadScheduler.parallelism to bound it"
      ) workers: Int = 1,
      @arg(doc = "do the I/O with non-blocking channels and a selector thread for each side, instead of blocking ones")
      selector: mainargs.Flag,
      @arg(
        name = "selector-here",
        doc = "do the I/O with non-blocking channels, each waiting virtual thread selecting on the channel's own " +
          "selector"
      )
      selectorHere: mainargs.Flag
  ): Unit =
    JdkBench.run(
      scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty),
      scale,
      connections,
      java.nio.file.Path.of(s"/tmp/jdk-bench-${System.nanoTime() % 1_000_000}.sock"),
      warmup.value,
      selector.value,
      selectorHere.value
    )
  def main(args: Array[String]): Unit = {
    ParserForMethods(this).runOrExit(args.toIndexedSeq)
  }
}
