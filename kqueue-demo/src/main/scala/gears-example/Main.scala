package gearsexample

import mainargs.{ParserForMethods, arg, main}
import asyncio.Backend
import asyncio.Address
import asyncio.kqueue.KqueueBackend

/** The gears mirrors, run as `kqueuedemo gears <command>` with the same commands and flags as the plain demos.
  *
  * This is the edge: the demos only know the `asyncio` interface, and this is the one place that chooses kqueue.
  */
object Main {
  given Backend = KqueueBackend

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
    GearsServerSocket.run(Address.IPv4(host, port))
  }
  @main(name = "sock4")
  def sock4(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsSocket.run(Address.IPv4(host, port))
  }
  @main(name = "sock6-serve")
  def sock6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsServerSocket.run(Address.IPv6(host, port))
  }
  @main(name = "sock6")
  def sock6(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsSocket.run(Address.IPv6(host, port))
  }
  @main
  def kvServe(@arg sock: String): Unit = {
    GearsKeyValue.serve(Address.Unix(sock))
  }
  @main
  def kv(@arg sock: String, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(Address.Unix(sock), count)
  }
  @main(name = "kv4-serve")
  def kv4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsKeyValue.serve(Address.IPv4(host, port))
  }
  @main(name = "kv4")
  def kv4(@arg host: String = "127.0.0.1", @arg port: Int = 9999, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(Address.IPv4(host, port), count)
  }
  @main(name = "kv6-serve")
  def kv6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsKeyValue.serve(Address.IPv6(host, port))
  }
  @main(name = "kv6")
  def kv6(@arg host: String = "::1", @arg port: Int = 9999, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(Address.IPv6(host, port), count)
  }
  @main
  def datagramServe(@arg sock: String): Unit = {
    GearsDatagram.serve(Address.Unix(sock))
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
    GearsDatagram.serve(Address.IPv4(host, port))
  }
  @main(name = "datagram4")
  def datagram4(
      @arg host: String = "127.0.0.1",
      @arg port: Int = 9999,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(Address.IPv4(host, port), message)
  }
  @main(name = "datagram6-serve")
  def datagram6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsDatagram.serve(Address.IPv6(host, port))
  }
  @main(name = "datagram6")
  def datagram6(
      @arg host: String = "::1",
      @arg port: Int = 9999,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(Address.IPv6(host, port), message)
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
  @main(doc = "Checks that kqueue reactors on different threads can share a descriptor, as a pool's workers will")
  def share(): Unit = {
    GearsShare.run()
  }
  @main(doc = "Checks the run queue under contention: every task runs once, whichever thread takes it")
  def runqueue(@arg tasks: Int = 2_000_000, @arg takers: Int = 3): Unit = {
    GearsRunQueue.run(tasks, takers)
  }
  @main(doc = "Checks that tasks queued for a worker in particular run on its thread, whatever other workers take")
  def inbox(@arg rounds: Int = 500): Unit = {
    GearsInbox.run(rounds)
  }
  @main(doc = "Checks that a suspended future resumes correctly on another thread, as a pool's workers will make it")
  def migrate(
      @arg futures: Int = 4,
      @arg resumes: Int = 20_000,
      @arg(doc = "keep each future on the thread it started on, to compare") stay: mainargs.Flag
  ): Unit = {
    GearsMigrate.run(futures, resumes, stay.value)
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
      if tcp.value then Address.IPv4("127.0.0.1", port)
      else Address.Unix(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock")
    val takePolicy = TakePolicy.values
      .find(_.toString.equalsIgnoreCase(policy))
      .getOrElse(throw new IllegalArgumentException(s"Unknown policy `$policy`; try free, affine or hybrid"))
    GearsBench.run(scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty), scale, connections, address, workers, takePolicy)
  }
  def main(args: Array[String]): Unit = {
    ParserForMethods(this).runOrExit(args.toIndexedSeq)
  }
}
