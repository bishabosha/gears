package example.tests

import java.nio.file.Paths

/** Checks of the gears demos beyond the plain demos' own: the bench on a pool of workers. */
trait GearsBenchCheck extends KQueueDemoSuite {
  demoTest("bench: every scenario runs on a pool of four workers, at a tiny scale") {
    val output = run("bench", "--scale", "0.01", "--connections", "8", "--workers", "4")
    for (
      scenario <- Seq(
        "pingpong",
        "pingpong-fresh-ops",
        "pingpong-deadline",
        "concurrent",
        "concurrent-compute",
        "concurrent-offload",
        "connections",
        "pipelined",
        "blocking",
        "blocking-io",
        "fairness",
        "fairness-tick",
        "mixed",
        "mixed-compute"
      ) ++ (if (hasReactors) Seq("timers") else Nil) // `timers` submits to a reactor, which only a reactor backend has
    )
      assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
    assert(!output.contains("Exception"), output)
  }
}

/** The same checks against the gears demos in `io-demos`, on kqueue, run as `iokqueuedemos gears <command>`. */
class GearsKQueueDemoSuite extends KQueueDemoSuite with GearsBenchCheck {
  override def commandPrefix: Seq[String] = Seq("gears")

  demoTest("bench: every scenario runs on the NWFramework backend over TCP, at a tiny scale") {
    val output = run("bench", "--backend", "nwframework", "--tcp", "--scale", "0.01", "--connections", "8")
    for (
      scenario <- Seq(
        "pingpong",
        "pingpong-fresh-ops",
        "pingpong-deadline",
        "concurrent",
        "concurrent-compute",
        "concurrent-offload",
        "connections",
        "pipelined",
        "blocking",
        "blocking-io",
        "timers",
        "fairness",
        "fairness-tick",
        "starvation-p99",
        "mixed",
        "mixed-compute"
      )
    )
      assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
    assert(!output.contains("Exception"), output)
  }

  demoTest("bench: the NWFramework backend refuses a Unix socket address") {
    val output = runFailing("bench", "--backend", "nwframework", "--scale", "0.01", "--scenarios", "pingpong")
    assert(output.contains("no Unix domain sockets"), output)
  }
}

/** The gears demos on Scala Native. No gears backend sends an empty datagram on a connected socket, since the JDK's
  * cannot (see `SocketOps.send`); empty packets to the server, from Java's own clients, are still echoed.
  */
class GearsSocketDemoSuite extends SocketDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")
  override def sendsEmptyConnectedDatagrams: Boolean = false
}

/** The same gears demos on the JVM, over NIO: `NioOnCaller` (`backend` "caller"), or `NioBackend` on a reactor pool
  * ("reactors"). NIO has neither FIFOs nor Unix datagram sockets nor readiness, so neither do its demos; and, as on
  * Scala Native, no empty datagram on a connected socket.
  */
trait OnJvm extends DemoSuite {
  def backend: String

  override def launcher: Seq[String] = {
    val classpath = sys.props.getOrElse("gears.demo.jvm.classpath", fail("set -Dgears.demo.jvm.classpath"))
    Seq(
      Paths.get(sys.props("java.home"), "bin", "java").toString,
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.vm=ALL-UNNAMED",
      s"-Dgears.io.nio.backend=$backend",
      "-cp",
      classpath,
      "gearsexample.Main"
    )
  }

  override def gearsMirrors: Boolean = true
  override def hasFifos: Boolean = false
  override def hasUnixDatagrams: Boolean = false
  override def hasReadiness: Boolean = false
  override def hasReactors: Boolean = backend == "reactors"
  override def sendsEmptyConnectedDatagrams: Boolean = false
}

class JvmGearsDemoSuite extends KQueueDemoSuite with GearsBenchCheck with OnJvm {
  def backend = "caller"
}

class JvmGearsSocketDemoSuite extends SocketDemoSuite with OnJvm {
  def backend = "caller"
}

class JvmReactorsGearsDemoSuite extends KQueueDemoSuite with GearsBenchCheck with OnJvm {
  def backend = "reactors"
}

class JvmReactorsGearsSocketDemoSuite extends SocketDemoSuite with OnJvm {
  def backend = "reactors"
}
