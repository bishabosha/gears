package example.tests

import java.nio.file.Paths

/** Checks of the gears demos beyond the plain demos' own: the bench on a pool of workers. */
trait GearsBenchCheck extends KQueueDemoSuite {
  demoTest("bench: every scenario runs on a pool of four workers, at a tiny scale") {
    val output = run("bench", "--scale", "0.01", "--connections", "8", "--workers", "4")
    for (
      scenario <- Seq(
        "pingpong", "pingpong-fresh-ops", "pingpong-deadline", "concurrent", "concurrent-compute", "concurrent-offload",
        "connections", "pipelined", "blocking",
        "blocking-io", "timers", "fairness", "fairness-tick", "mixed", "mixed-compute"
      )
    )
      assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
    assert(!output.contains("Exception"), output)
  }
}

/** The same checks against the gears demos in `gears-demo`, on kqueue, run as `kqueuedemo gears <command>`. */
class GearsKQueueDemoSuite extends KQueueDemoSuite with GearsBenchCheck {
  override def commandPrefix: Seq[String] = Seq("gears")

  demoTest("bench: every scenario runs on the Darwin backend over TCP, at a tiny scale") {
    val output = run("bench", "--backend", "darwin", "--tcp", "--scale", "0.01", "--connections", "8")
    for (
      scenario <- Seq(
        "pingpong", "pingpong-fresh-ops", "pingpong-deadline", "concurrent", "concurrent-compute", "concurrent-offload",
        "connections", "pipelined", "blocking", "blocking-io", "timers", "fairness", "fairness-tick", "starvation-p99",
        "mixed", "mixed-compute"
      )
    )
      assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
    assert(!output.contains("Exception"), output)
  }

  demoTest("bench: the Darwin backend refuses a Unix socket address") {
    val output = runFailing("bench", "--backend", "darwin", "--scale", "0.01", "--scenarios", "pingpong")
    assert(output.contains("no Unix domain sockets"), output)
  }
}

class GearsSocketDemoSuite extends SocketDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")
}

/** The same gears demos on the JVM, over NIO, with their futures carried by `carriers` (`asyncio.gears.Carriers`). NIO
  * has neither FIFOs nor Unix datagram sockets, so neither do its demos, and the JDK cannot send an empty datagram on a
  * connected socket. Empty packets to the server, from Java's own clients, are still echoed.
  */
trait OnJvm extends DemoSuite {
  def carriers: String

  override def launcher: Seq[String] = {
    val classpath = sys.props.getOrElse("gears.demo.jvm.classpath", fail("set -Dgears.demo.jvm.classpath"))
    Seq(
      Paths.get(sys.props("java.home"), "bin", "java").toString,
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      s"-Dasyncio.gears.carriers=$carriers",
      "-cp",
      classpath,
      "gearsexample.Main"
    )
  }

  override def gearsMirrors: Boolean = true
  override def hasFifos: Boolean = false
  override def hasUnixDatagrams: Boolean = false
  override def sendsEmptyConnectedDatagrams: Boolean = false
}

class JvmGearsDemoSuite extends KQueueDemoSuite with GearsBenchCheck with OnJvm {
  def carriers = "jdk"
}

class JvmGearsSocketDemoSuite extends SocketDemoSuite with OnJvm {
  def carriers = "jdk"
}

class JvmReactorsGearsDemoSuite extends KQueueDemoSuite with GearsBenchCheck with OnJvm {
  def carriers = "reactors"
}

class JvmReactorsGearsSocketDemoSuite extends SocketDemoSuite with OnJvm {
  def carriers = "reactors"
}
