package example.tests

/** The same checks against the gears mirrors in `src/main/scala/gears-example`, run as `kqueuedemo gears <command>`. */
class GearsKQueueDemoSuite extends KQueueDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")

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

class GearsSocketDemoSuite extends SocketDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")
}
