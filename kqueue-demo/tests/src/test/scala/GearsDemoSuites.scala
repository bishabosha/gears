package example.tests

/** The same checks against the gears mirrors in `src/main/scala/gears-example`, run as `kqueuedemo gears <command>`. */
class GearsKQueueDemoSuite extends KQueueDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")

  demoTest("bench: every scenario runs on a pool of four workers, at a tiny scale") {
    val output = run("bench", "--scale", "0.01", "--connections", "8", "--workers", "4")
    for (
      scenario <- Seq(
        "pingpong", "pingpong-fresh-ops", "pingpong-deadline", "concurrent", "connections", "pipelined", "blocking",
        "blocking-io", "timers", "fairness", "fairness-tick", "mixed", "mixed-compute"
      )
    )
      assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
    assert(!output.contains("Exception"), output)
  }

  demoTest("kqueue reactors on different threads share a descriptor, dropping stale filters and reused numbers") {
    val output = run("share")
    assert(output.contains("share: 0 failures"), output)
    assert(!output.contains("reuse was not checked"), output)
  }

  demoTest("the run queue runs every task once while other threads take tasks from it") {
    val output = run("runqueue")
    assert(output.contains("; 0 failures"), output)
  }

  demoTest("a suspended future resumes on another thread with its state and that thread's signal state") {
    val output = run("migrate")
    assert(output.contains("; 0 failures"), output)
  }
}

class GearsSocketDemoSuite extends SocketDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")
}
