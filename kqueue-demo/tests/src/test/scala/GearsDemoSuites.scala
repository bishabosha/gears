package example.tests

/** The same checks against the gears mirrors in `src/main/scala/gears-example`, run as `kqueuedemo gears <command>`. */
class GearsKQueueDemoSuite extends KQueueDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")

  demoTest("the run queue runs every task once while other threads steal from it") {
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
