package example.tests

/** The bench executables (`ioBench`), driven as the demos are: every scenario at a tiny scale, and the gears scenarios
  * on a pool of workers. The plain kqueue scenarios, `iobench bench`, have no mirror of the gears-only ones.
  */
trait BenchChecks extends DemoSuite {
  demoTest("bench: every scenario runs and reports, at a tiny scale") {
    val output = run("bench", "--scale", "0.01", "--connections", "8")
    val gearsOnly =
      if (gearsMirrors) Seq("mixed", "mixed-compute", "pingpong-fresh-ops", "fairness", "fairness-tick")
      else Seq.empty
    for (
      scenario <- Seq(
        "pingpong",
        "pingpong-deadline",
        "concurrent",
        "connections",
        "pipelined",
        "blocking",
        "timers"
      ) ++ gearsOnly
    )
      assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
  }

  if (gearsMirrors) {
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
          "mixed-compute",
          "timers"
        )
      )
        assert(output.linesIterator.exists(_.startsWith(s"bench: $scenario ")), output)
      assert(!output.contains("Exception"), output)
    }
  }
}

/** The bench against `KqueueLoop` directly, with no reactor: `iobench bench`. */
class KQueueRawBenchSuite extends BenchChecks {
  override def launcher: Seq[String] = Seq(benchBinary)
}

/** The gears bench on kqueue: `iobench gears bench`. */
class GearsKQueueBenchSuite extends BenchChecks {
  override def launcher: Seq[String] = Seq(benchBinary)
  override def commandPrefix: Seq[String] = Seq("gears")
}

/** The gears bench on Network.framework: `iobench gears bench --backend nwframework-everywhere`, whose own sockets are
  * TCP's, and `--backend nwframework`, which runs a Unix socket's scenarios on kqueue's code.
  */
class GearsNWFrameworkBenchSuite extends DemoSuite {
  override def launcher: Seq[String] = Seq(benchBinary)
  override def commandPrefix: Seq[String] = Seq("gears")

  demoTest("bench: every scenario runs on Network.framework over TCP, at a tiny scale") {
    val output =
      run("bench", "--backend", "nwframework-everywhere", "--tcp", "--scale", "0.01", "--connections", "8")
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

  demoTest("bench: the NWFramework backend runs a Unix socket's scenarios on kqueue's code") {
    val output = run("bench", "--backend", "nwframework", "--scale", "0.01", "--scenarios", "pingpong")
    assert(output.linesIterator.exists(_.startsWith("bench: pingpong ")), output)
  }
}

/** The gears bench on the JVM, `java -cp <ioBenchJVM> gearsbench.Main bench`, over `NioOnCaller`. */
class JvmGearsBenchSuite extends BenchChecks with OnJvm {
  def backend = "caller"
  override def program: (String, String) = ("gears.bench.jvm.classpath", "gearsbench.Main")
}

/** The gears bench on the JVM over `NioBackend`, on a reactor pool. */
class JvmReactorsGearsBenchSuite extends BenchChecks with OnJvm {
  def backend = "reactors"
  override def program: (String, String) = ("gears.bench.jvm.classpath", "gearsbench.Main")
}
