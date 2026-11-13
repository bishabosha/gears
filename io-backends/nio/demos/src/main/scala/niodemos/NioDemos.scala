package niodemos

import gears.async.Async
import gears.async.io.Backend
import gears.async.io.Io
import gears.async.io.nio.NioBackend
import gears.async.io.nio.NioOnCaller
import gears.async.io.nio.NioOps
import gears.io.demos.{DemoBackends, DemoCommands, DemoRunner}

/** The gears demos on the JVM, over NIO: the testkit's demo command line (`DemoCommands`), run as
  * `sbt "ioNioDemos/run <command>"`. The demos that need FIFOs or Unix datagram sockets, which NIO lacks, fail saying
  * so.
  *
  * This is the edge, the one place that chooses NIO and starts runs: `--backend caller`, `NioOnCaller`, with no
  * reactor, each op performed on the future's own virtual thread; or `--backend reactors`, `NioBackend`, its futures
  * carried by the reactor pool. The default is `caller`, or what `-Dgears.io.nio.backend` names, which the bench's
  * launchers set.
  */
object NioDemos {

  /** Runs programs over `backend` with `Io.run`. */
  private final class NioRunner(val backend: Backend { type Interface = NioOps.type }) extends DemoRunner[NioOps.type] {
    def run[T](program: Io[NioOps.type] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
  }

  /** NIO's backends, by the names `--backend` and `-Dgears.io.nio.backend` choose them with. */
  val backends: DemoBackends = DemoBackends(
    Seq("caller" -> NioRunner(NioOnCaller), "reactors" -> NioRunner(NioBackend)),
    default = sys.props.getOrElse("gears.io.nio.backend", "caller")
  )

  def main(args: Array[String]): Unit = DemoCommands(backends).main(args)
}
