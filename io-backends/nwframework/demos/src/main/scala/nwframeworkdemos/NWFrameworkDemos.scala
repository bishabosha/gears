package nwframeworkdemos

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Backend
import gears.async.io.Io
import gears.async.io.internal.ReactorBackend
import gears.async.io.internal.ReactorRuns
import gears.async.io.internal.TakePolicy
import gears.async.io.kqueue.KqueueBackend
import gears.async.io.nwframework.NWFrameworkBackend
import gears.io.demos.{DemoBackends, DemoCommands, DemoRunner}

/** The gears demos over the Network.framework backend: the testkit's demo command line (`DemoCommands`), run as
  * `sbt "ioNWFrameworkDemos/run <command>"`, with the commands and flags of `iokqueuedemos gears`.
  *
  * This is the edge, the one place that chooses the backend and starts runs: `--backend nwframework`, the default,
  * Network.framework for IP sockets off this host and kqueue's code for the rest, loopback's sockets included;
  * `nwframework-everywhere`, Network.framework for loopback's IP sockets too, to check and measure it; or `kqueue`, the
  * backend both are built on, to compare against. Every one has the capabilities the demos ask for.
  */
object NWFrameworkDemos {

  /** Runs programs over `backend`: with `Io.run`, or with `ReactorRuns.run` on a pool of the workers asked for. */
  private final class ReactorRunner[B <: ReactorBackend](val backend: B) extends DemoRunner[B] {
    def run[T](program: Io[B] ?=> (Async.Spawn, AsyncOperations) ?=> T): T = Io.run(using backend)(program)

    def onPool[T](workers: Int, policy: TakePolicy)(program: Io[B] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
      ReactorRuns.run(workers, policy)(using backend)(program)
  }

  /** The backends here, by the names `--backend` chooses them with, `default` chosen when it is not given: the bench
    * (`iobench gears`) offers them with kqueue as its default.
    */
  def backends(default: String = "nwframework"): DemoBackends = DemoBackends(
    Seq[(String, DemoRunner[Backend])](
      "nwframework" -> ReactorRunner(NWFrameworkBackend),
      "nwframework-everywhere" -> ReactorRunner(NWFrameworkBackend.Everywhere),
      "kqueue" -> ReactorRunner(KqueueBackend)
    ),
    default
  )

  def main(args: Array[String]): Unit = DemoCommands(backends()).main(args)
}
