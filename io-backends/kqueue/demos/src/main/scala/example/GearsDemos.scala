package example

import gears.async.Async
import gears.async.io.Io
import gears.async.io.kqueue.KqueueBackend
import gears.async.io.kqueue.KqueueOps
import gears.io.demos.{DemoBackends, DemoCommands, DemoRunner}

/** The gears mirrors of the plain demos, run as `iokqueuedemos gears <command>` with the same commands and flags: the
  * testkit's demo command line (`DemoCommands`) over kqueue.
  *
  * This is the edge: the demos only know the `gears.async.io` interface, and this is the one place that chooses kqueue
  * and starts runs over it. Kqueue has every capability the demos ask for.
  */
object GearsDemos {

  /** Runs programs over kqueue with `Io.run`. */
  private object KqueueRunner extends DemoRunner[KqueueOps.type] {
    val backend: KqueueBackend.type = KqueueBackend

    def run[T](program: Io[KqueueOps.type] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
  }

  /** The one backend here, `kqueue`. */
  val backends: DemoBackends = DemoBackends(Seq("kqueue" -> KqueueRunner), default = "kqueue")

  def main(args: Array[String]): Unit = DemoCommands(backends).main(args)
}
