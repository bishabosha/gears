package gears.async.io.kqueue

import gears.async.io.Io
import gears.async.Async.Source
import gears.async.Listener

@main def simpleRun(): Unit = {
  given KqueueBackend.type = KqueueBackend

  val Never = new Source[Nothing] {

    override def poll(k: Listener[Nothing]): Boolean =
      println("never poll called")
      false

    override def onComplete(k: Listener[Nothing]): Unit =
      println("registered listener to Never of class " + k.getClass)
      ()

    override def dropListener(k: Listener[Nothing]): Unit = ()


  }

  Io.run { io ?=>
    println("here we go...")
    io.perform(io.backend.ops.timer(500))
    println("after timer")
    io.perform(io.backend.ops.timer(500))
    println("done")
  }
}
