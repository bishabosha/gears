package gears.async.io

import gears.async.*
import gears.async.default.given

/** A `direct` stage's body, cancelled as it waits for an element, catches that and emits: its `finally` is run.
  *
  * On Scala Native (0.5.12 and the nightly of 2026-11-14) it is not: a `finally` is skipped when the `catch` beside it
  * calls something that throws, in a future that was resumed after waiting inside the `try`, unless the call is in a
  * `try` of its own. The JVM and Scala.js run it. This is the least of it found so far, kept to be made into a report.
  */
class DirectStageFinallyBehavior extends munit.FunSuite {

  private def cancelled(wrapped: Boolean): (Boolean, String) = {
    @volatile var ran = false
    @volatile var caught = ""
    val got = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
    Async.blocking {
      @volatile var closed = false
      var body: Future[Unit] | Null = null
      val drive = new Flow.Drive {
        def spawn(work: Async ?=> Unit): Unit = ()
        def later(run: Runnable): Unit = if !closed then run.run()
        def hold(): Unit = ()
        def release(): Unit = ()
        def stop(): Unit = ()
        def start(work: Async ?=> Unit): Runnable = {
          val begun = Future(work)
          body = begun
          () => begun.cancel()
        }
        def waits: Boolean = false
        def pause(register: Runnable => Unit): Unit = ()
      }
      val last = new Flow.Step[Int] {
        def push(element: Int): Unit = got.add(element): Unit
        def end(): Unit = ()
        def abandon(): Unit = ()
        def fail(thrown: Throwable): Unit = ()
      }
      val port = new Flow.Port[Int]
      val flow = port.direct[Int] { elements =>
        try for element <- elements do Flow.emit(element + 1)
        catch {
          case thrown: IllegalStateException =>
            caught = thrown.getClass.getSimpleName
            if wrapped then
              try Flow.emit(-1)
              catch case again: Throwable => throw again
            else Flow.emit(-1)
        } finally ran = true
      }
      val step = Flow.compile(flow, port, last, drive).nn
      step.push(1)
      while got.isEmpty() do AsyncOperations.sleep(1)
      AsyncOperations.sleep(20)
      closed = true
      body.nn.cancel()
      body.nn.awaitResult: Unit
    }
    (ran, s"caught $caught, got $got")
  }

  test("its finally is run") {
    assume(System.getProperty("java.vm.name") != "Scala Native", "Scala Native skips the finally: see above")
    val (ran, what) = cancelled(wrapped = false)
    assert(ran, what)
  }

  test("its finally is run, the handler's emit in a try of its own") {
    val (ran, what) = cancelled(wrapped = true)
    assert(ran, what)
  }
}
