package gears.async.io
package testkit

import java.util.concurrent.CountDownLatch

/** What a backend with readiness must do (see `ReactorContract`): finish an op at once when its handle is ready, run
  * `ready` ops, and use handles without waiting.
  */
trait ReadinessContract extends ReactorContract {
  val backend: ReactorBackend & ReadinessBackend & BlockingBackend & PipeBackend

  test("trySubmit finishes a read of data already waiting without calling its completion") {
    withReactor { r =>
      val (in, out) = backend.handles.pipe()
      try {
        backend.handles.writeNow(out, bytes("ready"))
        val buf = allocate(16)
        val outcome = Outcome()
        assert(r.trySubmit(backend.ops.read(in, buf), outcome))
        assertEquals(text(buf), "ready")
        for _ <- 1 to 3 do r.runOnce(block = false)
        assertEquals(outcome.completed, 0)
      } finally {
        close(r, in)
        close(r, out)
      }
    }
  }

  test("ready completes once the handle is ready, again at once while it still is, and waits once it is drained") {
    withReactor { r =>
      val (in, out) = backend.handles.pipe()
      try {
        val buf = allocate(64)
        val ready = backend.ops.ready(in, Interest.Read)
        val first = Outcome()
        r.submit(ready, first)
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals(first.completed, 0)
        backend.handles.writeNow(out, bytes("part"))
        turnUntil(r)(first.finished)
        assertEquals(first.completed, 1)
        val stillReady = Outcome() // the data has not been taken yet
        r.submit(ready, stillReady)
        turnUntil(r)(stillReady.finished)
        assertEquals(stillReady.completed, 1)
        assertEquals(backend.handles.readNow(in, buf), 4)
        assertEquals(text(buf), "part")
        val drained = Outcome()
        r.submit(ready, drained)
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals(drained.completed, 0)
        backend.handles.writeNow(out, bytes("!"))
        turnUntil(r)(drained.finished)
        assertEquals(backend.handles.readNow(in, buf), 1)
        assertEquals(text(buf), "part!")
      } finally {
        close(r, in)
        close(r, out)
      }
    }
  }

  test("closing a handle that a reactor blocked on another thread waits to write ends its peer's stream") {
    val (in, out) = backend.handles.pipe()
    val registered = new CountDownLatch(1)
    @volatile var other: Reactor | Null = null
    val blocked = new Thread(() => {
      val b = backend.open()
      other = b
      b.submit(backend.ops.ready(out, Interest.Write), _ => registered.countDown())
      b.run() // blocks between turns until stopped
      b.close()
    })
    blocked.start()
    try {
      registered.await()
      Thread.sleep(20) // so that the other reactor is blocked again
      closeAlone(out)
      withReactor { r =>
        val buf = allocate(16)
        val outcome = Outcome()
        r.submit(backend.ops.read(in, buf), outcome)
        turnUntil(r)(outcome.finished)
        assertEquals((outcome.completed, buf.position()), (1, 0))
      }
    } finally {
      val b = other
      if b != null then {
        b.stop()
        b.wakeup()
      }
      blocked.join()
      closeAlone(in)
    }
  }

  test("readNow reports the end of the stream") {
    val (in, out) = backend.handles.pipe()
    try {
      closeAlone(out)
      assertEquals(backend.handles.readNow(in, allocate(16)), -1)
    } finally closeAlone(in)
  }
}
