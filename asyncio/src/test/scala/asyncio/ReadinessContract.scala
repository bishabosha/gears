package asyncio

import java.util.concurrent.CountDownLatch

/** What a backend with readiness must do (see `ReactorContract`): finish an op at once when its handle is ready, run
  * `whenReady` bodies, and use handles without waiting.
  */
trait ReadinessContract extends ReactorContract {
  val backend: ReactorBackend & ReadinessBackend & BlockingBackend

  test("trySubmit finishes a read of data already waiting without calling its completion") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        backend.handles.writeNow(out, bytes("ready"))
        val buf = allocate(16)
        val outcome = Outcome()
        assert(r.trySubmit(backend.ops.read(in, buf), outcome))
        assertEquals(text(buf), "ready")
        for _ <- 1 to 3 do r.runOnce(block = false)
        assertEquals(outcome.completed, 0)
      } finally {
        r.handles.close(in)
        r.handles.close(out)
      }
    }
  }

  test("whenReady runs its body only once the handle is ready, and waits again while it returns false") {
    withReactor { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = allocate(64)
        var runs = 0
        val ready = backend.ops.whenReady(in, Interest.Read) { () =>
          runs += 1
          backend.handles.readNow(in, buf)
          text(buf).endsWith("!")
        }
        val outcome = Outcome()
        r.submit(ready, outcome)
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals(runs, 0)
        backend.handles.writeNow(out, bytes("part"))
        turnUntil(r)(runs > 0)
        assertEquals(outcome.completed, 0)
        backend.handles.writeNow(out, bytes("!"))
        turnUntil(r)(outcome.finished)
        assertEquals(outcome.completed, 1)
        assertEquals(text(buf), "part!")
      } finally {
        r.handles.close(in)
        r.handles.close(out)
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
      b.submit(backend.ops.whenReady(out, Interest.Write)(() => true), _ => registered.countDown())
      b.run() // blocks between turns until stopped
      b.close()
    })
    blocked.start()
    try {
      registered.await()
      Thread.sleep(20) // so that the other reactor is blocked again
      backend.handles.close(out)
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
      backend.handles.close(in)
    }
  }

  test("readNow reports the end of the stream") {
    val (in, out) = backend.handles.pipe()
    try {
      backend.handles.close(out)
      assertEquals(backend.handles.readNow(in, allocate(16)), -1)
    } finally backend.handles.close(in)
  }
}
