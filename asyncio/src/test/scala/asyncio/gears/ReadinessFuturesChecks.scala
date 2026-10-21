package asyncio.gears

import gears.async.Future

import java.nio.charset.StandardCharsets

import asyncio.Backend
import asyncio.Interest
import asyncio.ReadinessBackend
import ReactorAsync.*

/** `FuturesContract`'s checks of readiness, for a backend that has it. */
trait ReadinessFuturesChecks extends FuturesContract {
  val backend: Backend & ReadinessBackend

  test("whenReady runs its body once the handle is ready") {
    var outcome = ""
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        val runs = new java.util.concurrent.atomic.AtomicInteger
        val ready = r.ops.whenReady(in, Interest.Read) { () =>
          runs.incrementAndGet()
          r.handles.readNow(in, buf) != 0
        }
        val waiting = Future(perform(r, ready))
        perform(r, r.ops.timer(20))
        r.handles.writeNow(out, arena.of("ready".getBytes(StandardCharsets.UTF_8)))
        waiting.await
        outcome = s"${text(buf)} after ${runs.get()}"
      } finally {
        perform(r, r.ops.close(in))
        perform(r, r.ops.close(out))
      }
    }
    assertEquals(outcome, "ready after 1")
  }

}
