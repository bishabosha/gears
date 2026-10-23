package asyncio.gears

import gears.async.Future

import java.nio.charset.StandardCharsets

import asyncio.Backend
import asyncio.Interest
import asyncio.PipeBackend
import asyncio.ReadinessBackend
import asyncio.ResolveBackend
import asyncio.SocketBackend

/** `FuturesContract`'s checks of readiness, for a backend that has it. */
trait ReadinessFuturesChecks extends FuturesContract {
  val backend: Backend & SocketBackend & PipeBackend & ResolveBackend & ReadinessBackend

  test("ready completes once the handle is ready, and the handle can then be used without waiting") {
    var outcome = ""
    running { r =>
      val (in, out) = r.handles.pipe()
      try {
        val buf = arena.allocate(16)
        val ready = r.ops.ready(in, Interest.Read)
        val waiting = Future(r.perform(ready))
        r.perform(r.ops.timer(20))
        r.handles.writeNow(out, arena.of("ready".getBytes(StandardCharsets.UTF_8)))
        waiting.await
        val read = r.handles.readNow(in, buf)
        outcome = s"${text(buf)} ($read bytes)"
      } finally {
        r.perform(r.ops.close(in))
        r.perform(r.ops.close(out))
      }
    }
    assertEquals(outcome, "ready (5 bytes)")
  }

}
