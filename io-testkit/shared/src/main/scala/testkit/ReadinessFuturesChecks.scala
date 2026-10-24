package gears.async.io
package testkit

import gears.async.Future

import java.nio.charset.StandardCharsets

import gears.async.io.Backend
import gears.async.io.Interest
import gears.async.io.PipeBackend
import gears.async.io.ReadinessBackend
import gears.async.io.ResolveBackend
import gears.async.io.SocketBackend

/** `FuturesContract`'s checks of readiness, for a backend that has it. */
trait ReadinessFuturesChecks extends FuturesContract {
  val backend: Backend & SocketBackend & PipeBackend & ResolveBackend & ReadinessBackend

  test("ready completes once the handle is ready, and the handle can then be used without waiting") {
    val outcome = running { io ?=>
      val (in, out) = pipe(io)
      try {
        val buf = arena.allocate(16)
        val ready = io.backend.ops.ready(in, Interest.Read)
        val waiting = Future(io.perform(ready))
        io.perform(io.backend.ops.timer(20))
        io.backend.ops.writeNow(out, arena.of("ready".getBytes(StandardCharsets.UTF_8)))
        waiting.await
        val read = io.backend.ops.readNow(in, buf)
        s"${text(buf)} ($read bytes)"
      } finally {
        io.perform(io.backend.ops.close(in))
        io.perform(io.backend.ops.close(out))
      }
    }
    assertEquals(outcome, "ready (5 bytes)")
  }

}
