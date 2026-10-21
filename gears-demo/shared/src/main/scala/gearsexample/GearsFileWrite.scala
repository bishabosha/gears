package gearsexample

import java.nio.charset.StandardCharsets

import asyncio.FifoBackend
import asyncio.testkit.BufferArena
import asyncio.gears.ReactorFutures
import asyncio.gears.ReactorFutures.*

/** Mirror of `example.KQueueExampleFileWrite`: writes the message again after every completed write. */
object GearsFileWrite {
  def run(fifo: String, message: String)(using FifoBackend): Unit = {
    BufferArena.scoped { arena =>
      val bytes = message.getBytes(StandardCharsets.UTF_8)
      val buf = arena.allocate(math.max(bytes.length, 1))
      ReactorFutures.run { r =>
        println(s"attempt to open file $fifo")
        val file = r.handles.openFifo(fifo, write = true)
        try {
          println(s"opened file: $file")
          println("Event registered for file write.")
          println("starting to poll...")
          val write = r.ops.write(file, buf) // one repeatable op, resubmitted for every write
          while true do {
            buf.clear()
            buf.put(bytes)
            buf.flip()
            perform(r, write)
            println(s"actually wrote ${buf.position()} bytes") // the buffer was loaded at position 0
          }
        } finally perform(r, r.ops.close(file))
      }
    }
  }
}
