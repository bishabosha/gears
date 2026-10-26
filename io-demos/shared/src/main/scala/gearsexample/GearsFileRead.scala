package gearsexample

import gears.async.io.FifoBackend
import gears.async.io.Interest
import gears.async.io.ReactorFutures
import gears.async.io.ReadinessBackend
import gears.async.io.testkit.BufferArena

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import DemoIO.drainTo
import higherlevel.HigherLevel

/** Mirror of `example.KQueueExampleFileRead`. Use with a FIFO. */
object GearsFileRead {
  def run(fifo: String)(using FifoBackend & ReadinessBackend): Unit = {
    BufferArena.scoped { arena =>
      val chunk = arena.allocate(1024)
      val contents = new ByteArrayOutputStream()
      ReactorFutures.run { io ?=>
        println(s"attempt to open file $fifo")
        val file = HigherLevel.openFifo(fifo, write = false)
        try {
          println(s"opened file: $file")

          /** Reads everything available once the FIFO is readable; done only once the writer has closed it, so until
            * then the reactor keeps waiting without completing.
            */
          def drain(): Boolean = {
            var result: Boolean | Null = null
            while result == null do {
              chunk.clear()
              val read = io.backend.ops.readNow(file, chunk)
              if read < 0 then result = true
              else if read == 0 then {
                println("No more data available to read (EAGAIN or EWOULDBLOCK).")
                result = false
              } else {
                println(s"actually read $read bytes")
                chunk.flip()
                drainTo(chunk, contents)
              }
            }
            result.nn
          }

          // One repeatable op, performed again before each drain.
          val readable = io.backend.ops.ready(file, Interest.Read)
          println("Event registered for file read.")
          println("starting to poll...")
          while true do {
            io.perform(readable) // completes once the FIFO is readable, at its end included
            if drain() then { // end of file: everything the writer sent is in `contents`
              println(s"End of file reached. Contents are ${contents.size} long.")
              println(s"File contents: `${contents.toString(StandardCharsets.UTF_8)}`")
              contents.reset() // Start over for the next writer
            }
          }
        } finally io.perform(io.backend.ops.close(file))
      }
    }
  }
}
