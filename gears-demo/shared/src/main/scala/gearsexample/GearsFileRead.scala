package gearsexample

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import asyncio.FifoBackend
import asyncio.ReadinessBackend
import asyncio.Interest
import asyncio.testkit.BufferArena
import DemoIO.drainTo
import asyncio.gears.ReactorFutures
import asyncio.gears.ReactorFutures.*

/** Mirror of `example.KQueueExampleFileRead`. Use with a FIFO. */
object GearsFileRead {
  def run(fifo: String)(using FifoBackend & ReadinessBackend): Unit = {
    BufferArena.scoped { arena =>
      val chunk = arena.allocate(1024)
      val contents = new ByteArrayOutputStream()
      ReactorFutures.run { r =>
        println(s"attempt to open file $fifo")
        val file = r.handles.openFifo(fifo, write = false)
        try {
          println(s"opened file: $file")

          /** Reads everything available once the FIFO is readable; done only once the writer has closed it, so until
            * then the reactor keeps waiting without completing.
            */
          def drain(): Boolean = {
            var result: Boolean | Null = null
            while result == null do {
              chunk.clear()
              val read = r.handles.readNow(file, chunk)
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

          val readable = r.ops.whenReady(file, Interest.Read)(drain) // one repeatable op, resubmitted each time
          println("Event registered for file read.")
          println("starting to poll...")
          while true do {
            perform(r, readable) // completes only at end of file
            println(s"End of file reached. Contents are ${contents.size} long.")
            println(s"File contents: `${contents.toString(StandardCharsets.UTF_8)}`")
            contents.reset() // Start over for the next writer
          }
        } finally r.handles.close(file)
      }
    }
  }
}
