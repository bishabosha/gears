package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import asyncio.unsafe.KqueueLoop
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileRead {
  def run(fifo: String): Unit = {
    KqueueLoop.scoped { kq =>
      withFile(fifo, write = false) { fd =>
        registerRead(kq, fd, clear = true)
        println("Event registered for file read.")
        val chunk = new Frame(1024)
        val contents = new ByteArrayOutputStream()

        inline val ReadAgain = true
        inline val Stop = false

        /** Reads everything currently available; returns false once the writer has closed the FIFO. */
        def drain(): Boolean = {
          var isEOF = false
          while {
            chunk.clear()
            val read = nioReadBytes(fd, chunk)
            if read < 0 then {
              isEOF = true
              Stop
            } else if read == 0 then {
              println("No more data available to read (EAGAIN or EWOULDBLOCK).")
              Stop
            } else {
              println(s"actually read $read bytes")
              contents.write(chunk.data, 0, read)
              ReadAgain
            }
          } do ()
          isEOF
        }

        println("starting to poll...")
        pollLoop(kq, capacity = 1) { event =>
          println(describe(event))
          assert(KqueueLoop.isReadEvent(event) && KqueueLoop.fileIdent(event) == fd)
          println(s"Bytes available to read: ${KqueueLoop.rwAvailable(event)} (atEOF: ${KqueueLoop.isEOF(event)})")
          val EOF = drain()
          if EOF then
            println(s"End of file reached. Contents are ${contents.size} long.")
            println(s"File contents: `${contents.toString(StandardCharsets.UTF_8)}`")
            contents.reset() // Start over for the next writer
          true
        }
      }
    }
  }
}
