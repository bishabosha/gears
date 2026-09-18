package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileRead {

  /** Owns the chunk buffer the FIFO is read through and the contents gathered so far. */
  private final class Reader(fd: Int)(using Zone) {
    private val chunk = NativeBuffer.allocate(1024)
    private val contents = new ByteArrayOutputStream()

    inline val ReadAgain = true
    inline val Stop = false

    /** Reads everything currently available; returns true once the writer has closed the FIFO. */
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
          chunk.flip()
          drainTo(chunk, contents)
          ReadAgain
        }
      } do ()
      isEOF
    }

    def report(): Unit = {
      println(s"End of file reached. Contents are ${contents.size} long.")
      println(s"File contents: `${contents.toString(StandardCharsets.UTF_8)}`")
      contents.reset() // Start over for the next writer
    }
  }

  def run(fifo: String): Unit = {
    Reactor.scoped { r =>
      withFile(fifo, write = false) { fd =>
        r.registerRead(fd, clear = true)
        println("Event registered for file read.")
        Zone.acquire { implicit z =>
          val reader = new Reader(fd)
          println("starting to poll...")
          r.run(capacity = 1) { event =>
            println(r.describe(event))
            assert(r.isReadEvent(event) && r.fileIdent(event) == fd)
            println(s"Bytes available to read: ${r.available(event)} (atEOF: ${r.isEOF(event)})")
            if reader.drain() then reader.report()
            true
          }
        }
      }
    }
  }
}
