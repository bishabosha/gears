package example

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Completion
import asyncio.reactor.Reactor
import asyncio.reactor.WriteFromBuffer
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileWrite {

  /** Writes the message again after every completed write, until the reader stops draining the FIFO. */
  private final class Writer(fd: Int, message: String)(using Zone) extends Completion[WriteFromBuffer] {
    private val bytes = message.getBytes(StandardCharsets.UTF_8)
    private val buf = NativeBuffer.allocate(math.max(bytes.length, 1))

    /** Loads the whole message and asks for it to be written. */
    def start(r: Reactor): Unit = {
      buf.clear()
      buf.put(bytes)
      buf.flip()
      r.submit(WriteFromBuffer(fd, buf), this)
    }

    def onComplete(r: Reactor, command: WriteFromBuffer, written: Int): Unit = {
      if written < 0 then println("No data available to write (EAGAIN or EWOULDBLOCK).")
      else println(s"actually wrote $written bytes")
      start(r)
    }
  }

  def run(fifo: String, message: String): Unit = {
    Reactor.scoped { r =>
      withFile(fifo, write = true) { fd =>
        Zone.acquire { implicit z =>
          val writer = new Writer(fd, message)
          writer.start(r)
          println("Event registered for file write.")
          println("starting to poll...")
          r.run(capacity = 1)
        }
      }
    }
  }
}
