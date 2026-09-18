package example

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileWrite {

  /** Owns one buffer holding the message, reloaded before every write. */
  private final class Writer(fd: Int, message: String)(using Zone) {
    val bytes: Array[Byte] = message.getBytes(StandardCharsets.UTF_8)
    private val buf = NativeBuffer.allocate(math.max(bytes.length, 1))

    /** Writes the whole message once. Returns the bytes written, or -1 if the write would block. */
    def write(): Int = {
      buf.clear()
      buf.put(bytes)
      buf.flip()
      nioWriteBytes(fd, buf)
    }
  }

  def run(fifo: String, message: String): Unit = {
    Reactor.scoped { r =>
      withFile(fifo, write = true) { fd =>
        r.registerWrite(fd, clear = true)
        println("Event registered for file write.")
        Zone.acquire { implicit z =>
          val writer = new Writer(fd, message)
          println("starting to poll...")
          r.run(capacity = 1) { event =>
            println(r.describe(event))
            assert(r.isWriteEvent(event) && r.fileIdent(event) == fd)
            val available = r.available(event)
            println(s"Bytes available to write: $available")
            val length = writer.bytes.length
            assert(length <= available, s"Message length ($length) exceeds available bytes ($available)")
            // The message is sent again on every writable event, until the reader stops draining the FIFO.
            val written = writer.write()
            if written < 0 then println("No data available to write (EAGAIN or EWOULDBLOCK).")
            else println(s"actually wrote $written bytes")
            true
          }
        }
      }
    }
  }
}
