package example

import java.nio.charset.StandardCharsets

import asyncio.unsafe.KqueueLoop
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileWrite {
  def run(fifo: String, message: String): Unit = {
    KqueueLoop.scoped { kq =>
      withFile(fifo, write = true) { fd =>
        registerWrite(kq, fd, clear = true)
        println("Event registered for file write.")
        val bytes = message.getBytes(StandardCharsets.UTF_8)
        println("starting to poll...")
        pollLoop(kq, capacity = 1) { event =>
          println(describe(event))
          assert(KqueueLoop.isWriteEvent(event) && KqueueLoop.fileIdent(event) == fd)
          val available = KqueueLoop.rwAvailable(event)
          println(s"Bytes available to write: $available")
          assert(bytes.length <= available, s"Message length (${bytes.length}) exceeds available bytes ($available)")
          // The message is sent again on every writable event, until the reader stops draining the FIFO.
          val written = nioWriteBytes(fd, Frame.of(bytes))
          if written < 0 then println("No data available to write (EAGAIN or EWOULDBLOCK).")
          else println(s"actually wrote $written bytes")
          true
        }
      }
    }
  }
}
