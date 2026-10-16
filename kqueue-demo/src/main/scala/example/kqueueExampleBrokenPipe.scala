package example

import java.io.IOException
import scala.scalanative.unsafe.Zone

import asyncio.kqueue.KqueueHandles
import asyncio.unsafe.NativeBuffer

/** Writes to a pipe and to a FIFO, opened through `KqueueHandles`, after their readers have closed: each write must
  * fail with "Broken pipe", rather than raise `SIGPIPE` and end the process.
  */
object KQueueExampleBrokenPipe {

  /** What writing to `fd` did: written, or why it failed. */
  private def attempt(fd: Int): String =
    Zone.acquire { implicit z =>
      try {
        KqueueHandles.writeNow(fd, NativeBuffer.of("hello".getBytes))
        "written"
      } catch case e: IOException => s"failed: ${e.getMessage}"
    }

  def run(fifo: String): Unit = {
    val (readEnd, writeEnd) = KqueueHandles.pipe()
    KqueueHandles.close(readEnd)
    println(s"pipe write after its reader closed: ${attempt(writeEnd)}")
    KqueueHandles.close(writeEnd)

    val reader = KqueueHandles.openFile(fifo, write = false) // a non-blocking writer needs a reader to open
    val writer = KqueueHandles.openFile(fifo, write = true)
    KqueueHandles.close(reader)
    println(s"fifo write after its reader closed: ${attempt(writer)}")
    KqueueHandles.close(writer)
    println("still running")
  }
}
