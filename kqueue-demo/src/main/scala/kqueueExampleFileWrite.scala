package example

import java.io.IOException
import scala.scalanative.libc.errno
import scala.scalanative.libc.string
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.unistd
import scala.scalanative.posix.{errno => perrno}
import scala.scalanative.unsafe.UnsafeRichArray
import scala.scalanative.unsafe.Zone
import scala.scalanative.unsafe.fromCString
import scala.scalanative.unsafe.toCString
import scala.scalanative.unsigned.UnsignedRichInt

import asyncio.unsafe.Bracket
import asyncio.unsafe.KqueueLoop

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileWrite {
  def run(fifo: String, msg0: String): Unit = {
    Bracket.fileResource(KqueueLoop.open())(KqueueLoop.close) { kq =>
      val fd = Zone.acquire { implicit z =>
        println(s"attempt to open file ${fifo}")
        val path = toCString(fifo)
        fcntl.open(path, fcntl.O_WRONLY | fcntl.O_NONBLOCK)
      }
      if (fd < 0) {
        throw new IOException(
          s"Failed to open file: ${fromCString(string.strerror(errno.errno))}"
        )
      }
      println(s"opened file descriptor: $fd (file: ${fifo})")
      KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
        KqueueLoop.addFile(events(0), fd, read = false, clear = true)
      }
      println("Event registered for file write.")
      try {
        KqueueLoop.pollQueue(1) { polledEvents =>
          while (true) {
            // Wait for the event to be triggered
            println(
              s"starting to poll..."
            )
            val nev =
              KqueueLoop.pollEventsForever(kq, polledEvents, 1) // infinite wait
            if (nev == 0) {
              println("No events triggered within the timeout period.")
            } else {
              val event = polledEvents(0)
              println(
                s"Event triggered: ID = ${KqueueLoop.ident(event)}, Filter = ${KqueueLoop.filter(event)}, Data = ${KqueueLoop.data(event)}"
              )
              assert(
                KqueueLoop.isWriteEvent(event) && KqueueLoop.ident(event) == fd.toUSize
              )
              val available = KqueueLoop.data(event).toInt
              println(s"Bytes available to write: $available")
              val msg = msg0.getBytes()
              assert(
                msg.length <= available,
                s"Message length (${msg.length}) exceeds available bytes ($available)"
              )
              var continue = true
              while (continue) {
                // read all data until EOF or EAGAIN
                val buf = msg.at(0)
                var bytesWritten = unistd.write(
                  fd,
                  buf,
                  msg.length.toCSize
                )
                if (bytesWritten < 0) {
                  if (errno.errno != perrno.EAGAIN && errno.errno != perrno.EWOULDBLOCK) {
                    throw new IOException(
                      s"Failed to write file: ${fromCString(string.strerror(errno.errno))}"
                    )
                  } else {
                    println(
                      "No data available to write (EAGAIN or EWOULDBLOCK)."
                    )
                    continue = false // Exit the loop if no more data is available
                  }
                } else {
                  println(s"actually wrote $bytesWritten bytes")
                  continue = false // Exit after writing the message
                }
              }
            }
          }
        }
      } finally {
        val st = unistd.close(fd)
        if (st < 0) {
          throw new IOException(
            s"Failed to close file descriptor: ${fromCString(string.strerror(errno.errno))}"
          )
        }
      }
    }
  }
}
