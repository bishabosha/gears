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
object KQueueExampleFileRead {
  def run(fifo: String): Unit = {
    KqueueLoop.scoped { kq =>
      val fd = Zone.acquire { implicit z =>
        println(s"attempt to open file ${fifo}")
        val path = toCString(fifo)
        fcntl.open(path, fcntl.O_RDONLY | fcntl.O_NONBLOCK)
      }
      if (fd < 0) {
        throw new IOException(
          s"Failed to open file: ${fromCString(string.strerror(errno.errno))}"
        )
      }
      println(s"opened file descriptor: $fd (file: ${fifo})")
      KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
        KqueueLoop.addFile(events(0), fd, read = true, clear = true)
      }
      println("Event registered for file read.")
      var byteArray = null.asInstanceOf[Array[Byte]]
      def reset() = {
        byteArray = new Array[Byte](1024)
      }
      var offset = 0
      def dbl() = {
        offset = 0
        byteArray = new Array[Byte](byteArray.length * 2)
      }
      reset() // initialize the byte array
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
                KqueueLoop.isReadEvent(event) && KqueueLoop.ident(event) == fd.toUSize
              )
              val available = KqueueLoop.data(event).toInt
              val atEOF = KqueueLoop.isEOF(event)
              println(s"Bytes available to read: $available (atEOF: $atEOF)")

              var continue = true
              while (continue) {
                // read all data until EOF or EAGAIN
                var bytesRead = unistd.read(
                  fd,
                  byteArray.at(offset),
                  (1024 `min` (byteArray.length - offset)).toCSize
                )
                if (bytesRead < 0) {
                  if (errno.errno != perrno.EAGAIN && errno.errno != perrno.EWOULDBLOCK) {
                    throw new IOException(
                      s"Failed to read file: ${fromCString(string.strerror(errno.errno))}"
                    )
                  } else {
                    println(
                      "No more data available to read (EAGAIN or EWOULDBLOCK)."
                    )
                    continue = false // Exit the loop if no more data is available
                  }
                } else if (bytesRead == 0) {
                  println(s"End of file reached. Contents are $offset long.")
                  println(
                    s"File contents: `${new String(byteArray.take(offset))}`"
                  )
                  reset() // Reset the byte array for the next read
                  continue = false // Exit the loop if no more data is available
                } else {
                  println(s"actually read $bytesRead bytes")
                  offset += bytesRead.toInt
                  if (offset >= byteArray.length) {
                    dbl()
                  }
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
