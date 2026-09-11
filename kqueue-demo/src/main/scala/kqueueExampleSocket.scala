package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.Bracket
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport

object KQueueExampleSocket {
  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    Bracket.fileResource(KqueueLoop.open())(KqueueLoop.close) { kq =>
      Bracket.fileResource(PosixSockets.open(address.flavor, Transport.Stream))(PosixSockets.close) { clientFd =>
        println(s"opened file descriptor: $clientFd (socket-type)")
        PosixSockets.setNonBlocking(clientFd)
        address.connect(clientFd)
        println(s"connection in progress to: $address")
        KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
          // Keep receiving write events until the entire frame has been sent.
          KqueueLoop.addFile(events(0), clientFd, read = false, clear = false)
        }
        val message = "Hello from Scala Native KQueue Example!\n".getBytes(StandardCharsets.UTF_8)
        val frame = new Array[Byte](4 + message.length)
        frame(0) = (message.length >>> 24).toByte
        frame(1) = (message.length >>> 16).toByte
        frame(2) = (message.length >>> 8).toByte
        frame(3) = message.length.toByte
        Array.copy(message, 0, frame, 4, message.length)
        var offset = 0
        var connected = false
        val response = new java.io.ByteArrayOutputStream()

        def writeEvent(evt: KqueueLoop.Event): Boolean = {
          assert(KqueueLoop.fileIdent(evt) == clientFd, "Unexpected file descriptor for write event.")
          if (!connected) {
            PosixSockets.checkConnect(clientFd)
            connected = true
          }
          val written = unistd.write(clientFd, frame.at(offset), (frame.length - offset).toCSize)
          if (written < 0) {
            if (errno.errno != errno.EAGAIN && errno.errno != errno.EWOULDBLOCK && errno.errno != errno.EINTR) {
              throw new IOException(s"Failed to write to socket: ${cError()}")
            }
          } else {
            offset += written.toInt
            println(s"Wrote $written bytes to socket.")
          }
          if (offset == frame.length) {
            KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
              KqueueLoop.deleteFile(events(0), clientFd, read = false)
              KqueueLoop.addFile(events(1), clientFd, read = true, clear = false)
            }
          }
          true // Continue polling
        }

        def readEvent(evt: KqueueLoop.Event): Boolean = {
          assert(KqueueLoop.fileIdent(evt) == clientFd, "Unexpected file descriptor for read event.")
          val buf = new Array[Byte](4096)
          val read = unistd.read(clientFd, buf.at(0), buf.length.toCSize)
          if (read < 0) {
            if (errno.errno != errno.EAGAIN && errno.errno != errno.EWOULDBLOCK && errno.errno != errno.EINTR) {
              throw new IOException(s"Failed to read from socket: ${cError()}")
            }
            true
          } else if (read == 0) {
            println(s"Received message: `${new String(response.toByteArray, StandardCharsets.UTF_8)}`")
            false // The server closes the connection after its response.
          } else {
            response.write(buf, 0, read.toInt)
            println(s"Read $read bytes from socket.")
            true
          }
        }

        KqueueLoop.pollQueue(255) { polledEvents =>
          var doPoll = true
          while (doPoll) {
            val events = KqueueLoop.pollEventsForever(kq, polledEvents, 255)
            var i = 0
            while (i < events && doPoll) {
              val evt = KqueueLoop.eventAt(polledEvents, i)
              if (KqueueLoop.isError(evt)) {
                throw new IOException(s"Socket event error: ${KqueueLoop.errno(evt)}")
              } else if (KqueueLoop.isWriteEvent(evt)) {
                doPoll = writeEvent(evt)
              } else if (KqueueLoop.isReadEvent(evt)) {
                doPoll = readEvent(evt)
              }
              i += 1
            }
          }
        }
      }
    }
  }
}
