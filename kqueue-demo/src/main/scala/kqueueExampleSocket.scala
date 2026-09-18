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

  def registerWrite(kq: Int, clientFd: Int): Unit = {
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      // Keep receiving write events until the entire frame has been sent.
      KqueueLoop.addFile(events(0), clientFd, read = false, clear = false)
    }
  }

  def flipReadToWrite(kq: Int, clientFd: Int): Unit = {
    KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
      KqueueLoop.deleteFile(events(0), clientFd, read = false)
      KqueueLoop.addFile(events(1), clientFd, read = true, clear = false)
    }
  }

  class Frame private (private var _offset: Int, private var lim: Int) {
    def this() = this(0, -1)
    val data: Array[Byte] = new Array[Byte](8192)
    def push(b: Byte): Unit = {
      if (_offset < 8192) {
        data(_offset) = b
        _offset += 1
      }
    }
    def pushAll(bytes: Array[Byte]): Unit = {
      Array.copy(bytes, 0, data, _offset, bytes.length)
      _offset += bytes.length
    }

    def atOffset: Ptr[Byte] = {
      data.at(_offset)
    }

    def flip(): Unit = {
      lim = _offset
      _offset = 0
    }
    def capacity: Int = 8192
    def offset: Int = _offset
    def remaining: Int = {
      checkReadable()
      lim - _offset
    }
    def read(n: Int): Unit = {
      checkReadable()
      _offset += n
    }
    def empty: Boolean = remaining == 0
    def checkReadable(): Unit =
      if lim < 0 then throw IllegalStateException("not readable")
    def length: Int = {
      checkReadable()
      lim
    }
  }

  def makeFrame(message: String): Frame = {
    val msg = message.getBytes(StandardCharsets.UTF_8)
    val f = Frame()
    f.push((msg.length >>> 24).toByte)
    f.push((msg.length >>> 16).toByte)
    f.push((msg.length >>> 8).toByte)
    f.push(msg.length.toByte)
    f.pushAll(msg)
    f.flip()
    f
  }

  def ioReadBytes(clientFd: Int, frame: Frame): Int = {
    val read = unistd.read(clientFd, frame.atOffset, frame.capacity.toCSize)
    if (read < 0) {
      if (errno.errno != errno.EAGAIN && errno.errno != errno.EWOULDBLOCK && errno.errno != errno.EINTR) {
        throw new IOException(s"Failed to read from socket: ${cError()}")
      }
      0 // nothing read
    } else if (read == 0) {
      -1 // EOF
    } else {
      frame.read(read)
      read // number of bytes read
    }
  }

  def ioWriteBytes(clientFd: Int, frame: Frame): Int = {
    val written = unistd.write(clientFd, frame.atOffset, frame.remaining.toCSize)
    if (written < 0) {
      if (errno.errno != errno.EAGAIN && errno.errno != errno.EWOULDBLOCK && errno.errno != errno.EINTR) {
        throw new IOException(s"Failed to write to socket: ${cError()}")
      }
    } else {
      frame.read(written)
    }
    written
  }

  case class ClientSock(val clientFd: Int) {
    private var connected = false

    def ensureConnected(): Unit = {
      if !connected then {
        PosixSockets.checkConnect(clientFd)
        connected = true
      }
    }
  }

  class Exchange(sock: ClientSock) {
    private val response = new java.io.ByteArrayOutputStream()

    def responseMessage: String = new String(response.toByteArray, StandardCharsets.UTF_8)

    def readResponse(frame: Frame): Int = {
      val read = ioReadBytes(sock.clientFd, frame)
      if read > 0 then
        response.write(frame.data, 0, read)
        println(s"Read $read bytes from socket.")
      read
    }

    def writeMessage(frame: Frame): Unit = {
      val written = ioWriteBytes(sock.clientFd, frame)
      println(s"Wrote $written bytes to socket.")
    }
  }

  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    KqueueLoop.scoped { kq =>
      address.withSocket(Transport.Stream) { clientFd =>
        println(s"opened file descriptor: $clientFd (socket-type)")
        address.connect(clientFd)
        println(s"connection in progress to: $address")
        registerWrite(kq, clientFd)
        val clientSock = ClientSock(clientFd)
        val client = Exchange(clientSock)
        val frame = makeFrame("Hello from Scala Native KQueue Example!\n")
        KqueueLoop.pollQueue(255) { polledEvents =>
          var doPoll = true
          while doPoll do {
            val events = KqueueLoop.pollEventsForever(kq, polledEvents, 255)
            var i = 0
            while i < events && doPoll do {
              val evt = polledEvents(i)
              if KqueueLoop.isError(evt) then throw new IOException(s"Socket event error: ${KqueueLoop.errno(evt)}")
              assert(KqueueLoop.fileIdent(evt) == clientFd, "Unexpected file descriptor for read event.")
              if KqueueLoop.isWriteEvent(evt) then {
                clientSock.ensureConnected()
                client.writeMessage(frame)
                if frame.remaining == 0 then {
                  frame.flip()
                  flipReadToWrite(kq, clientFd)
                }
              } else if KqueueLoop.isReadEvent(evt) then {
                clientSock.ensureConnected()
                val eof = client.readResponse(frame) < 0
                if eof then {
                  println(s"Received message: `${client.responseMessage}`")
                }
                doPoll = !eof
              }
              i += 1
            }
          }
        }
      }
    }
  }
}
