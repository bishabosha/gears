package example

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.Bracket
import asyncio.unsafe.Bracket.FileOperation
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets

/** Building blocks shared by the kqueue demos: pointer-backed buffers, the stream framing, non-blocking I/O that
  * reports would-block and EOF as values instead of exceptions, and datagram peers.
  */
object KQueueExampleIO {

  /** Decodes the bytes between position and limit as UTF-8 without moving the position. */
  def text(buf: ByteBuffer): String = {
    val bytes = new Array[Byte](buf.remaining())
    buf.duplicate().get(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  /** Appends the bytes between position and limit to `out`, consuming them. */
  def drainTo(buf: ByteBuffer, out: ByteArrayOutputStream): Unit = {
    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)
    out.write(bytes)
  }

  /** The stream demos prefix each message with a four-byte big-endian length. */
  object StreamProtocol {
    val HeaderLength = 4
    val MaxMessageLength: Int = 1024 * 1024

    /** Fills `buf` with the framed message and flips it, ready to send. */
    def frame(message: String, buf: ByteBuffer): Unit = {
      val bytes = message.getBytes(StandardCharsets.UTF_8)
      buf.clear()
      buf.putInt(bytes.length)
      buf.put(bytes)
      buf.flip()
    }

    /** Decodes the length header at the start of `header`; negative values mean an invalid frame. */
    def messageLength(header: ByteBuffer): Int = header.getInt(0)
  }

  // Non-blocking I/O

  private def wouldBlock: Boolean =
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR

  inline val EOF = -1

  /** Accepts a new connection on `serverFd` in non-blocking mode. Returns the client file descriptor, or -1 if no
    * client was accepted.
    */
  def nioAccept(serverFd: Int): Int = {
    val clientFd = socket.accept(serverFd, null, null)
    if clientFd < 0 then {
      val transient = wouldBlock
      if !transient then throw new IOException(s"Failed to accept connection: ${cError()}")
      -1 // no client was accepted
    } else {
      PosixSockets.setNonBlocking(clientFd)
      clientFd
    }
  }

  /** Fills `buf` from `fd` between position and limit. Returns the number of bytes read, 0 if the read would block, or
    * -1 at EOF.
    */
  def nioReadBytes(fd: Int, buf: ByteBuffer): Int = {
    val read = unistd.read(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize)
    if read < 0 then
      if !wouldBlock then throw new IOException(s"Failed to read from descriptor $fd: ${cError()}")
      0
    else if read == 0 then EOF
    else
      NativeBuffer.advance(buf, read.toInt)
      read.toInt
  }

  /** Drains `buf` into `fd` between position and limit. Returns the number of bytes written, or -1 if the write would
    * block.
    */
  def nioWriteBytes(fd: Int, buf: ByteBuffer): Int = {
    val written = unistd.write(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize)
    if written < 0 then {
      if !wouldBlock then throw new IOException(s"Failed to write to descriptor $fd: ${cError()}")
      -1
    } else {
      NativeBuffer.advance(buf, written.toInt)
      written.toInt
    }
  }

  /** Opens `path` non-blocking for reading or writing and closes it after `use`. */
  def withFile(path: String, write: Boolean)(use: FileOperation): Unit = {
    println(s"attempt to open file $path")
    val fd = Zone.acquire { implicit z =>
      fcntl.open(toCString(path), (if write then fcntl.O_WRONLY else fcntl.O_RDONLY) | fcntl.O_NONBLOCK)
    }
    if fd < 0 then throw new IOException(s"Failed to open file: ${cError()}")
    println(s"opened file descriptor: $fd (file: $path)")
    Bracket.fileResource(fd)(closeFile)(use)
  }

  private def closeFile(fd: Int): Unit =
    if unistd.close(fd) < 0 then throw new IOException(s"Failed to close file descriptor: ${cError()}")

  // Datagrams

  /** The address of a datagram's sender, captured on receive and reusable as a reply destination. */
  final class PeerAddress(using Zone) {
    private val storage = alloc[socket.sockaddr_storage]()
    private val lengthPtr = alloc[socket.socklen_t]()

    def address: Ptr[socket.sockaddr] = storage.asInstanceOf[Ptr[socket.sockaddr]]
    def length: socket.socklen_t = !lengthPtr

    /** Receives one packet into `buf`, leaving it flipped for draining, and records the sender. Returns the packet
      * size, or -1 if no packet is ready.
      */
    def receive(fd: Int, buf: ByteBuffer): Int = {
      buf.clear()
      !lengthPtr = sizeOf[socket.sockaddr_storage].toUInt
      val read = PosixSockets.receiveDatagram(fd, NativeBuffer.atPosition(buf), buf.remaining(), address, lengthPtr)
      if read >= 0 then {
        NativeBuffer.advance(buf, read)
        buf.flip()
      }
      read
    }

    /** Sends the bytes between position and limit back to the recorded sender, consuming them. Returns false, leaving
      * the buffer intact for a retry, if it would block.
      */
    def reply(fd: Int, buf: ByteBuffer): Boolean =
      consumeIfSent(buf, PosixSockets.sendDatagram(fd, NativeBuffer.atPosition(buf), buf.remaining(), address, length))
  }

  /** Sends the bytes between position and limit to the peer of a connected datagram socket, consuming them. Returns
    * false, leaving the buffer intact for a retry, if it would block.
    */
  def sendDatagram(fd: Int, buf: ByteBuffer): Boolean =
    consumeIfSent(buf, PosixSockets.sendDatagram(fd, NativeBuffer.atPosition(buf), buf.remaining()))

  /** A datagram is sent whole or not at all, so a successful send drains the entire buffer. */
  private def consumeIfSent(buf: ByteBuffer, sent: Boolean): Boolean = {
    if sent then buf.position(buf.limit())
    sent
  }
}
