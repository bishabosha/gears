package asyncio.unsafe

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsigned.*

import PosixErr.cError

/** Non-blocking reads, writes, and accepts that report would-block and EOF as values instead of exceptions. The
  * descriptors must be non-blocking: the calls go through `Unblocked`, which tells the GC nothing, as they return at
  * once.
  */
object NonBlocking {

  private def wouldBlock: Boolean =
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR

  inline val EOF = -1

  /** Accepts a new connection on `serverFd`, which must be non-blocking, and returns it non-blocking too. Returns the
    * client file descriptor, or -1 if no client was accepted. On macOS and FreeBSD that takes the `accept` alone: an
    * accepted socket has the listening socket's properties, `O_NONBLOCK` among them (see accept(2)).
    */
  def accept(serverFd: Int): Int = {
    val clientFd = Unblocked.accept(serverFd, null, null)
    if clientFd < 0 then {
      val transient = wouldBlock
      if !transient then throw new IOException(s"Failed to accept connection: ${cError()}")
      -1 // no client was accepted
    } else {
      if !(LinktimeInfo.isMac || LinktimeInfo.isFreeBSD) then PosixSockets.setNonBlocking(clientFd)
      clientFd
    }
  }

  /** Fills `buf` from `fd` between position and limit. Returns the number of bytes read, 0 if the read would block, or
    * -1 at EOF.
    */
  def read(fd: Int, buf: ByteBuffer): Int = {
    val read = Unblocked.read(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize)
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
  def write(fd: Int, buf: ByteBuffer): Int = {
    val written = Unblocked.write(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize)
    if written < 0 then {
      if !wouldBlock then throw new IOException(s"Failed to write to descriptor $fd: ${cError()}")
      -1
    } else {
      NativeBuffer.advance(buf, written.toInt)
      written.toInt
    }
  }
}
