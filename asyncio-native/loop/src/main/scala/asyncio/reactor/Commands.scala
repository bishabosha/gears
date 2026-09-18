package asyncio.reactor

import java.nio.ByteBuffer

import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixSockets

/** The common commands a library can offer. Fine-grained ones live with the code that needs them. */

/** Fills `buf` between position and limit. Completes with the bytes read, 0 if it would block, or -1 at EOF. */
final case class ReadIntoBuffer(fd: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Int = NonBlocking.read(fd, buf)
}

/** Drains `buf` between position and limit. Completes with the bytes written, or -1 if it would block. */
final case class WriteFromBuffer(fd: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Int = NonBlocking.write(fd, buf)
}

/** Accepts one connection on a listening socket. Completes with a non-blocking client descriptor, or -1. */
final case class Accept(fd: Int) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Int = NonBlocking.accept(fd)
}

/** Waits for a non-blocking connect to finish and checks its outcome. Completes with 0, or throws. */
final case class Connect(fd: Int) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Int = {
    PosixSockets.checkConnect(fd)
    0
  }
}
