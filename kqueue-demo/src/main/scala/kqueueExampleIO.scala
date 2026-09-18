package example

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.libc.string
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.unsafe.Bracket
import asyncio.unsafe.Bracket.FileOperation
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets

/** Building blocks shared by the kqueue demos: a byte buffer, event registration, a polling loop, and non-blocking I/O
  * that reports would-block and EOF as values instead of exceptions.
  */
object KQueueExampleIO {

  /** A fixed-capacity byte buffer with NIO-style filling and draining. A new frame is in filling mode: `push` and
    * `ioReadBytes` add bytes at `position` up to `capacity`. After `flip()` it is in draining mode: `atPosition` and
    * `remaining` describe the bytes still to be written or decoded, and `advance` consumes them. `clear()` empties the
    * frame for reuse.
    */
  final class Frame(val capacity: Int = 8192) {
    val data: Array[Byte] = new Array[Byte](capacity)
    private var _position = 0
    private var _limit = capacity
    private var draining = false

    def position: Int = _position

    /** The number of bytes that can still be filled, or that remain to be drained. */
    def remaining: Int = _limit - _position
    def isEmpty: Boolean = remaining == 0

    /** The number of bytes filled so far, or, once flipped, the number filled before the flip. */
    def length: Int = if draining then _limit else _position

    def push(byte: Byte): Unit = {
      require(remaining > 0, "Frame is full")
      data(_position) = byte
      _position += 1
    }

    def pushAll(bytes: Array[Byte]): Unit = {
      require(bytes.length <= remaining, s"Frame has room for $remaining of ${bytes.length} bytes")
      Array.copy(bytes, 0, data, _position, bytes.length)
      _position += bytes.length
    }

    /** A pointer to the current position; valid while the frame is reachable. */
    def atPosition: Ptr[Byte] = data.at(_position)

    def advance(n: Int): Unit = {
      require(n >= 0 && n <= remaining, s"Cannot advance $n bytes with $remaining remaining")
      _position += n
    }

    /** Switches from filling to draining: the bytes filled so far become the drainable range. */
    def flip(): Unit = {
      _limit = _position
      _position = 0
      draining = true
    }

    /** Empties the frame and switches back to filling. */
    def clear(): Unit = {
      _position = 0
      _limit = capacity
      draining = false
    }

    /** The filled bytes decoded as UTF-8. */
    def text: String = new String(data, 0, length, StandardCharsets.UTF_8)
  }

  object Frame {

    /** A frame in draining mode holding exactly `bytes`. */
    def of(bytes: Array[Byte]): Frame = {
      val frame = new Frame(math.max(bytes.length, 1))
      frame.pushAll(bytes)
      frame.flip()
      frame
    }

    def of(message: String): Frame = of(message.getBytes(StandardCharsets.UTF_8))
  }

  /** The stream demos prefix each message with a four-byte big-endian length. */
  object StreamProtocol {
    inline val HeaderLength = 4
    inline val MaxMessageLength = 1024 * 1024

    /** Fills `frame` with the framed message and flips it, ready to send. */
    def frame(message: String, frame: Frame): Unit = {
      val bytes = message.getBytes(StandardCharsets.UTF_8)
      frame.clear()
      frame.push((bytes.length >>> 24).toByte)
      frame.push((bytes.length >>> 16).toByte)
      frame.push((bytes.length >>> 8).toByte)
      frame.push(bytes.length.toByte)
      frame.pushAll(bytes)
      frame.flip()
    }

    /** Decodes the length header filled into `header`; negative values mean an invalid frame. */
    def messageLength(header: Frame): Int = {
      val d = header.data
      ((d(0) & 0xff) << 24) | ((d(1) & 0xff) << 16) | ((d(2) & 0xff) << 8) | (d(3) & 0xff)
    }
  }

  // Event registration

  def registerRead(kq: Int, fd: Int, clear: Boolean = false): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addFile(events(0), fd, read = true, clear = clear)
    }

  def registerWrite(kq: Int, fd: Int, clear: Boolean = false): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addFile(events(0), fd, read = false, clear = clear)
    }

  /** Stops watching for writability and starts watching for readability. */
  def switchToRead(kq: Int, fd: Int): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
      KqueueLoop.deleteFile(events(0), fd, read = false)
      KqueueLoop.addFile(events(1), fd, read = true, clear = false)
    }

  /** Stops watching for readability and starts watching for writability. */
  def switchToWrite(kq: Int, fd: Int): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 2) { events =>
      KqueueLoop.deleteFile(events(0), fd, read = true)
      KqueueLoop.addFile(events(1), fd, read = false, clear = false)
    }

  def registerTimerOneShot(kq: Int, id: Int, milliseconds: Int): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addTimerOneShot(events(0), id.toUSize, milliseconds)
    }

  // Polling

  def describe(event: Event): String =
    s"Event triggered: ID = ${KqueueLoop.ident(event)}, Filter = ${KqueueLoop.filter(event)}, Data = ${KqueueLoop.data(event)}"

  /** Polls `kq` until `handle` returns false, passing up to `capacity` events per poll. Error events throw. */
  def pollLoop(kq: Int, capacity: Int)(handle: Event => Boolean): Unit =
    pollLoop(kq, capacity, timeoutSeconds = -1)(() => true)(handle)

  /** Like the untimed `pollLoop`, but `onTimeout` runs whenever `timeoutSeconds` pass without an event and decides
    * whether polling continues.
    */
  def pollLoop(kq: Int, capacity: Int, timeoutSeconds: Int)(onTimeout: () => Boolean)(
      handle: Event => Boolean
  ): Unit =
    KqueueLoop.pollQueue(capacity) { events =>
      var doPoll = true
      while doPoll do {
        val polled =
          if timeoutSeconds < 0 then KqueueLoop.pollEventsForever(kq, events, capacity)
          else KqueueLoop.pollEventsTimeout(kq, events, capacity, timeoutSeconds, 0)
        if polled == 0 then doPoll = onTimeout()
        var i = 0
        while i < polled && doPoll do {
          val event = events(i)
          if KqueueLoop.isError(event) then throw new IOException(s"Event error: ${KqueueLoop.errno(event)}")
          doPoll = handle(event)
          i += 1
        }
      }
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

  /** Fills `frame` from `fd`. Returns the number of bytes read, 0 if the read would block, or -1 at EOF. */
  def nioReadBytes(fd: Int, frame: Frame): Int = {
    val read = unistd.read(fd, frame.atPosition, frame.remaining.toCSize)
    if read < 0 then
      if !wouldBlock then throw new IOException(s"Failed to read from descriptor $fd: ${cError()}")
      0
    else if read == 0 then EOF
    else
      frame.advance(read.toInt)
      read.toInt
  }

  /** Drains `frame` into `fd`. Returns the number of bytes written, or -1 if the write would block. */
  def nioWriteBytes(fd: Int, frame: Frame): Int = {
    val written = unistd.write(fd, frame.atPosition, frame.remaining.toCSize)
    if written < 0 then {
      if !wouldBlock then throw new IOException(s"Failed to write to descriptor $fd: ${cError()}")
      -1
    } else {
      frame.advance(written.toInt)
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

    /** Receives one packet into `frame`, leaving it in draining mode, and records the sender. Returns the packet size,
      * or -1 if no packet is ready.
      */
    def receive(fd: Int, frame: Frame): Int = {
      frame.clear()
      !lengthPtr = sizeOf[socket.sockaddr_storage].toUInt
      val read = PosixSockets.receiveDatagram(fd, frame.atPosition, frame.remaining, address, lengthPtr)
      if read >= 0 then {
        frame.advance(read)
        frame.flip()
      }
      read
    }

    /** Sends the drainable bytes of `frame` back to the recorded sender, consuming them. Returns false, leaving the
      * frame intact for a retry, if it would block.
      */
    def reply(fd: Int, frame: Frame): Boolean =
      consumeIfSent(frame, PosixSockets.sendDatagram(fd, frame.atPosition, frame.remaining, address, length))
  }

  /** Sends the drainable bytes of `frame` to the peer of a connected datagram socket, consuming them. Returns false,
    * leaving the frame intact for a retry, if it would block.
    */
  def sendDatagram(fd: Int, frame: Frame): Boolean =
    consumeIfSent(frame, PosixSockets.sendDatagram(fd, frame.atPosition, frame.remaining))

  /** A datagram is sent whole or not at all, so a successful send drains the entire frame. */
  private def consumeIfSent(frame: Frame, sent: Boolean): Boolean = {
    if sent then frame.advance(frame.remaining)
    sent
  }
}
