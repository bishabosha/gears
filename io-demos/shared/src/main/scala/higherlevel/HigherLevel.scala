package higherlevel

import gears.async.Async
import gears.async.BufferedChannel
import gears.async.Channel
import gears.async.Future
import gears.async.io.Backend
import gears.async.io.BufferPool
import gears.async.io.Fifos
import gears.async.io.Io
import gears.async.io.IpAddress
import gears.async.io.IpSockets
import gears.async.io.Pipes
import gears.async.io.Received
import gears.async.io.ResolvedAddress
import gears.async.io.Resolving
import gears.async.io.Slot
import gears.async.io.UnixAddress
import gears.async.io.UnixDatagrams
import gears.async.io.UnixStreams

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException

/** The higher level the interface is meant to be wrapped by, written over `Io` alone: result-returning methods that own
  * the op and the slot the caller would otherwise reuse, and a stream of chunks over a repeatable pooled read, consumed
  * as a gears channel. Nothing here touches a backend, a kind or the runtime, and being outside the `gears.async.io`
  * package it could not: it sees the interface's public contract only, and its module depends on the interface alone.
  * It runs as it is on every backend (`HigherLevelChecks` runs on each, as part of the contract's suites), which is
  * what the lowest level is for; the demos use it where a result or a stream reads better than the op. Every method
  * takes the `Io` as a context parameter, `io`.
  */
object HigherLevel {

  /** Performs the op `open` builds, which opens a handle into the slot it is given, and returns the handle. */
  private def opened(using io: Io[Backend], async: Async)(open: Slot[io.Handle] => io.Op): io.Handle = {
    val into = Slot[io.Handle]()
    io.perform(open(into))
    into.clear()
  }

  /** Connects a stream socket to `address` and returns it. */
  def connect(address: IpAddress)(using io: Io[IpSockets], async: Async): io.Handle =
    opened(io.backend.ops.connect(address, _))

  /** Connects a stream socket to a Unix socket path and returns it. */
  def connect(address: UnixAddress)(using io: Io[UnixStreams], async: Async): io.Handle =
    opened(io.backend.ops.connect(address, _))

  /** Listens on `address` and returns the listener. */
  def listen(address: IpAddress, reuseAddr: Boolean = true)(using io: Io[IpSockets], async: Async): io.Handle =
    opened(io.backend.ops.listen(address, _, reuseAddr))

  /** Listens on a Unix socket path and returns the listener. */
  def listen(address: UnixAddress)(using io: Io[UnixStreams], async: Async): io.Handle =
    opened(io.backend.ops.listen(address, _))

  /** Opens a datagram socket, bound to `local` and connected to `remote` where given, and returns it. */
  def datagram(local: IpAddress | Null, remote: IpAddress | Null)(using io: Io[IpSockets], async: Async): io.Handle =
    opened(io.backend.ops.datagram(local, remote, _))

  /** Opens a Unix datagram socket, bound to `local` and connected to `remote` where given, and returns it. */
  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null)(using
      io: Io[UnixDatagrams],
      async: Async
  ): io.Handle =
    opened(io.backend.ops.datagram(local, remote, _))

  /** Opens a pipe and returns its read end and its write end. */
  def pipe()(using io: Io[Pipes], async: Async): (io.Handle, io.Handle) = {
    val read = Slot[io.Handle]()
    val write = Slot[io.Handle]()
    io.perform(io.backend.ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  /** Opens the FIFO at `path`, for writing with `write`, and returns it. */
  def openFifo(path: String, write: Boolean)(using io: Io[Fifos], async: Async): io.Handle =
    opened(io.backend.ops.openFifo(path, write, _))

  /** Reads into `buf` once and returns how many bytes arrived, 0 at the end of the stream. */
  def read(using io: Io[Backend])(handle: io.Handle, buf: ByteBuffer)(using Async): Int = {
    val before = buf.position()
    io.perform(io.backend.ops.read(handle, buf))
    buf.position() - before
  }

  /** Writes all of `buf`, however many writes it takes. */
  def writeAll(using io: Io[Backend])(handle: io.Handle, buf: ByteBuffer)(using Async): Unit = {
    val write = io.backend.ops.write(handle, buf)
    while buf.hasRemaining do io.perform(write)
  }

  /** Shuts a connected stream down for writing: the peer reads what was written, then the end of the stream, and this
    * side may still read (see `SocketOps.shutdownOutput`).
    */
  def shutdownOutput(using io: Io[IpSockets])(handle: io.Handle)(using Async): Unit =
    io.perform(io.backend.ops.shutdownOutput(handle))

  /** Closes a connected stream without resetting it: shuts it down for writing, so that the peer reads all that was
    * written and then the end, reads into `scratch` and discards until the peer's end, at most `timeoutMillis`, and
    * closes. Closing with bytes from the peer unread would reset the connection, and a reset can discard the reply the
    * peer has not read yet. A peer that resets or never ends its side is closed on anyway.
    */
  def closeGracefully(using
      io: Io[IpSockets]
  )(handle: io.Handle, scratch: ByteBuffer, timeoutMillis: Int = 2000)(using
      Async
  ): Unit =
    try {
      shutdownOutput(handle)
      val read = io.backend.ops.read(handle, scratch)
      val deadline = System.nanoTime() + timeoutMillis * 1_000_000L
      var open = true
      while open do {
        val left = ((deadline - System.nanoTime()) / 1_000_000L).toInt
        if left <= 0 then open = false
        else {
          scratch.clear()
          io.perform(read, left)
          open = scratch.position() > 0 // a read that adds nothing is the peer's end
        }
      }
    } catch {
      case _: IOException | _: TimeoutException => () // reset, or never ended: closed on all the same
    } finally io.perform(io.backend.ops.close(handle))

  /** Accepts one connection and returns it. */
  def accept(using io: Io[IpSockets])(listener: io.Handle)(using Async): io.Handle = {
    val into = Slot[io.Handle]()
    io.perform(io.backend.ops.accept(listener, into))
    into.clear()
  }

  /** Resolves `host` and returns its addresses. */
  def resolve(host: String)(using io: Io[Resolving], async: Async): List[ResolvedAddress] = {
    val into = Slot[List[ResolvedAddress]]()
    io.perform(io.backend.ops.resolve(host, into))
    into.clear()
  }

  /** A stream of chunks, each a `Received` the consumer releases: `read` gives the next, or `None` at the end of the
    * stream, and throws the read's failure if it ended with one; `close` stops the stream early and releases the chunks
    * read ahead and not yet taken, so the pool may be closed afterwards. The channel is never closed, since a closed
    * gears channel drops what it still holds: the end of the stream is a `None` sent after the last chunk.
    */
  final class Chunks private[HigherLevel] (channel: Channel[Option[Received]], reader: Future[Unit]) {

    def read()(using Async): Option[Received] = channel.read() match {
      case Right(Some(chunk)) => Some(chunk)
      case Right(None)        =>
        reader.await // rethrows the failure that ended the stream, if one did
        None
      case Left(Channel.Closed) => None // never closed; for the compiler
    }

    def close()(using Async): Unit = {
      reader.cancel()
      reader.awaitResult // once it has stopped, what it sent is all the channel will ever hold
      var draining = true
      while draining do
        channel.readSource.poll() match {
          case Some(Right(Some(chunk))) => chunk.release()
          case _                        => draining = false
        }
    }
  }

  /** The bytes of `handle` as `Chunks`: a future reads into the pool's buffers with one repeatable op and sends each
    * chunk on a channel of `backlog` chunks, which is the back-pressure: the reader holds at most `backlog` chunks in
    * the channel, one it is sending and one its read is filling, so the pool needs `backlog + 2` buffers, and the
    * consumer's unreleased chunks come out of the rest. Cancelling the group cancels the read, as it would any future's
    * op; a chunk in the reader's hands when it is cancelled, one a `send` did not deliver, is released by the reader
    * itself.
    */
  def chunks(using io: Io[Backend])(handle: io.Handle, pool: BufferPool, backlog: Int)(using Async.Spawn): Chunks = {
    require(pool.buffers >= backlog + 2, s"a backlog of $backlog needs a pool of ${backlog + 2} buffers")
    val channel = BufferedChannel[Option[Received]](backlog)
    val reader = Future {
      val into = Slot[Received]()
      val read = io.backend.ops.read(handle, pool, into)
      var inHand: Received | Null = null
      try {
        var atEnd = false
        while !atEnd do {
          io.perform(read)
          val chunk = into.clear()
          if chunk.buffer.hasRemaining then {
            inHand = chunk
            channel.send(Some(chunk)) // delivered, or thrown with the chunk still in hand, never both
            inHand = null
          } else {
            chunk.release()
            atEnd = true
          }
        }
        channel.send(None)
      } catch {
        case cancelled: CancellationException => throw cancelled // the consumer is closing, and drains the channel
        case failure: Throwable               =>
          channel.send(None) // so that the consumer's next read finds the end, and the failure from `reader`
          throw failure
      } finally if inHand != null then inHand.release()
    }
    new Chunks(channel, reader)
  }

  /** Reads every chunk of `stream` until it ends, releasing each, and returns how many bytes they held. */
  def drain(stream: Chunks)(using Async): Long = {
    var total = 0L
    var open = true
    while open do
      stream.read() match {
        case Some(chunk) =>
          total += chunk.buffer.remaining()
          chunk.release()
        case None => open = false
      }
    total
  }
}
