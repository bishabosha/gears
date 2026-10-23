package higherlevel

import gears.async.Async
import gears.async.BufferedChannel
import gears.async.Channel
import gears.async.Future

import java.nio.ByteBuffer
import java.util.concurrent.CancellationException

import asyncio.BufferPool
import asyncio.Handle
import asyncio.Io
import asyncio.Received
import asyncio.ResolveBackend
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.SocketBackend

/** The higher level the interface is meant to be wrapped by, written over `Io` alone: result-returning methods that
  * own the op and the slot the caller would otherwise reuse, and a stream of chunks over a repeatable pooled read,
  * consumed as a gears channel. Nothing here touches a backend, a kind or the runtime, and being outside the
  * `asyncio` package it could not: it sees the interface's public contract only. It runs as it is on every backend
  * (`asyncio.gears.FuturesContract` checks it on each), which is what the lowest level is for.
  */
object HigherLevel {

  /** Reads into `buf` once and returns how many bytes arrived, 0 at the end of the stream. */
  def read(r: Io, handle: Handle, buf: ByteBuffer)(using Async): Int = {
    val before = buf.position()
    r.perform(r.ops.read(handle, buf))
    buf.position() - before
  }

  /** Writes all of `buf`, however many writes it takes. */
  def writeAll(r: Io, handle: Handle, buf: ByteBuffer)(using Async): Unit = {
    val write = r.ops.write(handle, buf)
    while buf.hasRemaining do r.perform(write)
  }

  /** Accepts one connection and returns it. */
  def accept(r: Io & SocketBackend, listener: Handle)(using Async): Handle = {
    val into = Handle.Slot()
    r.perform(r.ops.accept(listener, into))
    into.clear()
  }

  /** Resolves `host` and returns its addresses. */
  def resolve(r: Io & ResolveBackend, host: String)(using Async): List[ResolvedAddress] = {
    val into = Slot[List[ResolvedAddress]]()
    r.perform(r.ops.resolve(host, into))
    into.clear()
  }

  /** A stream of chunks, each a `Received` the consumer releases: `read` gives the next, or `None` at the end of the
    * stream, and throws the read's failure if it ended with one; `close` stops the stream early and releases the
    * chunks read ahead and not yet taken, so the pool may be closed afterwards. The channel is never closed, since a
    * closed gears channel drops what it still holds: the end of the stream is a `None` sent after the last chunk.
    */
  final class Chunks private[HigherLevel] (channel: Channel[Option[Received]], reader: Future[Unit]) {

    def read()(using Async): Option[Received] = channel.read() match {
      case Right(Some(chunk)) => Some(chunk)
      case Right(None) =>
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
    * consumer's unreleased chunks come out of the rest. Cancelling the group cancels the read, as it would any
    * future's op; a chunk in the reader's hands when it is cancelled, one a `send` did not deliver, is released by
    * the reader itself.
    */
  def chunks(r: Io, handle: Handle, pool: BufferPool, backlog: Int)(using Async.Spawn): Chunks = {
    require(pool.buffers >= backlog + 2, s"a backlog of $backlog needs a pool of ${backlog + 2} buffers")
    val channel = BufferedChannel[Option[Received]](backlog)
    val reader = Future {
      val into = Slot[Received]()
      val read = r.ops.read(handle, pool, into)
      var inHand: Received | Null = null
      try {
        var atEnd = false
        while !atEnd do {
          r.perform(read)
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
        case failure: Throwable =>
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
