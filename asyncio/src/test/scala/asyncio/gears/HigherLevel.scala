package asyncio.gears

import gears.async.Async
import gears.async.BufferedChannel
import gears.async.Channel
import gears.async.Future
import gears.async.ReadableChannel

import java.nio.ByteBuffer

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
  * consumed as a gears channel. Nothing here touches a backend, a kind or the runtime, so it runs as it is on every
  * backend (`FuturesContract` checks it on each), which is what the lowest level is for.
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

  /** The bytes of `handle` as a stream of chunks, each a `Received` the consumer releases: a future reads into the
    * pool's buffers with one repeatable op and sends each chunk on a channel of `backlog` chunks, which, with the
    * pool's window, is the back-pressure; the channel closes at the end of the stream, and the future fails the
    * consumer's group if the read fails. Cancelling the group cancels the read, as it would any future's op.
    */
  def chunks(r: Io, handle: Handle, pool: BufferPool, backlog: Int)(using Async.Spawn): ReadableChannel[Received] = {
    val channel = BufferedChannel[Received](backlog)
    Future {
      val into = Slot[Received]()
      val read = r.ops.read(handle, pool, into)
      var atEnd = false
      try
        while !atEnd do {
          r.perform(read)
          val chunk = into.clear()
          if chunk.buffer.hasRemaining then channel.send(chunk)
          else {
            chunk.release()
            atEnd = true
          }
        }
      finally channel.close()
    }
    channel
  }

  /** Reads every chunk of `stream` until it closes, releasing each, and returns how many bytes they held. */
  def drain(stream: ReadableChannel[Received])(using Async): Long = {
    var total = 0L
    var open = true
    while open do
      stream.read() match {
        case Right(chunk) =>
          total += chunk.buffer.remaining()
          chunk.release()
        case Left(Channel.Closed) => open = false
      }
    total
  }
}
