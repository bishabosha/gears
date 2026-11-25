package gears.async.io.flow

import gears.async.Async
import gears.async.Future
import gears.async.Semaphore
import gears.async.io.ByteStreams
import gears.async.io.Interface
import gears.async.io.Io
import gears.async.io.IpSocketAddress
import gears.async.io.IpStreams
import gears.async.io.Repeatable
import gears.async.io.Slot
import gears.async.io.SocketOptions
import gears.async.io.StreamSockets

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** Finds whole messages in the bytes a source of bytes lends (see `Flow.read`). One is made for each run, so it may
  * keep state.
  */
trait Framing[A] {

  /** Adds to `into` every whole message `buf` holds between its position and its limit, and leaves the position past
    * the last of them. What is left, the beginning of a message, comes again with the bytes that follow it. A message
    * may refer to `buf`'s bytes only until the stages after have returned: the buffer is the source's.
    */
  def decode(buf: ByteBuffer, into: Chunk.Builder[A]): Unit
}

object Framing {

  /** Lines of text in UTF-8, each ended by a line feed, which is not part of it, nor is a carriage return before it. */
  def lines: Framing[String] = new Framing[String] {
    def decode(buf: ByteBuffer, into: Chunk.Builder[String]): Unit = {
      var at = buf.position()
      val limit = buf.limit()
      while at < limit do {
        if buf.get(at) == '\n' then {
          val from = buf.position()
          var length = at - from
          if length > 0 && buf.get(at - 1) == '\r' then length -= 1
          val bytes = new Array[Byte](length)
          buf.get(bytes) // from the position, which is where the line begins
          buf.position(at + 1)
          into += new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
        }
        at += 1
      }
    }
  }

  /** Messages of `size` bytes each, as copies. */
  def fixed(size: Int): Framing[Array[Byte]] = new Framing[Array[Byte]] {
    def decode(buf: ByteBuffer, into: Chunk.Builder[Array[Byte]]): Unit =
      while buf.remaining() >= size do {
        val bytes = new Array[Byte](size)
        buf.get(bytes)
        into += bytes
      }
  }
}

/** The flows that perform ops, and the stages for bytes: `Flow`'s companion has them. */
trait FlowIo {

  /** The bytes of `handle` as they are read, until the end of the stream: each element is the source's own buffer, one
    * that `buffer` gives for the run, holding between its position and its limit what the stream has brought. The
    * buffer is lent for the call: the stage after takes what it can use by moving the position, and what it leaves, the
    * beginning of a message say, is there again with the bytes that follow it. A stage that keeps bytes beyond the call
    * copies them (`copied`). A buffer left full with nothing taken, or a stream that ends with bytes left, is a
    * failure: an `IOException`, and an `EOFException`.
    *
    * With `idle`, a read that waits longer than it for bytes fails with a `TimeoutException`.
    */
  def read(io: Io[ByteStreams])(
      handle: io.ops.Readable,
      buffer: => ByteBuffer,
      idle: FiniteDuration | Null = null
  ): Flow[ByteBuffer] = Flow.make[ByteBuffer] { emit =>
    val buf = buffer
    val lent = Chunk.one(buf)
    val read = io.ops.read(handle, buf)
    var ended = false
    while !ended do {
      val had = buf.position()
      if !buf.hasRemaining() then
        throw new IOException(s"A message is larger than the buffer it is read into, of ${buf.capacity()} bytes")
      if idle == null then io.perform(read) else io.perform(read, idle.nn)
      if buf.position() == had then {
        if had > 0 then throw new EOFException(s"The stream ended within a message, $had bytes into it")
        ended = true
      } else {
        buf.flip()
        emit(lent)
        if buf.hasRemaining() then buf.compact() else buf.clear()
      }
    }
  }

  /** An element every `period`, the first one a period after the run began. */
  def tick(io: Io[Interface])(period: FiniteDuration): Flow[Unit] = Flow.make[Unit] { emit =>
    val timer = io.ops.timer(period)
    val unit = Chunk.one(())
    while true do {
      io.perform(timer)
      emit(unit)
    }
  }

  /** A connection to `address`, closed when the stages after it have returned (see `Flow.resource`). */
  def connect(io: Io[IpStreams])(
      address: IpSocketAddress,
      options: SocketOptions = SocketOptions.default
  ): Flow[io.ops.Connection] =
    Flow.resource {
      val into = Slot[io.ops.Connection]()
      io.perform(io.ops.connect(address, options, into))
      into.clear()
    }(connection => io.perform(io.ops.close(connection)))

  /** A listener on `address`, closed when the stages after it have returned (see `Flow.resource`). */
  def listen(io: Io[IpStreams])(
      address: IpSocketAddress,
      options: SocketOptions = SocketOptions.default
  ): Flow[io.ops.Listener] =
    Flow.resource {
      val into = Slot[io.ops.Listener]()
      io.perform(io.ops.listen(address, options, into))
      into.clear()
    }(listener => io.perform(io.ops.close(listener)))

  /** Serves the connections that come to `listener`, for as long as it is run: each is given to `handler`, and the flow
    * that gives is run in a future of the connection's own, which then closes the connection, however the flow ended.
    * No more than `limit` connections are served at once: beyond that the ones that come wait, accepted or in the
    * system's queue. A connection whose flow fails is closed and its failure given to `failed`; the others go on. When
    * the run ends, by a cancel or a failure of the accept, every connection's flow is cancelled and its connection
    * closed before the run returns. It emits nothing.
    */
  def accept(io: Io[StreamSockets])(
      listener: io.ops.Listener,
      limit: Int = Int.MaxValue,
      failed: Throwable => Unit = _ => ()
  )(handler: io.ops.Connection => Flow[Any]): Flow[Nothing] = Flow.make[Nothing] { _ =>
    Async.group {
      val into = Slot[Array[io.ops.Connection]]()
      val accept = io.ops.acceptAll(listener, into, 64)
      val permits = if limit == Int.MaxValue then null else Semaphore(limit)
      while true do {
        io.perform(accept)
        val accepted = into.clear()
        var served = 0
        try
          while served < accepted.length do {
            val guard = if permits == null then null else permits.nn.acquire()
            val connection = accepted(served)
            served += 1
            Future {
              try Flow.releasing(handler(connection).run())(io.perform(io.ops.close(connection)))
              catch { case NonFatal(failure) if !Flow.isCancel(failure) => failed(failure) }
              finally if guard != null then guard.nn.release()
            }: Unit
          }
        finally
          // Those accepted that no future was begun for, the run having ended while they waited their turn.
          while served < accepted.length do {
            val connection = accepted(served)
            served += 1
            try gears.async.uninterruptible(io.perform(io.ops.close(connection)))
            catch { case NonFatal(_) => () }
          }
      }
    }
  }

  /** Listens on `address` and serves what comes, as `accept` does, the listener closed when the run ends. */
  def serve(io: Io[IpStreams])(
      address: IpSocketAddress,
      options: SocketOptions = SocketOptions.default,
      limit: Int = Int.MaxValue,
      failed: Throwable => Unit = _ => ()
  )(handler: io.ops.Connection => Flow[Any]): Flow[Nothing] =
    listen(io)(address, options).flatMap(listener => accept(io)(listener, limit, failed)(handler))

  extension (flow: Flow[ByteBuffer]) {

    /** Writes every buffer that comes to `handle`, all of what it holds between position and limit, so that a chunk of
      * one buffer is one write if the stream takes it whole. It emits nothing, and ends as the flow before it does.
      */
    def writeTo(io: Io[ByteStreams])(handle: io.ops.Writable): Flow[Nothing] = Flow.make[Nothing] { _ =>
      var last: ByteBuffer | Null = null
      var write: (io.ops.Op & Repeatable) | Null = null
      flow.runWith(_.foreach { buf =>
        if buf ne last then { // an op is its buffer's: most flows write the one buffer again and again
          write = io.ops.write(handle, buf)
          last = buf
        }
        while buf.hasRemaining() do io.perform(write.nn)
      })
    }

    /** The whole messages `framing` finds in the bytes that come, a chunk for each buffer that held any: what a read
      * brought goes on together.
      */
    def frames[A](framing: => Framing[A]): Flow[A] = Flow.make[A] { emit =>
      val finds = framing
      val found = new Chunk.Builder[A]
      flow.runWith { chunk =>
        chunk.foreach(buf => finds.decode(buf, found))
        if !found.isEmpty then emit(found.result())
      }
    }

    /** Each buffer's bytes as a buffer of their own, for a stage that keeps them, the lent one taken whole. */
    def copied: Flow[ByteBuffer] = flow.map { buf =>
      val copy = ByteBuffer.allocate(buf.remaining())
      copy.put(buf)
      copy.flip()
      copy
    }
  }

  extension [A](flow: Flow[A]) {

    /** The elements written as bytes: `write` puts each into a buffer, one that `buffer` gives for the run, and the
      * buffer goes on once for each chunk, so that what a read brought is answered in one write. `largest` is the most
      * one element writes: the buffer goes on sooner when it has less room than that. The buffer is lent, as
      * `Flow.read`'s is: the stage after takes all of it.
      */
    def encode(buffer: => ByteBuffer, largest: Int)(write: (A, ByteBuffer) => Unit): Flow[ByteBuffer] =
      Flow.make[ByteBuffer] { emit =>
        val out = buffer
        require(out.capacity() >= largest, s"The buffer, of ${out.capacity()} bytes, has no room for $largest")
        val lent = Chunk.one(out)
        def flush(): Unit =
          if out.position() > 0 then {
            out.flip()
            emit(lent)
            if out.hasRemaining() then
              throw new IllegalStateException("The stage after an encode left bytes of its buffer untaken")
            out.clear()
          }
        flow.runWith { chunk =>
          chunk.foreach { a =>
            if out.remaining() < largest then flush()
            write(a, out)
          }
          flush()
        }
      }
  }
}
