package gears.async.io
package flow

import gears.async.Async

import java.io.EOFException
import java.nio.ByteBuffer

/** Many byte streams served by one future, a whole message at a time: over a `Batch`, which it hides. A stream is asked
  * to `receive` a buffer full or to `send` one, which begins nothing; `next` then returns what has finished, one event
  * at a time, each for a whole buffer however many reads or writes it took, and suspends the caller only when nothing
  * has, once, for all the streams together.
  *
  * {{{
  * val streams = Streams[Session](io)
  * for connection <- connections do streams.open(connection, connection, new Session).receive(request)
  * while serving do
  *   streams.next() match
  *     case Received(stream) => stream.send(reply(stream.note))
  *     case Sent(stream)     => stream.receive(stream.note.request)
  *     case Ended(stream)    => close(stream)
  *     case Failed(stream, cause) => ...
  * }}}
  *
  * It is one future's, as its batch is, and is closed by it.
  */
final class Streams[A, I <: Io[ByteStreams] & Singleton] private (val io: I) {
  import Streams.*

  private val batch: Batch[io.Op] = Batch(io)

  // What the last perform found done, and how far `next` has got through it.
  private var found = 0
  private var taken = 0

  /** One stream of the set: what is read from `in` and written to `out`, with the caller's `note`. It has one receive
    * and one send under way at most, which may be both at once.
    */
  final class Stream private[Streams] (in: io.ops.Readable, out: io.ops.Writable, val note: A) {
    private[Streams] var reading: ByteBuffer | Null = null
    private[Streams] var writing: ByteBuffer | Null = null
    private[Streams] var readOp: io.Op | Null = null
    private[Streams] var writeOp: io.Op | Null = null
    private var readInto: ByteBuffer | Null = null
    private var writeFrom: ByteBuffer | Null = null
    private[Streams] var before = -1

    // An event of each kind, the stream's own: `next` makes nothing as it tells of them.
    private[Streams] val received = Received(this)
    private[Streams] val sent = Sent(this)

    /** Asks for `buf` to be filled: cleared now, and flipped as `Received` tells of it. The stream ending before its
      * first byte is `Ended`; ending part way through is a failure.
      */
    def receive(buf: ByteBuffer): Unit = {
      if reading != null then throw new IllegalStateException("The stream has a receive under way")
      // A read is of one buffer: made again only for another.
      if readInto ne buf then {
        readOp = io.ops.read(in, buf)
        readInto = buf
      }
      buf.clear()
      before = 0
      reading = buf
      batch.add(readOp.nn, this)
    }

    /** Asks for what remains of `buf` to be written, all of it: `Sent` tells of it. */
    def send(buf: ByteBuffer): Unit = {
      if writing != null then throw new IllegalStateException("The stream has a send under way")
      if writeFrom ne buf then {
        writeOp = io.ops.write(out, buf)
        writeFrom = buf
      }
      writing = buf
      batch.add(writeOp.nn, this)
    }
  }

  /** A stream over `in` and `out`, which may be one connection, with `note` to tell it by. */
  def open(in: io.ops.Readable, out: io.ops.Writable, note: A): Stream = new Stream(in, out, note)

  /** How many receives and sends are under way. */
  def pending: Int = batch.size + (found - taken)

  /** The next receive or send to finish, of whichever stream, or null if none is under way. The caller is suspended
    * only if none has finished yet, until the first does. Throws only the caller's cancellation.
    */
  def next()(using Async): Event[Stream] | Null = {
    var event: Event[Stream] | Null = null
    while event == null do {
      if taken == found then {
        if batch.size == 0 then return null
        found = batch.perform()
        taken = 0
      }
      while event == null && taken < found do {
        val i = taken
        taken += 1
        val stream = batch.note(i).asInstanceOf[Stream]
        val failure = batch.failure(i)
        val isRead = stream.readOp != null && batch.isOp(i, stream.readOp.nn)
        if failure != null then {
          if isRead then stream.reading = null else stream.writing = null
          event = Failed(stream, failure)
        } else if isRead then {
          val buf = stream.reading.nn
          if !buf.hasRemaining() then {
            buf.flip()
            stream.reading = null
            event = stream.received
          } else if buf.position() == stream.before then {
            // A read that added nothing is the end of the stream.
            stream.reading = null
            event =
              if buf.position() == 0 then Ended(stream)
              else Failed(stream, new EOFException(s"The stream ended ${buf.remaining()} bytes short of the buffer"))
          } else {
            stream.before = buf.position()
            batch.add(stream.readOp.nn, stream)
          }
        } else if stream.writing.nn.hasRemaining() then batch.add(stream.writeOp.nn, stream)
        else {
          stream.writing = null
          event = stream.sent
        }
      }
    }
    event
  }

  /** Ends what is under way, on every stream. The streams' handles are the caller's to close. */
  def close()(using Async): Unit = {
    batch.close()
    found = 0
    taken = 0
  }
}

object Streams {

  /** A set of streams of `io`'s, for the calling future, each with a note of type `A`. */
  def apply[A](io: Io[ByteStreams]): Streams[A, io.type] = new Streams[A, io.type](io)

  /** What `next` tells of: a buffer filled, a buffer written, the stream's end where a receive began, or the failure of
    * a receive or a send.
    */
  sealed trait Event[+S]
  final case class Received[+S](stream: S) extends Event[S]
  final case class Sent[+S](stream: S) extends Event[S]
  final case class Ended[+S](stream: S) extends Event[S]
  final case class Failed[+S](stream: S, cause: Throwable) extends Event[S]
}
