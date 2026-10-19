package gearsexample

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import asyncio.Slot

/** What the demos share on every platform: decoding buffers, the stream demos' framing, and reading a slot. */
object DemoIO {

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
    inline val HeaderLength = 4
    inline val MaxMessageLength = 1024 * 1024

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

    /** Appends one framed message at the buffer's position; returns false, leaving it untouched, if it does not fit.
      */
    def append(message: String, buf: ByteBuffer): Boolean = {
      val bytes = message.getBytes(StandardCharsets.UTF_8)
      if HeaderLength + bytes.length > buf.remaining() then false
      else {
        buf.putInt(bytes.length)
        buf.put(bytes)
        true
      }
    }

    /** Appends one framed message to a byte stream. */
    def appendTo(out: ByteArrayOutputStream, message: String): Unit = {
      val bytes = message.getBytes(StandardCharsets.UTF_8)
      out.write(bytes.length >>> 24)
      out.write(bytes.length >>> 16)
      out.write(bytes.length >>> 8)
      out.write(bytes.length)
      out.write(bytes)
    }
  }

  /** Reassembles framed messages from a byte stream that arrives in arbitrary chunks. */
  final class FrameParser {
    private var pending = new Array[Byte](0)

    /** Takes the bytes between position and limit of `buf`. */
    def feed(buf: ByteBuffer): Unit = {
      val chunk = new Array[Byte](buf.remaining())
      buf.get(chunk)
      pending = pending ++ chunk
    }

    /** The next complete message, or null if the pending bytes do not hold one yet. */
    def next(): String | Null = {
      if pending.length < StreamProtocol.HeaderLength then return null
      val length = ByteBuffer.wrap(pending).getInt(0)
      require(length >= 0 && length <= StreamProtocol.MaxMessageLength, s"Invalid frame length $length")
      val end = StreamProtocol.HeaderLength + length
      if pending.length < end then null
      else {
        val message = new String(pending, StreamProtocol.HeaderLength, length, StandardCharsets.UTF_8)
        pending = pending.drop(end)
        message
      }
    }
  }

  /** Whether `slot` is empty, taking out any value it holds. */
  def isEmpty(slot: Slot[?]): Boolean =
    try {
      slot.clear()
      false
    } catch case _: IllegalStateException => true
}
