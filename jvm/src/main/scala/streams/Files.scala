package gears.async.streams

import gears.async.Async
import gears.async.io.{Io, RegularFiles, Slot}

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.language.experimental.modularity

opaque type Path = String
object Path {
  def apply(s: String): Path = s
}

extension (path: Path) {

  /** The path as the string it was made of. */
  def value: String = path
}

/** Files as streams, over a backend that has regular files. A stream here opens its file when it is run and closes it
  * when the run ends, however it ends, so the same stream may be run again.
  */
class Files[I <: RegularFiles] {

  /** The bytes of the file at `path`, in chunks of at most `chunkSize`. */
  def readAll(path: Path, chunkSize: Int = Files.DefaultChunkSize): Stream[I, Byte] = {
    require(chunkSize > 0, s"A chunk has room for at least a byte: $chunkSize")
    new Stream[I, Byte] {
      def foreachChunk(sink: Chunk[Byte] => Unit)(using async: Async, io: Io[I]): Unit =
        withFile(path.value, write = false) { file =>
          val buf = ByteBuffer.allocateDirect(chunkSize)
          val read = io.ops.read(file, buf) // one repeatable op, performed for every chunk
          var end = false
          while !end do {
            buf.clear()
            io.perform(read)
            buf.flip()
            if buf.hasRemaining then {
              val bytes = new Array[Byte](buf.remaining)
              buf.get(bytes)
              sink(Chunk.bytes(bytes))
            } else end = true
          }
        }
    }
  }

  /** The lines of the file at `path`, read as UTF-8, a chunk of them for each read of up to `chunkSize` bytes. A line
    * ends at a `\n` or a `\r\n`, neither of which it includes, or at the end of the file; a file that ends with a line
    * ending has no empty line after it. Bytes that are not UTF-8 become U+FFFD, and a byte order mark at the start is
    * dropped. The bytes are decoded straight from the buffer they were read into, with no chunk of bytes in between.
    */
  def readUtf8Lines(path: Path, chunkSize: Int = Files.DefaultChunkSize): Stream[I, String] = {
    // A character cut off by the end of a read stays in the buffer for the next, which must still have room to add to it.
    require(chunkSize >= 8, s"A chunk has room for at least a character and more: $chunkSize")
    new Stream[I, String] {
      def foreachChunk(sink: Chunk[String] => Unit)(using async: Async, io: Io[I]): Unit =
        withFile(path.value, write = false) { file =>
          val buf = ByteBuffer.allocateDirect(chunkSize)
          val read = io.ops.read(file, buf)
          val lines = new Utf8Lines(chunkSize)
          var end = false
          while !end do {
            val before = buf.position()
            io.perform(read)
            end = buf.position() == before
            buf.flip()
            val chunk = lines.feed(buf, end)
            buf.compact()
            if chunk.nonEmpty then sink(chunk)
          }
        }
    }
  }

  /** Writes the bytes of a stream to the file at `path`, created if it is not there and emptied if it is, and emits
    * nothing. Small chunks are gathered into one buffer, so a write is of `Files.DefaultChunkSize` bytes however the
    * stream chunks them, but for the last.
    */
  def writeAll(path: Path): Stream[I, Byte] => Stream[I, Unit] = in =>
    new Stream[I, Unit] {
      def foreachChunk(sink: Chunk[Unit] => Unit)(using async: Async, io: Io[I]): Unit = {
        // `RegularFiles.open` neither creates nor truncates, and the interface has no op that does: done here, on the
        // caller's thread, with the JDK.
        java.nio.file.Files.write(java.nio.file.Paths.get(path.value), new Array[Byte](0))
        withFile(path.value, write = true) { file =>
          val buf = ByteBuffer.allocateDirect(Files.DefaultChunkSize)
          val write = io.ops.write(file, buf)
          def flush(): Unit = {
            buf.flip()
            while buf.hasRemaining do io.perform(write)
            buf.clear()
          }
          in.foreachChunk {
            case bytes: Chunk.Bytes =>
              var done = 0
              while done < bytes.size do {
                if !buf.hasRemaining then flush()
                val n = math.min(buf.remaining, bytes.size - done)
                bytes.copyTo(buf, done, n)
                done += n
              }
            case boxed =>
              boxed.foreach { b =>
                if !buf.hasRemaining then flush()
                buf.put(b)
              }
          }
          flush()
        }
      }
    }

  /** Opens the file at `path`, runs `body` with it, and closes it, however `body` ends. */
  private def withFile[T](path: String, write: Boolean)(using
      io: Io[I],
      async: Async
  )(
      body: io.ops.File => T
  ): T = {
    val slot = Slot[io.ops.File]()
    io.perform(io.ops.open(path, write, slot))
    val file = slot.clear()
    try body(file)
    finally io.perform(io.ops.close(file))
  }
}

object Files {

  /** The size of a read or a write, unless a stream is given another. */
  inline val DefaultChunkSize = 64 * 1024
}

/** Decodes UTF-8 and splits it into lines, across the boundaries of what it is fed: the end of a line, and of a
  * character, may come in a later buffer than its start. For one run of one stream, on one thread at a time.
  */
private[streams] final class Utf8Lines(bufferSize: Int) {
  private val decoder = StandardCharsets.UTF_8
    .newDecoder()
    .onMalformedInput(CodingErrorAction.REPLACE)
    .onUnmappableCharacter(CodingErrorAction.REPLACE)

  /** What was last decoded, split and emptied before more is. */
  private val chars = CharBuffer.allocate(math.max(bufferSize, 16))

  /** The start of a line whose end has not been decoded yet. */
  private val partial = new java.lang.StringBuilder

  private val lines = new ArrayBuffer[AnyRef]
  private var atStart = true

  /** Decodes `bytes` and returns the lines that end in them, with the last line of all if `last`. The bytes of a
    * character that `bytes` ends in the middle of are left in it, for the caller to give again with the rest.
    */
  def feed(bytes: ByteBuffer, last: Boolean): Chunk[String] = {
    var more = true
    while more do {
      more = decoder.decode(bytes, chars, last).isOverflow
      split()
    }
    if last then {
      while decoder.flush(chars).isOverflow do split()
      split()
      if partial.length > 0 then {
        lines += partial.toString
        partial.setLength(0)
      }
    }
    if lines.isEmpty then Chunk.empty
    else {
      val chunk = new Chunk.Boxed[String](lines.toArray, 0, lines.length)
      lines.clear()
      chunk
    }
  }

  /** Takes the lines that end in `chars`, and keeps what follows the last of them as the start of the next. */
  private def split(): Unit = {
    val array = chars.array()
    val end = chars.position()
    var from = 0
    if atStart && end > 0 then {
      atStart = false
      if array(0) == '﻿' then from = 1
    }
    var i = from
    while i < end do {
      if array(i) == '\n' then {
        lines += line(array, from, i)
        from = i + 1
      }
      i += 1
    }
    partial.append(array, from, end - from)
    chars.clear()
  }

  /** The line that ends before `to`: what was kept of it, then `array` from `from`, without a `\r` at its end. */
  private def line(array: Array[Char], from: Int, to: Int): String =
    if partial.length == 0 then {
      val end = if to > from && array(to - 1) == '\r' then to - 1 else to
      new String(array, from, end - from)
    } else {
      partial.append(array, from, to - from)
      val length = partial.length
      if partial.charAt(length - 1) == '\r' then partial.setLength(length - 1)
      val whole = partial.toString
      partial.setLength(0)
      whole
    }
}
