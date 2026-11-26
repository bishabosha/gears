package gears.async.streams

import gears.async.io.Interface

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import scala.language.experimental.modularity

object text {
  val utf8: StreamCodec = new StreamCodec(StandardCharsets.UTF_8)

  final class StreamCodec(charset: Charset) {

    /** The strings of `s` as bytes, a chunk of bytes for each chunk of strings. A character the charset has no bytes
      * for, half a surrogate pair say, becomes the charset's replacement, `?` in UTF-8.
      */
    def encode[I <: Interface](s: Stream[I, String]): Stream[I, Byte] = s.mapChunks(encodeChunk)

    private def encodeChunk(strings: Chunk[String]): Chunk[Byte] =
      if strings.size == 1 then Chunk.bytes(strings(0).getBytes(charset))
      else {
        val parts = new Array[Array[Byte]](strings.size)
        var total = 0
        var i = 0
        while i < parts.length do {
          parts(i) = strings(i).getBytes(charset)
          total += parts(i).length
          i += 1
        }
        val out = new Array[Byte](total)
        var at = 0
        i = 0
        while i < parts.length do {
          System.arraycopy(parts(i), 0, out, at, parts(i).length)
          at += parts(i).length
          i += 1
        }
        Chunk.bytes(out)
      }
  }
}
