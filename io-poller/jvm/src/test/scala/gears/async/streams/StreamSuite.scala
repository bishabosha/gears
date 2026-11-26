package gears.async.streams

import gears.async.io.Io
import gears.async.io.nio.NioBackend

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files as JFiles
import scala.language.experimental.modularity

class StreamSuite extends munit.FunSuite {
  private given backend: NioBackend.type = NioBackend

  private val files = Files[IOApp.Platform]

  private def tempFile(content: Array[Byte]): java.nio.file.Path = {
    val path = JFiles.createTempFile("gears-streams", ".txt")
    JFiles.write(path, content)
    path
  }

  test("the stages give what the collections' give") {
    val got = Io.run {
      Stream(1, 2, 3, 4, 5, 6).filter(_ % 2 == 0).map(_ * 10).intersperse(0).compile.toList
    }
    assertEquals(got, List(20, 0, 40, 0, 60))
  }

  test("intersperse puts its element between chunks too, and none around an empty stream") {
    Io.run {
      assertEquals(
        (Stream(1, 2) ++ Stream(3) ++ Stream(4, 5)).intersperse(0).compile.toList,
        List(1, 0, 2, 0, 3, 0, 4, 0, 5)
      )
      assertEquals(Stream.empty[Int].intersperse(0).compile.toList, Nil)
    }
  }

  test("flatMap, evalMap and fold") {
    Io.run {
      assertEquals(Stream(1, 2, 3).flatMap(n => Stream(n, n)).compile.fold(0)(_ + _), 12)
      assertEquals(Stream(1, 2).evalMap[gears.async.io.Interface, Int](n => n + 1).compile.toList, List(2, 3))
    }
  }

  test("take stops the stream before it, which is not run past the chunk that had enough") {
    Io.run {
      var seen = 0
      val counted = (Stream(1, 2) ++ Stream(3, 4) ++ Stream(5, 6)).map { n => seen += 1; n }
      assertEquals(counted.take(3).compile.toList, List(1, 2, 3))
      assertEquals(seen, 4)
      assertEquals(counted.take(0).compile.toList, Nil)
      assertEquals(counted.take(100).compile.toList, List(1, 2, 3, 4, 5, 6))
    }
  }

  test("a file's lines are the same whatever the size of the reads that find them") {
    val text = "﻿first\r\nsecond\n\nnaïve ☃ 😀 line\n" + ("x" * 300) + "\r\nlast, with no ending"
    val expected = List("first", "second", "", "naïve ☃ 😀 line", "x" * 300, "last, with no ending")
    val path = tempFile(text.getBytes(UTF_8))
    try
      for size <- List(8, 9, 10, 11, 13, 64, 65536) do {
        val got = Io.run(files.readUtf8Lines(Path(path.toString), size).compile.toList)
        assertEquals(got, expected, s"with reads of $size bytes")
      }
    finally JFiles.delete(path)
  }

  test("a file ending in a line ending has no empty line after it, and an empty file has no lines") {
    val path = tempFile("a\nb\n".getBytes(UTF_8))
    val empty = tempFile(Array.emptyByteArray)
    try {
      assertEquals(Io.run(files.readUtf8Lines(Path(path.toString)).compile.toList), List("a", "b"))
      assertEquals(Io.run(files.readUtf8Lines(Path(empty.toString)).compile.toList), Nil)
    } finally {
      JFiles.delete(path)
      JFiles.delete(empty)
    }
  }

  test("bytes that are not UTF-8 are replaced, not a failure") {
    val path = tempFile(Array[Byte]('a', 0xff.toByte, 'b', '\n', 0xe2.toByte))
    try assertEquals(Io.run(files.readUtf8Lines(Path(path.toString)).compile.toList), List("a�b", "�"))
    finally JFiles.delete(path)
  }

  test("readAll and writeAll copy a file larger than their buffers, over one that was longer") {
    val bytes = Array.tabulate[Byte](200_000)(i => (i * 31).toByte)
    val from = tempFile(bytes)
    val to = tempFile(new Array[Byte](300_000))
    try {
      val copy = files.readAll(Path(from.toString), 1000).through(files.writeAll(Path(to.toString)))
      Io.run(copy.compile.drain)
      assert(java.util.Arrays.equals(JFiles.readAllBytes(to), bytes))
      Io.run(copy.compile.drain) // a stream may be run again
      assert(java.util.Arrays.equals(JFiles.readAllBytes(to), bytes))
      // Bytes that came through a stage that boxes them are written the same.
      Io.run(files.readAll(Path(from.toString)).map(b => b).through(files.writeAll(Path(to.toString))).compile.drain)
      assert(java.util.Arrays.equals(JFiles.readAllBytes(to), bytes))
    } finally {
      JFiles.delete(from)
      JFiles.delete(to)
    }
  }

  test("writeAll creates its file") {
    val dir = JFiles.createTempDirectory("gears-streams")
    val to = dir.resolve("new.txt")
    try {
      Io.run(
        Stream("a", "é")
          .intersperse("\n")
          .through(text.utf8.encode)
          .through(files.writeAll(Path(to.toString)))
          .compile
          .drain
      )
      assertEquals(JFiles.readString(to), "a\né")
    } finally {
      JFiles.deleteIfExists(to)
      JFiles.delete(dir)
    }
  }

  test("a file whose stream is stopped early or fails is closed: its run ends with nothing left to close") {
    val path = tempFile(("line\n" * 100_000).getBytes(UTF_8))
    try {
      assertEquals(
        Io.run(files.readUtf8Lines(Path(path.toString), 64).take(3).compile.toList),
        List("line", "line", "line")
      )
      val failure = intercept[IllegalStateException] {
        Io.run(
          files.readUtf8Lines(Path(path.toString), 64).map(_ => throw new IllegalStateException("stop")).compile.drain
        )
      }
      assertEquals(failure.getMessage, "stop")
    } finally JFiles.delete(path)
  }
}
