package asyncio

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/** What a backend with regular files must do (see `ReactorContract`): opened by the `open` op, read as a stream, and
  * read, written and synced at offsets.
  */
trait FileContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & FileBackend

  /** Opens the file at `path` through `r` with the `open` op, throwing its failure. */
  protected def open(r: R, path: Path, write: Boolean)(using munit.Location): Handle = {
    val into = Handle.Slot()
    val failure = run(r, backend.ops.open(path.toString, write, into))
    if failure != null then throw failure
    into.clear()
  }

  test("open refuses what is not a regular file, leaving its slot empty") {
    val dir = tempDir()
    try
      withReactor { r =>
        val into = Handle.Slot()
        val failure = run(r, backend.ops.open(dir.toString, write = false, into))
        assert(failure.isInstanceOf[IOException], String.valueOf(failure))
        intercept[IllegalStateException](into.clear())
        val missing = run(r, backend.ops.open(dir.resolve("missing").toString, write = false, into))
        assert(missing.isInstanceOf[IOException], String.valueOf(missing))
      }
    finally deleteAll(dir)
  }

  test("an opened file reads to its end as a stream") {
    val dir = tempDir()
    try
      withReactor { r =>
        val path = dir.resolve("contents.txt")
        Files.write(path, "file contents".getBytes(StandardCharsets.UTF_8))
        val file = open(r, path, write = false)
        try {
          val buf = allocate(64)
          assertEquals(readToEnd(r, backend.ops.read(file, buf), buf), "file contents")
        } finally close(r, file)
      }
    finally deleteAll(dir)
  }

  test("a positional read starts at its offset, goes on from where it left off, and ends with nothing added") {
    val dir = tempDir()
    try
      withReactor { r =>
        val path = dir.resolve("digits.txt")
        Files.write(path, "0123456789".getBytes(StandardCharsets.UTF_8))
        val file = open(r, path, write = false)
        try {
          val buf = allocate(2)
          val read = backend.ops.read(file, buf, offset = 6)
          assertEquals(run(r, read), null)
          assertEquals(text(buf), "67")
          buf.clear()
          assertEquals(run(r, read), null)
          assertEquals(text(buf), "89")
          buf.clear()
          assertEquals(run(r, read), null)
          assertEquals(buf.position(), 0)
          val fresh = allocate(4)
          assertEquals(run(r, backend.ops.read(file, fresh, offset = 0)), null)
          assertEquals(text(fresh), "0123")
        } finally close(r, file)
      }
    finally deleteAll(dir)
  }

  test("a positional write lands at its offset, and sync completes") {
    val dir = tempDir()
    try
      withReactor { r =>
        val path = dir.resolve("overwritten.txt")
        Files.write(path, "0123456789".getBytes(StandardCharsets.UTF_8))
        val file = open(r, path, write = true)
        try {
          val write = backend.ops.write(file, bytes("ab"), offset = 2)
          assertEquals(run(r, write), null)
          val sync = backend.ops.sync(file)
          assertEquals(run(r, sync), null)
          assertEquals(run(r, sync), null) // repeatable
          assertEquals(Files.readString(path), "01ab456789")
        } finally close(r, file)
      }
    finally deleteAll(dir)
  }

  test("a positional op on a closed file fails") {
    val dir = tempDir()
    try
      withReactor { r =>
        val path = dir.resolve("closed.txt")
        Files.write(path, "x".getBytes(StandardCharsets.UTF_8))
        val file = open(r, path, write = false)
        close(r, file)
        val failure = run(r, backend.ops.read(file, allocate(4), offset = 0))
        assert(failure.isInstanceOf[IOException], String.valueOf(failure))
      }
    finally deleteAll(dir)
  }
}
