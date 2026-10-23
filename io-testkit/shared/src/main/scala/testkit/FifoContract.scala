package gears.async.io
package testkit

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** What a backend with FIFOs must do (see `ReactorContract`); with regular files too, to tell the two apart. */
trait FifoContract extends FileContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & FifoBackend & FileBackend

  /** A FIFO from the `openFifo` op, whose failure is thrown. */
  protected def openFifo(r: R, path: String, write: Boolean)(using munit.Location): Handle = {
    val into = Handle.Slot()
    opened(r, into, backend.ops.openFifo(path, write, into))
  }

  test("a FIFO carries what its writer writes, then ends when the writer closes") {
    val dir = tempDir()
    try withReactor { r =>
      val fifo = dir.resolve("fifo")
      val made = new ProcessBuilder("mkfifo", fifo.toString).start().waitFor()
      assertEquals(made, 0)
      val reader = openFifo(r, fifo.toString, write = false) // a non-blocking writer needs a reader to open
      try {
        val writer = openFifo(r, fifo.toString, write = true)
        val sent = Outcome()
        r.submit(backend.ops.write(writer, bytes("through the fifo")), sent)
        turnUntil(r)(sent.finished)
        assertEquals(sent.completed, 1, String.valueOf(sent.failure))
        closeAlone(writer)
        val buf = allocate(64)
        assertEquals(readToEnd(r, backend.ops.read(reader, buf), buf), "through the fifo")
      } finally closeAlone(reader)
    } finally deleteAll(dir)
  }
  test("open refuses a FIFO, and openFifo a regular file") {
    val dir = tempDir()
    try withReactor { r =>
      val fifo = dir.resolve("fifo")
      assertEquals(new ProcessBuilder("mkfifo", fifo.toString).start().waitFor(), 0)
      intercept[java.io.IOException](open(r, fifo, write = false))
      val file = dir.resolve("file")
      Files.write(file, "x".getBytes(StandardCharsets.UTF_8))
      val into = Handle.Slot()
      val failure = run(r, backend.ops.openFifo(file.toString, write = false, into))
      assert(failure.isInstanceOf[java.io.IOException], String.valueOf(failure))
      intercept[IllegalStateException](into.clear())
    } finally deleteAll(dir)
  }
}
