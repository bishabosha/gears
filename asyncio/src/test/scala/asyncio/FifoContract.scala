package asyncio

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** What a backend with FIFOs must do (see `ReactorContract`). */
trait FifoContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & FifoBackend

  test("a FIFO carries what its writer writes, then ends when the writer closes") {
    val dir = tempDir()
    try withReactor { r =>
      val fifo = dir.resolve("fifo")
      val made = new ProcessBuilder("mkfifo", fifo.toString).start().waitFor()
      assertEquals(made, 0)
      val reader = backend.handles.openFifo(fifo.toString, write = false) // a non-blocking writer needs a reader to open
      try {
        val writer = backend.handles.openFifo(fifo.toString, write = true)
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
  test("openFile refuses a FIFO, and openFifo a regular file") {
    val dir = tempDir()
    try {
      val fifo = dir.resolve("fifo")
      assertEquals(new ProcessBuilder("mkfifo", fifo.toString).start().waitFor(), 0)
      intercept[java.io.IOException](backend.handles.openFile(fifo.toString, write = false))
      val file = dir.resolve("file")
      Files.write(file, "x".getBytes(StandardCharsets.UTF_8))
      intercept[java.io.IOException](backend.handles.openFifo(file.toString, write = false))
    } finally deleteAll(dir)
  }
}
