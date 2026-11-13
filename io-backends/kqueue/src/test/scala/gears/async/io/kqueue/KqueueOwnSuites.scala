package gears.async.io
package kqueue

import gears.async.Future
import gears.async.io.internal.Interest
import gears.async.io.internal.ReactorChecks
import gears.async.io.testkit.FileSystemFixture
import gears.async.io.testkit.PipeFixture

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import scala.concurrent.duration.*

/** Kqueue's own readiness, outside the interface (`KqueueOps.ready`, `readNow`, `writeNow`), as programs perform it:
  * `ready` ops, level-triggered, on streams, regular files and FIFOs, and handles used without waiting.
  */
class KqueueOwnSuite extends PipeFixture with FileSystemFixture {
  val backend: KqueueBackend.type = KqueueBackend

  test("readNow into a buffer with no room throws, reading nothing; writeNow with nothing remaining returns 0") {
    running { io ?=>
      val (in, out) = pipe(io)
      try {
        backend.ops.writeNow(out, bytes("kept"))
        val full = allocate(4)
        full.position(full.limit())
        intercept[IllegalArgumentException](backend.ops.readNow(in, full))
        val buf = allocate(16)
        assertEquals(backend.ops.readNow(in, buf), 4)
        assertEquals(text(buf), "kept", "the refused read took none of the bytes")
        val empty = allocate(4)
        empty.limit(0)
        close(io, in) // a write of bytes would fail now; one of nothing touches nothing
        assertEquals(backend.ops.writeNow(out, empty), 0)
      } finally close(io, out)
    }
  }

  test("readNow reports the end of the stream") {
    running { io ?=>
      val (in, out) = pipe(io)
      try {
        close(io, out)
        assertEquals(backend.ops.readNow(in, allocate(16)), -1)
      } finally close(io, in)
    }
  }

  test("ready completes once the handle is ready, again at once while it still is, and waits once it is drained") {
    running { io ?=>
      val (in, out) = pipe(io)
      try {
        val buf = allocate(64)
        val ready = backend.ops.ready(in, Interest.Read)
        val first = Future(io.perform(ready))
        settle(io)
        assert(!first.poll().isDefined, "ready with nothing written")
        backend.ops.writeNow(out, bytes("part"))
        first.await
        io.perform(ready, 1.second) // the data has not been taken yet
        assertEquals(backend.ops.readNow(in, buf), 4)
        assertEquals(text(buf), "part")
        val drained = Future(io.perform(ready))
        settle(io)
        assert(!drained.poll().isDefined, "ready once drained")
        backend.ops.writeNow(out, bytes("!"))
        drained.await
        assertEquals(backend.ops.readNow(in, buf), 1)
        assertEquals(text(buf), "part!")
      } finally {
        close(io, in)
        close(io, out)
      }
    }
  }

  test("closing a handle that another run waits to write on, while it waits on nothing else, ends its peer's stream") {
    val (in, out) = running(io ?=> pipe(io))
    val timer = backend.ops.deadlineTimer(System.nanoTime() + 60_000_000_000L)
    val registered = new CountDownLatch(1)
    val other = new Thread(() =>
      running { io ?=>
        io.perform(io.ops.ready(out, Interest.Write)) // registers the write end with this run's reactor
        registered.countDown()
        io.perform(timer)
      }
    )
    other.start()
    try {
      registered.await()
      Thread.sleep(20) // so that the other run waits in its poll
      val received = running { io ?=>
        close(io, out)
        val buf = allocate(16)
        readToEnd(io, io.ops.read(in, buf), buf)
      }
      assertEquals(received, "")
    } finally {
      timer.moveTo(System.nanoTime())
      other.join()
      running(io ?=> close(io, in))
    }
  }

  // Kqueue's readiness on what its filters do not report, which `FifoWatcher`'s select does.

  test("ready on a regular file at its end completes, as select and poll say a file is always readable") {
    withTempDir { dir =>
      running { io ?=>
        val path = dir.resolve("file.txt")
        Files.write(path, "x".getBytes(StandardCharsets.UTF_8))
        val file = opening(io)(slot => backend.ops.open(path.toString, write = false, slot))
        try {
          io.perform(backend.ops.read(file, allocate(8))) // the one byte, leaving it at its end
          io.perform(backend.ops.ready(file, Interest.Read), 5.seconds)
        } finally close(io, file)
      }
    }
  }

  test("ready waiting on a FIFO completes when its writer closes") {
    withTempDir { dir =>
      running { io ?=>
        val fifo = dir.resolve("fifo")
        mkfifo(fifo)
        val reader = opening(io)(slot => backend.ops.openFifoForReading(fifo.toString, slot))
        try {
          val writer = opening(io)(slot => backend.ops.openFifoForWriting(fifo.toString, slot))
          val ready = Future(io.perform(backend.ops.ready(reader, Interest.Read)))
          settle(io)
          assert(!ready.poll().isDefined, "ready with the writer open and nothing written")
          close(io, writer)
          ready.await
          assertEquals(backend.ops.readNow(reader, allocate(8)), -1, "at its end")
        } finally close(io, reader)
      }
    }
  }
}

/** What kqueue's reactors do that only the `Reactor` API shows: `ReactorChecks`, and a read of data already waiting
  * finished by `trySubmit` without its completion.
  */
class KqueueReactorChecksSuite extends ReactorChecks {
  val backend: KqueueBackend.type = KqueueBackend

  test("trySubmit finishes a read of data already waiting without calling its completion") {
    withReactor { r =>
      val (in, out) = pipe(r)
      try {
        backend.ops.writeNow(out, bytes("ready"))
        val buf = allocate(16)
        val outcome = Outcome()
        assert(r.trySubmit(backend.ops.read(in, buf), outcome))
        assertEquals(text(buf), "ready")
        for _ <- 1 to 3 do r.runOnce(block = false)
        assertEquals(outcome.completed, 0)
      } finally {
        close(r, in)
        close(r, out)
      }
    }
  }
}
