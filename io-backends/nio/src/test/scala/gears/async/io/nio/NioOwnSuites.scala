package gears.async.io
package nio

import gears.async.Async
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport
import gears.async.io.Io
import gears.async.io.internal.OnCaller
import gears.async.io.internal.ReactorRuns
import gears.async.io.internal.ReactorScheduler
import gears.async.io.nio.NioBackend
import gears.async.io.nio.NioOnCaller
import gears.async.io.testkit.BufferArena
import gears.async.io.testkit.ContractSuite
import gears.async.io.testkit.FileSystemFixture

import java.nio.charset.StandardCharsets

/** What NIO alone does, beyond the testkit's contracts (`NioSuites`): its types, and `NioOnCaller` on its own. */
class NioOnCallerOwnSuite extends ContractSuite {
  val backend: NioOnCaller.type = NioOnCaller

  test("NIO's kinds of handle are distinct types, though one class underneath") {
    assert(compileErrors("def k(c: NioOps.Connection): NioOps.Listener = c").nonEmpty)
    assert(compileErrors("def k(r: NioOps.Readable): NioOps.Writable = r").nonEmpty)
    assert(compileErrors("def k(f: NioOps.File): NioOps.Connection = f").nonEmpty)
    assertEquals(compileErrors("def k(c: NioOps.Connection): NioOps.Readable = c"), "")
  }

  test("a pool of workers, an internal knob, refuses a caller backend, which has nothing for it to run") {
    intercept[IllegalArgumentException](ReactorRuns.run(2)(using NioOnCaller)(42))
  }

  test("a program over NioOnCaller needs no pool: its Io, an OnCaller, performs on the calling thread") {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    var outcome = ""
    Async.blocking {
      val io = OnCaller(NioOnCaller)
      val read = Slot[io.ops.Readable]()
      val write = Slot[io.ops.Writable]()
      io.perform(io.ops.pipe(read, write)) // on this, a platform thread
      val (in, out) = (read.clear(), write.clear())
      val arena = BufferArena.open()
      try {
        val buf = arena.allocate(16)
        val reading = Future(io.perform(io.ops.read(in, buf))) // on a virtual thread
        io.perform(io.ops.timer(10)) // on this, a platform thread
        io.perform(io.ops.write(out, arena.of("no runner".getBytes(StandardCharsets.UTF_8))))
        reading.await
        buf.flip()
        outcome = StandardCharsets.UTF_8.decode(buf).toString
      } finally {
        io.perform(io.ops.close(in))
        io.perform(io.ops.close(out))
        arena.close()
      }
    }
    assertEquals(outcome, "no runner")
  }

  test("an interrupt the caller's thread already had neither fails nor closes a file it reads, and is kept") {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    val path = java.nio.file.Files.createTempFile("gears-io", ".txt")
    java.nio.file.Files.writeString(path, "file")
    var read = ""
    var kept = false
    try
      Async.blocking {
        val io = OnCaller(NioOnCaller)
        val into = Slot[io.ops.File]()
        io.perform(io.ops.open(path.toString, write = false, into))
        val file = into.clear()
        val arena = BufferArena.open()
        try {
          val buf = arena.allocate(16)
          Thread.currentThread().interrupt() // a JDK FileChannel used with it set closes
          io.perform(io.ops.read(file, buf, 0))
          kept = Thread.interrupted()
          buf.flip()
          read = StandardCharsets.UTF_8.decode(buf).toString
          io.perform(io.ops.read(file, arena.allocate(16), 0)) // the file is still open
        } finally {
          io.perform(io.ops.close(file))
          arena.close()
        }
      }
    finally java.nio.file.Files.delete(path)
    assertEquals(read, "file")
    assert(kept, "the interrupt was lost")
  }
}

/** What `NioBackend` alone does: its programs on a pool of one worker driving its reactors. */
class NioReactorsOwnSuite extends ContractSuite with FileSystemFixture with OnWorkers(1) {
  val backend: NioBackend.type = NioBackend

  test("a run leaves its caller's file system as it was: the caller is not one of the pool's carriers") {
    withTempDir { dir =>
      val path = dir.resolve("file.txt")
      java.nio.file.Files.writeString(path, "file")
      assert(java.nio.file.Files.isDirectory(dir)) // the JDK caches this path's native form for this thread
      running { io ?=>
        val file = opening(io)(slot => io.ops.open(path.toString, write = false, slot))
        try io.perform(io.ops.read(file, allocate(16)))
        finally close(io, file)
      }
      assert(java.nio.file.Files.isDirectory(dir), "the caller was given a carried virtual thread's cached path")
    }
  }

  test("perform reuses the operation's completion state without replacing its attachment") {
    running { io ?=>
      val op = NioOps.timer(0)
      val attachment = new Object
      op.attachment = attachment
      io.perform(op)
      io.perform(op)
      assert(op.attachment eq attachment)
    }
  }

  test("a program's Io is an OnPool over the backend: its ops go to the reactor of the worker running the caller") {
    var onWorker = false
    running { io ?=>
      onWorker = summon[Async].scheduler match {
        case s: ReactorScheduler => s.currentWorker.reactor.isInstanceOf[gears.async.io.nio.NioReactor]
        case _                   => false
      }
    }
    assert(onWorker)
  }

  test("a program's Io cannot start a run: it is not a Backend") {
    val errors = compileErrors("running(io ?=> Io.run(using io)(()))")
    assert(errors.nonEmpty, errors)
  }

  test("an op of a reactor backend cannot be performed with the backend itself, only with a program's Io") {
    val errors = compileErrors("def outside(using Async): Unit = NioBackend.perform(NioBackend.ops.timer(1))")
    assert(errors.nonEmpty, errors)
  }
}

/** What `NioBackend`'s reactors do that only the `Reactor` API shows. */
class NioReactorChecksSuite extends gears.async.io.internal.ReactorChecks {
  val backend: NioBackend.type = NioBackend
}
