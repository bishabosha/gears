package gears.async.io
package testkit

import gears.async.io.Io
import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.AsyncOperations.sleep
import gears.async.Future.awaitAll
import gears.async.withTimeout

import scala.concurrent.duration.*

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import munit.FunSuite

import gears.async.io.Handle
import gears.async.io.Interest
import gears.async.io.IpAddress
import gears.async.io.Backend
import gears.async.io.PipeBackend
import gears.async.io.ResolveBackend
import gears.async.io.ResolvedAddress
import gears.async.io.Slot
import gears.async.io.SocketBackend
import gears.async.io.UnixAddress
import gears.async.io.UnixStreamBackend
import gears.async.io.testkit.BufferArena

/** A suite of programs over one backend: the backend under test, `running`, which starts a program over it the
  * platform's way (`ReactorFutures.run`), and what the checks share, buffers from an arena and a listener to connect
  * to. `FuturesContract` and the higher level's checks extend it.
  */
trait ProgramSuite extends FunSuite {

  /** The backend under test: one with sockets, pipes and name resolution, which the checks use. */
  val backend: Backend & SocketBackend & PipeBackend & ResolveBackend

  /** A listener for a test's server, and a way to connect to it: on a Unix socket in `dir` where the backend has
    * them, and on TCP at a free port of the loopback otherwise.
    */
  protected def listenAndConnect(r: Io[backend.type], dir: java.nio.file.Path)(using
      Async
  ): (Handle, () => Handle) = r.backend match {
    case unix: (gears.async.io.UnixStreams & r.backend.type) =>
      val address = UnixAddress(dir.resolve("echo.sock").toString)
      val listener = unix.handles.listener(address)
      r.perform(unix.ops.listening(listener))
      (listener, () => unix.handles.connect(address))
    case _ =>
      // A port nothing listens on: the first the listening op does not fail on.
      val random = new java.util.Random()
      var attempts = 0
      var found: (Handle, () => Handle) | Null = null
      while found == null do {
        val address = IpAddress.IPv4("127.0.0.1", 20000 + random.nextInt(30000))
        val listener = r.backend.handles.listener(address)
        try {
          r.perform(r.backend.ops.listening(listener))
          found = (listener, () => r.backend.handles.connect(address))
        } catch {
          case e: IOException =>
            r.perform(r.backend.ops.close(listener))
            attempts += 1
            if attempts > 10 then throw e
        }
      }
      found.nn
  }

  /** Runs `body` as a program over `backend`, the platform's way, given the backend's `Io`, and returns its result. */
  protected def running[T](body: Io[backend.type] => (Async.Spawn, AsyncOperations) ?=> T): T

  /** A port nothing listens on, found by listening on it with the backend and letting it go. */
  protected def freePort(): Int = running { r =>
    var port = -1
    val random = new java.util.Random()
    var attempts = 0
    while port < 0 do {
      val candidate = 20000 + random.nextInt(30000)
      val listener = r.backend.handles.listener(IpAddress.IPv4("127.0.0.1", candidate))
      try {
        r.perform(r.backend.ops.listening(listener))
        port = candidate
      } catch {
        case e: IOException =>
          attempts += 1
          if attempts > 10 then throw e
      } finally r.perform(r.backend.ops.close(listener))
    }
    port
  }

  // The buffers of every test, which their futures share across threads.
  protected lazy val arena: BufferArena = BufferArena.open()

  override def afterAll(): Unit = {
    arena.close()
    super.afterAll()
  }

  protected def withTempDir[T](body: java.nio.file.Path => T): T = {
    val dir = Files.createTempDirectory("gears")
    try body(dir)
    finally if Files.exists(dir) then {
      Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
      Files.deleteIfExists(dir): Unit
    }
  }

  protected def text(buf: ByteBuffer): String = {
    val copy = buf.duplicate().flip()
    val out = new Array[Byte](copy.remaining())
    copy.get(out)
    new String(out, StandardCharsets.UTF_8)
  }
}
