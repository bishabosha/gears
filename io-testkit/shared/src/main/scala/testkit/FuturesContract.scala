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

/** What gears programs over any backend must do, checked through `perform` alone, so that the same program runs the
  * same on every backend and every way of carrying its futures: a backend's suite extends this with the backend and
  * `running`, which starts a program the platform's way (`ReactorFutures.run`); NIO's does so in each carrier mode and
  * on one and several reactors, kqueue's on a pool of one and of several workers, Darwin's likewise. The checks with
  * stream sockets use Unix ones on a backend that has them, and TCP on the loopback otherwise.
  */
trait FuturesContract extends ProgramSuite {

  test("a program waits on a timer, and run returns what the program returns") {
    val waited = running { r =>
      val started = System.nanoTime()
      r.perform(r.backend.ops.timer(20))
      System.nanoTime() - started
    }
    assert(waited >= 20_000_000L, s"waited only ${waited / 1000} µs")
  }

  test("two futures stream bytes through a pipe") {
    val total = 1 << 20
    val received = running { r =>
      val (in, out) = pipe(r)
      try {
        val writer = Future {
          val buf = arena.allocate(8192)
          val write = r.backend.ops.write(out, buf)
          var sent = 0
          while sent < total do {
            buf.clear().limit(math.min(8192, total - sent))
            while buf.hasRemaining do r.perform(write)
            sent += buf.limit()
          }
          r.perform(r.backend.ops.close(out))
        }
        val reader = Future {
          val buf = arena.allocate(8192)
          val read = r.backend.ops.read(in, buf)
          var received = 0
          var atEnd = false
          while !atEnd do {
            buf.clear()
            r.perform(read)
            if buf.position() == 0 then atEnd = true else received += buf.position()
          }
          received
        }
        writer.await
        reader.await
      } finally r.perform(r.backend.ops.close(in))
    }
    assertEquals(received, total)
  }

  test("clients and a server echo over stream sockets") {
    val dir = Files.createTempDirectory("gears")
    val clients = 16
    val trips = 200
    val echoed =
      try running { r =>
      val (listener, connect) = listenAndConnect(r, dir)
      try {
        val server = Future {
          val accepted = Handle.Slot()
          val accept = r.backend.ops.accept(listener, accepted)
          val connections = (1 to clients).map { _ =>
            r.perform(accept)
            val socket = accepted.clear()
            Future {
              val buf = arena.allocate(64)
              val read = r.backend.ops.read(socket, buf)
              val write = r.backend.ops.write(socket, buf)
              try
                for _ <- 1 to trips do {
                  buf.clear().limit(5)
                  while buf.hasRemaining do r.perform(read)
                  buf.flip()
                  while buf.hasRemaining do r.perform(write)
                }
              finally r.perform(r.backend.ops.close(socket))
            }
          }
          connections.awaitAll
        }
        val done = (1 to clients).map { _ =>
          Future {
            val socket = connect()
            try {
              val buf = arena.allocate(64)
              val read = r.backend.ops.read(socket, buf)
              val write = r.backend.ops.write(socket, buf)
              for _ <- 1 to trips do {
                buf.clear().put("hello".getBytes(StandardCharsets.UTF_8)).flip()
                while buf.hasRemaining do r.perform(write)
                buf.clear().limit(5)
                while buf.hasRemaining do r.perform(read)
                assertEquals(text(buf), "hello")
              }
              1
            } finally r.perform(r.backend.ops.close(socket))
          }
        }
        val echoed = done.awaitAll.sum
        server.await
        echoed
      } finally r.perform(r.backend.ops.close(listener))
      } finally if Files.exists(dir) then {
        Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
        Files.deleteIfExists(dir): Unit
      }
    assertEquals(echoed, clients)
  }

  test("cancelling a future cancels the op it waits on, which may then be performed again") {
    val outcome = running { r =>
      val (in, out) = pipe(r)
      try {
        val buf = arena.allocate(16)
        val read = r.backend.ops.read(in, buf)
        val waiting = Future(r.perform(read))
        r.perform(r.backend.ops.timer(20)) // so that the read is pending
        waiting.cancel()
        val cancelled = waiting.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
        r.perform(r.backend.ops.write(out, arena.of("again".getBytes(StandardCharsets.UTF_8))))
        r.perform(read)
        s"$cancelled ${text(buf)}"
      } finally {
        r.perform(r.backend.ops.close(in))
        r.perform(r.backend.ops.close(out))
      }
    }
    assertEquals(outcome, "true again")
  }

  test("performing a read with a time limit throws TimeoutException, and the read may then be performed again") {
    val outcome = running { r =>
      val (in, out) = pipe(r)
      try {
        val buf = arena.allocate(16)
        val read = r.backend.ops.read(in, buf)
        val started = System.nanoTime()
        val timedOut =
          try {
            r.perform(read, timeoutMillis = 20)
            false
          } catch case _: TimeoutException => true
        val waited = System.nanoTime() - started
        r.perform(r.backend.ops.write(out, arena.of("in time".getBytes(StandardCharsets.UTF_8))))
        r.perform(read, timeoutMillis = 5_000)
        s"$timedOut ${waited >= 20_000_000L} ${buf.position()} ${text(buf)}"
      } finally {
        r.perform(r.backend.ops.close(in))
        r.perform(r.backend.ops.close(out))
      }
    }
    assertEquals(outcome, "true true 7 in time")
  }

  test("a blocking op past its time limit is interrupted, and perform throws TimeoutException") {
    val outcome = running { r =>
      r.backend match {
        case b: (gears.async.io.Blocking & r.backend.type) =>
          val returned = new java.util.concurrent.atomic.AtomicBoolean
          val op = b.ops.blocking { () =>
            try Thread.sleep(10_000)
            catch case _: InterruptedException => ()
            returned.set(true)
          }
          val started = System.nanoTime()
          val timedOut =
            try {
              r.perform(op, timeoutMillis = 20)
              false
            } catch case _: TimeoutException => true
          val waited = System.nanoTime() - started
          s"$timedOut ${returned.get()} ${waited >= 20_000_000L && waited < 5_000_000_000L}"
        case _ => "true true true" // no blocking ops to check
      }
    }
    assertEquals(outcome, "true true true")
  }

  test("a timer with a shorter time limit throws TimeoutException from perform") {
    val outcome = running { r =>
      val started = System.nanoTime()
      val timedOut =
        try {
          r.perform(r.backend.ops.timer(60_000), timeoutMillis = 20)
          false
        } catch case _: TimeoutException => true
      val waited = System.nanoTime() - started
      s"$timedOut ${waited >= 20_000_000L && waited < 30_000_000_000L}"
    }
    assertEquals(outcome, "true true")
  }

  test("gears' sleep and withTimeout run on the backend's timers: the program's Io is its AsyncOperations") {
    val outcome = running { r =>
      val (in, out) = pipe(r)
      try {
        val started = System.nanoTime()
        sleep(20)
        val slept = System.nanoTime() - started
        val buf = arena.allocate(16)
        val timedOut =
          try {
            withTimeout(20.millis)(r.perform(r.backend.ops.read(in, buf)))
            false
          } catch case _: TimeoutException => true
        s"${slept >= 20_000_000L} $timedOut ${buf.position()}"
      } finally {
        r.perform(r.backend.ops.close(in))
        r.perform(r.backend.ops.close(out))
      }
    }
    assertEquals(outcome, "true true 0")
  }

  test("a cancelled timer stops waiting, and may be performed again") {
    val outcome = running { r =>
      val timer = r.backend.ops.timer(60_000)
      val sleeping = Future(r.perform(timer))
      r.perform(r.backend.ops.timer(20)) // so that the long timer is waiting
      val started = System.nanoTime()
      sleeping.cancel()
      val cancelled = sleeping.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
      val waited = System.nanoTime() - started
      r.perform(r.backend.ops.timer(1)) // the op again, now for a moment
      s"$cancelled ${waited < 1_000_000_000L}"
    }
    assertEquals(outcome, "true true")
  }

  test("a read waiting on a socket is cancelled with the connection left open") {
    val outcome = withTempDir { dir =>
      running { r =>
        val (listener, connect) = listenAndConnect(r, dir)
        val client = connect()
        try {
          val accepted = Handle.Slot()
          r.perform(r.backend.ops.accept(listener, accepted))
          val server = accepted.clear()
          try {
            val buf = arena.allocate(16)
            val read = r.backend.ops.read(client, buf)
            val waiting = Future(r.perform(read))
            r.perform(r.backend.ops.timer(20))
            waiting.cancel()
            val cancelled = waiting.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
            r.perform(r.backend.ops.write(server, arena.of("still".getBytes(StandardCharsets.UTF_8))))
            r.perform(read)
            s"$cancelled ${text(buf)}"
          } finally r.perform(r.backend.ops.close(server))
        } finally {
          r.perform(r.backend.ops.close(client))
          r.perform(r.backend.ops.close(listener))
        }
      }
    }
    assertEquals(outcome, "true still")
  }

  test("an op waiting on a handle that is closed fails") {
    val failure = running { r =>
      val (in, out) = pipe(r)
      try {
        val waiting = Future(r.perform(r.backend.ops.read(in, arena.allocate(4))))
        r.perform(r.backend.ops.timer(20))
        r.perform(r.backend.ops.close(in))
        waiting.awaitResult.failed.toOption.orNull
      } finally r.perform(r.backend.ops.close(out))
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("a connection refused after waiting for it fails perform") {
    val port = freePort() // nothing listens on it: one just given up
    val failure = running { r =>
      val into = Handle.Slot()
      try {
        r.perform(r.backend.ops.connect(IpAddress.IPv4("127.0.0.1", port), into))
        r.perform(r.backend.ops.close(into.clear()))
        null
      } catch {
        case t: Throwable =>
          intercept[IllegalStateException](into.clear()) // a failed connect leaves its slot empty
          t
      }
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("an op that is not Repeatable is performed only once") {
    val failure = running { r =>
      val into = Slot[List[ResolvedAddress]]()
      val resolve = r.backend.ops.resolve("localhost", into)
      r.perform(resolve)
      into.clear()
      try {
        r.perform(resolve)
        null
      } catch case t: Throwable => t
    }
    assert(failure.isInstanceOf[IllegalStateException], String.valueOf(failure))
  }

  test("an op that fails fails perform") {
    val failure = running { r =>
      val (in, out) = pipe(r)
      r.perform(r.backend.ops.close(in))
      r.perform(r.backend.ops.close(out))
      try {
        r.perform(r.backend.ops.read(in, arena.allocate(4)))
        null
      } catch case t: Throwable => t
    }
    assert(failure.isInstanceOf[IOException], String.valueOf(failure))
  }

  test("a submission is cancelled when the group it was made in ends, and the group waits for its completion") {
    val outcome = running { r =>
      r match {
        case p: gears.async.io.OnPool[backend.type] =>
          val notified = new java.util.concurrent.atomic.AtomicBoolean
          val started = System.nanoTime()
          Async.group {
            p.submit(p.backend.ops.timer(60_000), new gears.async.io.Completion[p.Op] {
              def onComplete(op: p.Op): Unit = ()
              override def onCancel(op: p.Op): Unit = notified.set(true)
            })
          } // ends with the timer pending: the submission is cancelled, and its onCancel has run by here
          val waited = System.nanoTime() - started
          s"${notified.get()} ${waited < 1_000_000_000L}"
        case _ => "true true" // no submissions on a caller backend
      }
    }
    assertEquals(outcome, "true true")
  }

  test("a submission is a source: raced in select against a timer, and completed by its cancel") {
    val outcome = running { r =>
      r match {
        case p: gears.async.io.OnPool[backend.type] =>
          val (in, out) = pipe(r)
          try {
            val buf = arena.allocate(16)
            val read = p.submit(p.backend.ops.read(in, buf)) // pending: nothing is written
            val timer = p.submit(p.backend.ops.timer(20))
            val first = Async.select(timer.handle(_ => "timer"), read.handle(_ => "read"))
            read.cancel()
            val cancelled = read.awaitResult.failed.toOption.exists(_.isInstanceOf[CancellationException])
            val timed = timer.awaitResult.isSuccess
            s"$first $cancelled $timed"
          } finally {
            p.perform(p.backend.ops.close(in))
            p.perform(p.backend.ops.close(out))
          }
        case _ => "timer true true" // no submissions on a caller backend
      }
    }
    assertEquals(outcome, "timer true true")
  }

  test("a failure of the program is rethrown") {
    intercept[IllegalStateException](running(_ => throw new IllegalStateException("boom")))
  }
}
