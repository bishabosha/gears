package gears.async.io
package poller

import gears.async.Async
import gears.async.Future
import gears.async.io.flow.Flow
import gears.async.io.flow.Framing

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Try

/** Flows: lazy until run, and what they acquired released however a run ends. */
class FlowSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def pipe(io: Io[Pipes])(using Async): (io.ops.Readable, io.ops.Writable) = {
    val read = Slot[io.ops.Readable]()
    val write = Slot[io.ops.Writable]()
    io.perform(io.ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  private def bytes(text: String): ByteBuffer = ByteBuffer.wrap(text.getBytes(UTF_8))

  private def send(io: Io[ByteStreams])(to: io.ops.Writable, text: String)(using Async): Unit = {
    val buf = bytes(text)
    val write = io.ops.write(to, buf)
    while buf.hasRemaining() do io.perform(write)
  }

  /** A resource that writes when it was acquired and released into `log`. */
  private def logged(name: String, log: ListBuffer[String]): Flow[String] =
    Flow.resource { log += s"+$name"; name }(_ => log += s"-$name")

  test("a flow does nothing until it is run, and all of it each time it is") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val log = ListBuffer[String]()
      val flow = logged("a", log).flatMap(_ => Flow.of(1, 2, 3)).tap(n => log += s"$n").map(_ * 2)
      assertEquals(log.toList, Nil)
      val first = flow.toList
      val second = flow.toList
      assertEquals(first, List(2, 4, 6))
      assertEquals(second, List(2, 4, 6))
      assertEquals(log.toList, List("+a", "1", "2", "3", "-a", "+a", "1", "2", "3", "-a"))
    }
  }

  test("the stages give what collections' do") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val numbers = Flow.fromIterable(1 to 10)
      val evens = numbers.filter(_ % 2 == 0).toList
      val firstThree = numbers.take(3).toList
      val small = numbers.takeWhile(_ < 4).toList
      val late = numbers.drop(8).toList
      val sum = numbers.fold(0)(_ + _)
      val running = numbers.take(4).mapStateful(0)((sum, n) => (sum + n, sum + n)).toList
      val nested = Flow.of(1, 2).flatMap(n => Flow.of(n, n * 10)).toList
      val joined = (Flow.of(1) ++ Flow.of(2, 3)).toList
      val odd = numbers.collect { case n if n % 2 == 1 => s"$n" }.first
      val written = Flow
        .emit[Int] { emit =>
          var i = 0
          while i < 3 do { emit(i); i += 1 }
        }
        .toList
      val slept = Flow.of(1, 2).mapEval { n => io.sleep(1); n + 1 }.toList
      assertEquals(evens, List(2, 4, 6, 8, 10))
      assertEquals(firstThree, List(1, 2, 3))
      assertEquals(small, List(1, 2, 3))
      assertEquals(late, List(9, 10))
      assertEquals(sum, 55)
      assertEquals(running, List(1, 3, 6, 10))
      assertEquals(nested, List(1, 10, 2, 20))
      assertEquals(joined, List(1, 2, 3))
      assertEquals(odd, Some("1"))
      assertEquals(written, List(0, 1, 2))
      assertEquals(slept, List(2, 3))
      assertEquals(numbers.count, 10L)
      assertEquals(numbers.last, Some(10))
    }
  }

  test("what was acquired is released once, the last first, however the run ends") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val log = ListBuffer[String]()
      def both = logged("a", log).flatMap(a => logged("b", log).map(b => a + b))

      assertEquals(both.toList, List("ab"))
      assertEquals(log.toList, List("+a", "+b", "-b", "-a"))

      log.clear()
      val laterFailed = Try(both.map(_ => throw new IllegalStateException("later")).run())
      assert(laterFailed.isFailure)
      assertEquals(log.toList, List("+a", "+b", "-b", "-a"))

      log.clear()
      val earlierFailed = Try(both.flatMap(_ => Flow.failed(new IOException("inner"))).run())
      assert(earlierFailed.isFailure)
      assertEquals(log.toList, List("+a", "+b", "-b", "-a"))

      // A stage that wants no more ends the ones before it, which release as it returns.
      log.clear()
      val taken = both.flatMap(_ => Flow.fromIterable(1 to 100)).tap(n => log += s"$n").take(2).toList
      assertEquals(taken, List(1, 2))
      assertEquals(
        log.toList.filter(entry => entry.startsWith("+") || entry.startsWith("-")),
        List("+a", "+b", "-b", "-a")
      )

      // A failure of the release is the run's if nothing else failed, and added to what did otherwise.
      val releaseFails = Flow.resource(())(_ => throw new IOException("release"))
      val alone = Try(releaseFails.run())
      assertEquals(alone.failed.get.getMessage, "release")
      val added = Try(releaseFails.map(_ => throw new IllegalStateException("body")).run())
      assertEquals(added.failed.get.getMessage, "body")
      assertEquals(added.failed.get.getSuppressed.toList.map(_.getMessage), List("release"))
    }
  }

  test("a failure is recovered from by the stages after it, and a failure of those passes through") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val failing = Flow.of(1, 2) ++ Flow.failed(new IOException("gone"))
      val recovered = failing.recover { case _: IOException => -1 }.toList
      assertEquals(recovered, List(1, 2, -1))
      val replaced = failing.recoverWith { case _: IOException => Flow.of(7, 8) }.toList
      assertEquals(replaced, List(1, 2, 7, 8))

      // Not this flow's failure: the stage after it failed.
      var handled = 0
      val later = Try(
        Flow.of(1).recover { case _ => handled += 1; 0 }.map(_ => throw new IllegalStateException("later")).run()
      )
      assertEquals(later.failed.get.getMessage, "later")
      assertEquals(handled, 0)

      val seen = ListBuffer[String]()
      val told = Try(failing.onError(failure => seen += failure.getMessage).run())
      assert(told.isFailure)
      assertEquals(seen.toList, List("gone"))

      var runs = 0
      val flaky = Flow.defer {
        runs += 1
        if runs < 3 then Flow.failed(new IOException(s"run $runs")) else Flow.of(runs)
      }
      val retried = flaky.retry(5).toList
      assertEquals(retried, List(3))
      runs = 0
      val spent = Try(flaky.retry(1).toList)
      assertEquals(spent.failed.get.getMessage, "run 2")
    }
  }

  test("bytes are read and written, and a message that comes in pieces is one element") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val (echoIn, echoOut) = pipe(io)
      val echo = Future {
        Flow
          .read(io)(in, ByteBuffer.allocate(64))
          .frames(Framing.lines)
          .map(_.toUpperCase)
          .encode(ByteBuffer.allocate(64), 32)((line, buf) => buf.put((line + "\n").getBytes(UTF_8)): Unit)
          .writeTo(io)(echoOut)
          .run()
      }
      val lines = Future(Flow.read(io)(echoIn, ByteBuffer.allocate(64)).frames(Framing.lines).take(3).toList)
      send(io)(out, "one\ntw")
      io.sleep(20)
      send(io)(out, "o\r\nthr")
      io.sleep(20)
      send(io)(out, "ee\n")
      val got = lines.await
      assertEquals(got, List("ONE", "TWO", "THREE"))
      io.perform(io.ops.close(out)) // the end of the stream ends the echo
      echo.await
      io.perform(io.ops.close(in))
      io.perform(io.ops.close(echoIn))
      io.perform(io.ops.close(echoOut))
    }
  }

  test("a stream that ends within a message, and a message larger than its buffer, are failures") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      send(io)(out, "whole\npart")
      io.perform(io.ops.close(out))
      val seen = ListBuffer[String]()
      val ended = Try(Flow.read(io)(in, ByteBuffer.allocate(64)).frames(Framing.lines).foreach(seen += _))
      assertEquals(seen.toList, List("whole"))
      assert(ended.failed.get.isInstanceOf[EOFException], ended.toString)
      io.perform(io.ops.close(in))

      val (longIn, longOut) = pipe(io)
      send(io)(longOut, "a line longer than eight bytes\n")
      val tooLong = Try(Flow.read(io)(longIn, ByteBuffer.allocate(8)).frames(Framing.lines).run())
      assert(tooLong.failed.get.isInstanceOf[IOException], tooLong.toString)
      io.perform(io.ops.close(longIn))
      io.perform(io.ops.close(longOut))
    }
  }

  test("a read that waits too long fails with a timeout") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val (in, out) = pipe(io)
      val idle = Try(Flow.read(io)(in, ByteBuffer.allocate(8), idle = 30.millis).run())
      assert(idle.failed.get.isInstanceOf[TimeoutException], idle.toString)
      io.perform(io.ops.close(in))
      io.perform(io.ops.close(out))
    }
  }

  test("a cancelled run releases what it acquired, with ops, before it ends") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val log = ListBuffer[String]()
      val (in, out) = pipe(io)
      val waiting = Future {
        Flow
          .resource { log += "+pipe"; in } { held =>
            io.perform(io.ops.timer(5.millis)) // a release that waits is not cut short by the cancel
            io.perform(io.ops.close(held))
            log += "-pipe"
          }
          .flatMap(held => Flow.read(io)(held, ByteBuffer.allocate(8)))
          .run()
      }
      io.sleep(30)
      assertEquals(log.toList, List("+pipe"))
      waiting.cancel()
      val outcome = waiting.awaitResult
      assert(outcome.failed.get.isInstanceOf[CancellationException], outcome.toString)
      assertEquals(log.toList, List("+pipe", "-pipe"))
      // Closed already: a second close fails.
      val again = Try(io.perform(io.ops.close(in)))
      assert(again.isFailure)
      io.perform(io.ops.close(out))
    }
  }

  test("a server serves each connection in a flow of its own, and closes them all as it ends") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val served = new AtomicInteger
      val closed = new AtomicInteger
      val listening = Slot[io.ops.Listener]()
      io.perform(io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), SocketOptions.default, listening))
      val listener = listening.clear()
      val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
      val server = Future {
        Flow
          .accept(io)(listener, limit = 4) { connection =>
            served.incrementAndGet()
            Flow
              .read(io)(connection, ByteBuffer.allocate(256))
              .frames(Framing.lines)
              .encode(ByteBuffer.allocate(256), 64)((line, buf) => buf.put(s"echo $line\n".getBytes(UTF_8)): Unit)
              .writeTo(io)(connection)
              .onEnd(closed.incrementAndGet(): Unit)
          }
          .run()
      }
      def client(id: Int): Flow[String] =
        Flow.connect(io)(address).flatMap { connection =>
          Flow.eval(send(io)(connection, s"a$id\nb$id\n")).flatMap { _ =>
            Flow.read(io)(connection, ByteBuffer.allocate(256)).frames(Framing.lines).take(2)
          }
        }
      // More clients than the server serves at once: the rest wait their turn.
      val clients = (0 until 12).map(id => Future(client(id).toList))
      val answers = clients.map(_.await)
      assertEquals(answers.toList, (0 until 12).map(id => List(s"echo a$id", s"echo b$id")).toList)
      assertEquals(served.get(), 12)

      // One that stays connected is closed by the server as it ends.
      val staying = Future {
        Flow
          .connect(io)(address)
          .flatMap(connection => Flow.read(io)(connection, ByteBuffer.allocate(8)))
          .count
      }
      while served.get() < 13 do io.sleep(5)
      server.cancel()
      val ended = server.awaitResult
      assert(ended.failed.get.isInstanceOf[CancellationException], ended.toString)
      assertEquals(closed.get(), 13)
      val read = staying.await // the end of its stream: the server closed it
      assertEquals(read, 0L)
      io.perform(io.ops.close(listener))
    }
  }

  test("a connection's failure is its own: it is closed and told, and the server goes on") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val failures = new java.util.concurrent.ConcurrentLinkedQueue[String]()
      val listening = Slot[io.ops.Listener]()
      io.perform(io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), SocketOptions.default, listening))
      val listener = listening.clear()
      val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
      val server = Future {
        Flow
          .accept(io)(listener, failed = failure => failures.add(failure.getMessage): Unit) { connection =>
            Flow
              .read(io)(connection, ByteBuffer.allocate(64))
              .frames(Framing.lines)
              .map(line => if line == "bad" then throw new IllegalStateException("refused") else line)
              .encode(ByteBuffer.allocate(64), 32)((line, buf) => buf.put((line + "\n").getBytes(UTF_8)): Unit)
              .writeTo(io)(connection)
          }
          .run()
      }
      def ask(line: String): List[String] =
        Flow
          .connect(io)(address)
          .flatMap { connection =>
            Flow.eval(send(io)(connection, line + "\n")).flatMap { _ =>
              Flow.read(io)(connection, ByteBuffer.allocate(64)).frames(Framing.lines)
            }
          }
          .take(1)
          .toList
      val refused = ask("bad") // closed with no answer
      val answered = ask("good")
      assertEquals(refused, Nil)
      assertEquals(answered, List("good"))
      assertEquals(failures.toArray.toList, List[AnyRef]("refused"))
      server.cancel()
      server.awaitResult: Unit
      io.perform(io.ops.close(listener))
    }
  }

  test("stages in futures of their own: a buffer, a merge, and a map of several at once") {
    given PollerPlatform.backend.type = PollerPlatform.backend
    Io.run { io ?=>
      val log = ListBuffer[String]()
      val buffered = logged("source", log).flatMap(_ => Flow.fromIterable(1 to 50)).buffer(4).map(_ + 1).toList
      assertEquals(buffered, (2 to 51).toList)
      assertEquals(log.toList, List("+source", "-source"))

      // The stage after wants no more: the future before it is ended, and has released, by the time the run returns.
      log.clear()
      val ticking = logged("ticks", log).flatMap(_ => Flow.tick(io)(1.millis)).buffer(1).take(3).count
      assertEquals(ticking, 3L)
      assertEquals(log.toList, List("+ticks", "-ticks"))

      val merged = Flow.of(1, 2, 3).merge(Flow.of(10, 20)).toList
      assertEquals(merged.sorted, List(1, 2, 3, 10, 20))

      val failed = Try(Flow.tick(io)(1.millis).merge(Flow.failed(new IOException("one side"))).run())
      assertEquals(failed.failed.get.getMessage, "one side")

      val running = new AtomicInteger
      val most = new AtomicInteger
      val mapped = Flow
        .fromIterable(1 to 20)
        .mapPar(4) { n =>
          most.accumulateAndGet(running.incrementAndGet(), math.max(_, _))
          io.sleep(5)
          running.decrementAndGet()
          n * 2
        }
        .toList
      assertEquals(mapped, (1 to 20).map(_ * 2).toList)
      assert(most.get() > 1 && most.get() <= 4, s"${most.get()} at once")
    }
  }
}
