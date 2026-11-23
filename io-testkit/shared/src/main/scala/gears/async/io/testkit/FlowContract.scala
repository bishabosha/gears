package gears.async.io
package testkit

import steps.result.Result
import steps.result.Result.eval.{ok, raise}
import gears.async.Async
import gears.async.Future
import gears.async.Semaphore
import gears.async.io.Flow.emit

import java.nio.ByteBuffer

/** What `Flow`, `Sink` and `Server` must do on a backend with IP stream sockets, under every way a server may serve
  * (`Server.Strategy`): a handler is the same under each, so each check is made under each. A strategy the backend
  * cannot serve by must be refused.
  */
trait FlowContract extends ContractSuite {
  val backend: Backend { type Interface <: IpStreams }

  private val Loopback = IpSocketAddress(IpAddress.IPv4Loopback, 0)

  private def sizes(read: Int = 4096, write: Int = 4096): Server.Sizes =
    Server.Sizes(read = read, write = write, buffers = allocate)

  private def connect(io: Io[backend.Interface], server: Server)(using Async): io.ops.Connection =
    opening(io)(slot => io.ops.connect(server.address, slot))

  private def send(io: Io[backend.Interface], to: io.ops.Connection, values: Array[Byte])(using Async): Unit = {
    val buf = allocate(values.length)
    buf.put(values).flip()
    writeAll(io, to, buf)
  }

  /** The next `count` bytes from `from`; fewer if it ends first. */
  private def receive(io: Io[backend.Interface], from: io.ops.Connection, count: Int)(using Async): Array[Byte] = {
    val buf = allocate(count)
    val read = io.ops.read(from, buf)
    var ended = false
    while buf.hasRemaining() && !ended do {
      val before = buf.position()
      try io.perform(read)
      catch case _: java.io.IOException => ended = true
      if buf.position() == before then ended = true
    }
    buf.flip()
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    out
  }

  /** Runs `check` against a server serving with `handler` by `strategy`, and then closes it and waits for `serve` to
    * return: its result, a failure if a handler failed. A strategy the backend has not must be refused instead.
    */
  private def serving(strategy: Server.Strategy, sizes: Server.Sizes = sizes())(handler: Connection => Async ?=> Unit)(
      check: (Io[backend.Interface], Server) => Async.Spawn ?=> Unit
  ): Throwable | Null =
    running { io ?=>
      if strategy != Server.Strategy.Auto && !Server.strategies(io).contains(strategy) then {
        intercept[IllegalArgumentException](Server.tcp(Loopback, sizes, strategy)): Unit
        Refused
      } else {
        val server = Server.tcp(Loopback, sizes, strategy)
        val served = Future(server.serve(handler))
        try check(io, server)
        finally server.close()
        failureOf(served)
      }
    }

  /** What `serving` gives for a strategy the backend has not, which was refused as it must be. */
  private object Refused extends Throwable

  private def assertServed(failure: Throwable | Null): Unit =
    assert(failure == null || (failure eq Refused), s"serve ended with $failure")

  private def copy(buf: ByteBuffer): Array[Byte] = {
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    out
  }

  test("a queue that grows after it has been taken from keeps what it holds, in order") {
    val ring = new Ring[String]()
    ring.add("first")
    assertEquals(ring.poll(), "first")
    for i <- 0 until 100 do ring.add(s"e$i")
    assertEquals(ring.size, 100)
    for i <- 0 until 100 do assertEquals(ring.poll(), s"e$i")
    assertEquals(ring.poll(), null)
    assert(ring.isEmpty)
  }

  test("a flow is the code that emits it, run each time the flow is, and not before") {
    running {
      var runs = 0
      val numbers = Flow[Int] {
        runs += 1
        emit(1)
        emit(2)
        emit(3)
      }
      assertEquals(runs, 0)
      assertEquals(numbers.toList, List(1, 2, 3))
      assertEquals(numbers.fold(0)(_ + _), 6)
      assertEquals(runs, 2)
    }
  }

  test("an emit returns when its consumer has done with the element") {
    running {
      val order = List.newBuilder[String]
      val flow = Flow[Int] {
        for i <- 1 to 2 do {
          order += s"emit $i"
          emit(i)
          order += s"emitted $i"
        }
      }
      for element <- flow do order += s"consumed $element"
      assertEquals(order.result(), List("emit 1", "consumed 1", "emitted 1", "emit 2", "consumed 2", "emitted 2"))
    }
  }

  test("a flow in a for: its elements filtered and mapped, and flows one inside another") {
    running {
      val evens = for element <- Flow.of(1, 2, 3, 4, 5, 6, 7, 8) if element % 2 == 0 yield element * 10
      assertEquals(evens.toList, List(20, 40, 60, 80))
      val pairs = for {
        letter <- Flow.of("a", "b")
        number <- Flow.of(1, 2)
      } yield s"$letter$number"
      assertEquals(pairs.toList, List("a1", "a2", "b1", "b2"))
    }
  }

  test("a flow's combinators") {
    running {
      val numbers = Flow.from(1 to 10)
      assertEquals(numbers.drop(7).toList, List(8, 9, 10))
      assertEquals(numbers.takeWhile(_ < 4).toList, List(1, 2, 3))
      assertEquals(numbers.take(0).toList, Nil)
      assertEquals(numbers.scan(0)(_ + _).take(4).toList, List(1, 3, 6, 10))
      assertEquals(Flow.of("a", "b").zipWithIndex.toList, List(("a", 0L), ("b", 1L)))
      assertEquals((Flow.of(1) ++ Flow.of(2, 3)).toList, List(1, 2, 3))
      assertEquals(numbers.collect { case n if n > 8 => n.toString }.toList, List("9", "10"))
      assertEquals(numbers.headOption, Some(1))
      assertEquals(Flow.empty.headOption, None)
      val seen = List.newBuilder[Int]
      assertEquals(numbers.tap(seen += _).take(2).toList, List(1, 2))
      assertEquals(seen.result(), List(1, 2))
      val doubled: Flow[Int] => Flow[Int] = _.map(_ * 2)
      assertEquals(Flow.of(1, 2, 3).through(doubled).toList, List(2, 4, 6))
    }
  }

  test("more combinators: sources, groups, and what a flow comes to") {
    running {
      val numbers = Flow.from(1 to 10)
      assertEquals(Flow.unfold(1)(n => if n > 4 then None else Some((n * n, n + 1))).toList, List(1, 4, 9, 16))
      assertEquals(Flow.iterate(1)(_ * 2).take(5).toList, List(1, 2, 4, 8, 16))
      assertEquals(Flow.repeat("x").take(3).toList, List("x", "x", "x"))
      assertEquals(Flow.concat(List(Flow.of(1, 2), Flow.empty, Flow.of(3))).toList, List(1, 2, 3))
      assertEquals(numbers.dropWhile(_ < 8).toList, List(8, 9, 10))
      assertEquals(numbers.mapConcat(n => if n < 3 then List(n, n) else Nil).toList, List(1, 1, 2, 2))
      assertEquals(numbers.grouped(4).toList, List(List(1, 2, 3, 4), List(5, 6, 7, 8), List(9, 10)))
      assertEquals(Flow.of(1, 1, 2, 2, 2, 1).changes.toList, List(1, 2, 1))
      assertEquals(Flow.of("a", "b", "c").intersperse(",").toList, List("a", ",", "b", ",", "c"))
      assertEquals(numbers.toVector, (1 to 10).toVector)
      assertEquals(numbers.count, 10L)
      assertEquals(numbers.sum, 55)
      assertEquals(numbers.reduceOption(_ max _), Some(10))
      assertEquals((Flow.empty: Flow[Int]).reduceOption(_ + _), None)
      assertEquals(numbers.lastOption, Some(10))
      assertEquals(numbers.find(_ > 6), Some(7))
      assert(numbers.exists(_ == 3) && !numbers.exists(_ > 10))
      assert(numbers.forall(_ > 0) && !numbers.forall(_ < 10))
      assert(numbers.contains(4) && !numbers.contains(11))
      assert(Flow.empty.isEmpty && !numbers.isEmpty)
      var ran = 0
      numbers.tap(_ => ran += 1).run()
      assertEquals(ran, 10)
    }
  }

  test("mapPar gives what is done in the order it came, without waiting for more to come") {
    running { io ?=>
      // The second is done first, and the first is still emitted first.
      val ordered = Flow.from(List(30, 1, 1)).mapPar(3) { millis => io.perform(io.ops.timer(millis)); millis }.toList
      assertEquals(ordered, List(30, 1, 1))
      // One element and then nothing for a long while: what it made does not wait for a second.
      val slow = Flow[Int] {
        emit(1)
        io.perform(io.ops.timer(60_000))
      }
      assertEquals(slow.mapPar(3)(_ * 2).headOption, Some(2))
    }
  }

  test("zip steps another flow beside this one, an element for an element, and leaves it when either ends") {
    running {
      var left = false
      val letters = Flow[String] {
        try for letter <- List("a", "b", "c", "d") do emit(letter)
        finally left = true
      }
      assertEquals(Flow.of(1, 2, 3).zip(letters).toList, List((1, "a"), (2, "b"), (3, "c")))
      assert(left)
      assertEquals(Flow.from(1 to 100).zip(Flow.of("x")).toList, List((1, "x")))
      val failing = Flow[Int] { emit(1); throw new IllegalStateException("zipped failed") }
      assertEquals(intercept[IllegalStateException](Flow.from(1 to 5).zip(failing).run()).getMessage, "zipped failed")
    }
  }

  test("merge runs flows beside one another: every element of each, each flow's in its order") {
    running {
      val merged = Flow.merge(Flow.from(0 until 50).map(_ * 2), Flow.from(0 until 50).map(_ * 2 + 1)).toList
      assertEquals(merged.sorted, (0 until 100).toList)
      assertEquals(merged.filter(_ % 2 == 0), (0 until 100 by 2).toList)
      assertEquals(merged.filter(_ % 2 == 1), (1 until 100 by 2).toList)
      assertEquals(Flow.of(1).merge(Flow.of(2)).toList.sorted, List(1, 2))
    }
  }

  test("mapPar runs up to so many at once, and no more, and emits every result") {
    running { io ?=>
      var running = 0
      var most = 0
      val guard = new Semaphore(1)
      def counted[T](body: => T)(using Async): T = {
        val held = guard.acquire()
        try body
        finally held.release()
      }
      val results = Flow.from(1 to 24).mapPar(4) { n =>
        counted { running += 1; most = math.max(most, running) }
        io.perform(io.ops.timer(5))
        counted(running -= 1)
        n * 10
      }.toList
      assertEquals(results.sorted, (1 to 24).map(_ * 10).toList)
      assert(most <= 4, s"$most at once")
      assert(most >= 2, s"only $most at once")
    }
  }

  test("futures parked on a stream's next element, and their dealer, are ended by a cancel") {
    running { io ?=>
      var streamsLeft = 0
      val guard = new Semaphore(1)
      val serving = Future {
        // Two keys, an element each, and then nothing more for as long as anyone waits.
        val slow = Flow[Int] {
          emit(1)
          emit(2)
          io.perform(io.ops.timer(60_000))
        }
        slow.groupBy(_ % 2).foreachPar { (_, elements) =>
          try elements.run()
          finally {
            val held = guard.acquire()(using summon[Async].withGroup(gears.async.CompletionGroup.Unlinked))
            try streamsLeft += 1
            finally held.release()
          }
        }
      }
      settle(io)
      serving.cancel()
      assert(failureOf(serving).isInstanceOf[java.util.concurrent.CancellationException], s"${failureOf(serving)}")
      assertEquals(streamsLeft, 2)
    }
  }

  test("a flow that is left early is left through its producer's finally, and emits no more") {
    running {
      var emitted = 0
      var left = false
      val endless = Flow[Int] {
        try
          while true do {
            emitted += 1
            emit(emitted)
          }
        finally left = true
      }
      assertEquals(endless.take(3).toList, List(1, 2, 3))
      assertEquals(emitted, 3)
      assert(left)
    }
  }

  test("a flow of a million elements runs in the stack its stages take, not its elements") {
    running {
      val count = 1_000_000
      val sum = Flow.from(0 until count).map(_ + 1).filter(_ % 2 == 0).map(_.toLong).fold(0L)(_ + _)
      assertEquals(sum, (1L to count).filter(_ % 2 == 0).sum)
      var seen = 0
      for element <- Flow[Int] { var i = 0; while i < count do { emit(i); i += 1 } } do seen += 1
      assertEquals(seen, count)
    }
  }

  test("many flows are joined with concat, and run one after another on a stack no deeper for it") {
    running {
      val flows = List.tabulate(20_000)(i => Flow.of(i))
      assertEquals(Flow.concat(flows).fold(0L)(_ + _), (0L until 20_000).sum)
      assertEquals(Flow.concat(flows).depth, 1)
    }
  }

  test("a pipeline of very many stages runs, cut onto stacks of its own, in order") {
    running {
      // Fifteen cuts. Ten times as many run too, but on Node.js leave it unable to suspend afterwards: see the
      // design document.
      val stages = 2_000
      var flow: Flow[Int] = Flow.from(1 to 50)
      for _ <- 1 to stages do flow = flow.map(_ + 1)
      assert(flow.depth <= Flow.Segment + 1, s"depth ${flow.depth}")
      assertEquals(flow.toList, (1 to 50).map(_ + stages).toList)
      // Joined with `++` in a fold, which is as deep.
      val joined = List.tabulate(600)(i => Flow.of(i)).foldLeft(Flow.empty: Flow[Int])(_ ++ _)
      assertEquals(joined.toList, List.tabulate(600)(identity))
    }
  }

  test("a long pipeline left early has its producer leave through its finally, and its failure is its consumer's") {
    running {
      var left = false
      var emitted = 0
      var flow: Flow[Int] = Flow[Int] {
        try
          while true do {
            emitted += 1
            emit(emitted)
          }
        finally left = true
      }
      for _ <- 1 to 1_000 do flow = flow.map(_ + 1)
      assertEquals(flow.take(3).toList, List(1001, 1002, 1003))
      assert(left)
      assert(emitted <= 4, s"emitted $emitted")

      var failing: Flow[Int] = Flow[Int] {
        emit(1)
        throw new IllegalStateException("deep producer failed")
      }
      for _ <- 1 to 1_000 do failing = failing.map(_ + 1)
      val seen = List.newBuilder[Int]
      val thrown = intercept[IllegalStateException](failing.foreach(seen += _))
      assertEquals(thrown.getMessage, "deep producer failed")
      assertEquals(seen.result(), List(1001))
    }
  }

  test("a flow's failure is thrown where the flow is run") {
    running {
      val failing = Flow[Int] {
        emit(1)
        throw new IllegalStateException("producer failed")
      }
      val seen = List.newBuilder[Int]
      val thrown = intercept[IllegalStateException](failing.foreach(seen += _))
      assertEquals(thrown.getMessage, "producer failed")
      assertEquals(seen.result(), List(1))
    }
  }

  test("groupBy and foreachPar: every element reaches its key's flow, in order") {
    running {
      val seen = new java.util.concurrent.ConcurrentHashMap[Int, List[Int]]()
      Flow.from(1 to 40).groupBy(_ % 4).foreachPar { (key, elements) =>
        seen.put(key, elements.fold(List.empty[Int])((sofar, next) => next :: sofar).reverse): Unit
      }
      for key <- 0 until 4 do assertEquals(seen.get(key), (1 to 40).filter(_ % 4 == key).toList)
    }
  }

  test("foreachPar throws a failure of one of its flows") {
    running {
      val thrown = intercept[IllegalStateException] {
        Flow.of(1, 2, 3).foreachPar(element => if element == 2 then throw new IllegalStateException("two"))
      }
      assertEquals(thrown.getMessage, "two")
    }
  }

  for machine <- List(true, false) do {
    val how = if machine then "by the machine where there is one" else "by what runs a handler"

    test(s"a pipeline of stages is served $how: bytes in, bytes out, and the server ends when closed") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              // The even bytes, each with the one after it.
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .filter(_ % 2 == 0)
                .mapConcat(value => List(value, value + 1))
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val clients = List.fill(5)(connect(io, server))
          val asking = clients.map { client =>
            Future {
              for round <- 0 until 20 do {
                send(io, client, Array.tabulate(8)(i => (round + i).toByte))
                val evens = (0 until 8).map(round + _).filter(_ % 2 == 0)
                assertEquals(receive(io, client, 8).toList, evens.flatMap(v => List(v.toByte, (v + 1).toByte)).toList)
              }
            }
          }
          asking.foreach(_.await)
          clients.foreach(close(io, _))
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"streams dealt through pipelines of their own, served $how, keep what is each key's") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .groupThrough(_ & 3) { (key, values) =>
                  // A count of its own for each key.
                  var seen = 0
                  values.map { value =>
                    seen += 1
                    ByteBuffer.wrap(Array[Byte](key.toByte, seen.toByte, value.toByte))
                  }
                },
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array.tabulate(64)(_.toByte))
          val answers =
            receive(io, client, 64 * 3).grouped(3).map(triple => (triple(0).toInt, triple(1).toInt, triple(2) & 0xff)).toList
          assertEquals(answers, (0 until 64).map(value => (value & 3, value / 4 + 1, value)).toList)
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"more comes out of a pipeline than a write holds, served $how, and all of it arrives") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes(read = 256, write = 512))
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                // Each within what a write holds, as a send must be; together many times that.
                .map(value => ByteBuffer.wrap(Array.fill[Byte](400)(value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array.tabulate(40)(_.toByte))
          val got = receive(io, client, 40 * 400)
          assertEquals(got.length, 40 * 400)
          for at <- got.indices do if got(at) != (at / 400).toByte then fail(s"byte $at is ${got(at)}")
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"stages that keep something and a stage that wants no more, served $how: the connection is closed at it") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .drop(2)
                .takeWhile(_ < 100)
                .scan(0)(_ + _)
                .zipWithIndex
                .take(14)
                // The last group is of what is left when `take` ends the pipeline.
                .grouped(4)
                .map { group =>
                  val out = ByteBuffer.allocate(group.length * 2)
                  for (sum, index) <- group do out.put(sum.toByte).put(index.toByte)
                  out.flip()
                  out
                },
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array.tabulate(20)(i => (i + 1).toByte))
          // Fourteen answers and then the end, with the client's own end not yet sent.
          val got = receive(io, client, 64).grouped(2).map(pair => (pair(0) & 0xff, pair(1).toInt)).toList
          val sums = (3 to 16).scanLeft(0)(_ + _).tail.map(_ & 0xff)
          assertEquals(got, sums.zipWithIndex.toList)
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"a key's pipeline that wants no more, served $how, ends its key and leaves the others") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .groupThrough(_ & 1)((key, values) => if key == 0 then values.take(2) else values)
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array.tabulate(10)(_.toByte))
          assertEquals(receive(io, client, 7).toList, List[Byte](0, 1, 2, 3, 5, 7, 9))
          send(io, client, Array[Byte](10, 11))
          assertEquals(receive(io, client, 1).toList, List[Byte](11))
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"a pipeline that is not of stages alone, served $how, is run instead, and served the same") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              Flow[ByteBuffer] {
                connection.bytes.foreach { chunk =>
                  val doubled = ByteBuffer.allocate(chunk.remaining())
                  while chunk.hasRemaining() do doubled.put((chunk.get() * 2).toByte)
                  doubled.flip()
                  emit(doubled)
                }
              },
            machine
          )
        }
        try {
          val first = connect(io, server)
          val second = connect(io, server)
          send(io, first, Array[Byte](1, 2, 3))
          send(io, second, Array[Byte](10, 20))
          assertEquals(receive(io, second, 2).toList, List[Byte](20, 40))
          assertEquals(receive(io, first, 3).toList, List[Byte](2, 4, 6))
          close(io, first)
          close(io, second)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"a stage that waits, served $how: its work under way beside the pipeline, no more of it than asked") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val under = new java.util.concurrent.atomic.AtomicInteger(0)
        val most = new java.util.concurrent.atomic.AtomicInteger(0)
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .mapAsync(3) { value =>
                  val now = under.incrementAndGet()
                  most.accumulateAndGet(now, math.max(_, _)): Unit
                  io.perform(io.ops.timer(5))
                  under.decrementAndGet(): Unit
                  value * 2
                }
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          val sent = Array.tabulate(30)(i => (i + 1).toByte)
          send(io, client, sent)
          assertEquals(receive(io, client, sent.length).map(_.toInt).sorted.toList, sent.map(_ * 2).toList)
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
        assert(most.get() <= 3, s"${most.get()} at once")
        assert(most.get() >= 2, s"only ${most.get()} at once")
      }
    }

    test(s"a stage that waits and keeps the order, served $how: what is done first still waits its turn") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .mapPar(4) { value =>
                  // The earlier the slower.
                  io.perform(io.ops.timer(if value % 4 == 0 then 12 else 1))
                  value + 1
                }
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          val sent = Array.tabulate(24)(_.toByte)
          send(io, client, sent)
          assertEquals(receive(io, client, sent.length).toList, sent.map(value => (value + 1).toByte).toList)
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"work still under way when its pipeline wants no more, served $how, is cancelled") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val cancelled = new java.util.concurrent.atomic.AtomicInteger(0)
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .mapAsync(4) { value =>
                  // One is quick; the rest would be a minute.
                  try io.perform(io.ops.timer(if value == 1 then 1 else 60_000))
                  catch { case thrown: Throwable => cancelled.incrementAndGet(); throw thrown }
                  value
                }
                .take(1)
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array[Byte](0, 1, 2, 3))
          // The one answer and then the end.
          assertEquals(receive(io, client, 4).toList, List[Byte](1))
          close(io, client)
        } finally server.close()
        // And the server ends, which it would not for a minute with that work left to itself.
        assertEquals(failureOf(served), null)
        assertEquals(cancelled.get(), 3)
      }
    }

    test(s"a server cancelled, served $how, gives up all it has: pipelines, work, connections and listener") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val made = new java.util.concurrent.atomic.AtomicInteger(0)
        val begun = new java.util.concurrent.atomic.AtomicInteger(0)
        val completed = new java.util.concurrent.atomic.AtomicInteger(0)
        val cancelled = new java.util.concurrent.atomic.AtomicInteger(0)
        val served = Future {
          server.serveFlow(
            connection => {
              made.incrementAndGet(): Unit
              connection.bytes
                .onComplete(completed.incrementAndGet(): Unit)
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .mapAsync(2) { value =>
                  begun.incrementAndGet(): Unit
                  try io.perform(io.ops.timer(60_000))
                  catch { case thrown: Throwable => cancelled.incrementAndGet(); throw thrown }
                  value
                }
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte)))
            },
            machine
          )
        }
        // Three with work under way, a minute of it, and one that has sent nothing.
        val busy = List.fill(3)(connect(io, server))
        val idle = connect(io, server)
        busy.foreach(send(io, _, Array[Byte](1)))
        while begun.get() < 3 || made.get() < 4 do io.perform(io.ops.timer(2))
        served.cancel()
        // It ends, and not in a minute; and by then all of it is done.
        served.awaitResult: Unit
        assertEquals(cancelled.get(), 3)
        assertEquals(completed.get(), 4)
        for client <- idle :: busy do assertEquals(receive(io, client, 1).length, 0)
        (idle :: busy).foreach(close(io, _))
        assert(scala.util.Try(connect(io, server)).isFailure, "the listener is still open")
      }
    }

    test(s"a stage's onComplete, served $how, is run once: at the end, at a failure, and at a stage that wants no more") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val completed = new java.util.concurrent.ConcurrentLinkedQueue[String]()
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .onComplete(completed.add("bytes"): Unit)
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .map(value => if value == 'X' then throw new IllegalStateException("stage failed") else value)
                .takeWhile(_ != 'S')
                .onComplete(completed.add("after"): Unit)
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        def once(what: String): Unit = {
          while completed.size() < 2 do io.perform(io.ops.timer(2))
          io.perform(io.ops.timer(10))
          assertEquals(List(completed.poll(), completed.poll(), completed.poll()).map(String.valueOf).sorted, List("after", "bytes", "null"), what)
        }
        try {
          val ending = connect(io, server)
          send(io, ending, Array[Byte]('a'))
          assertEquals(receive(io, ending, 1).toList, List[Byte]('a'))
          close(io, ending)
          once("at the end")
          val failing = connect(io, server)
          send(io, failing, Array[Byte]('X'))
          assertEquals(receive(io, failing, 1).length, 0)
          once("at a failure")
          close(io, failing)
          val stopping = connect(io, server)
          send(io, stopping, Array[Byte]('b', 'S', 'c'))
          assertEquals(receive(io, stopping, 3).toList, List[Byte]('b'))
          once("at a stage that wants no more")
          close(io, stopping)
        } finally server.close()
        assert(failureOf(served).isInstanceOf[IllegalStateException], s"${failureOf(served)}")
      }
    }

    test(s"a stage that is code, served $how: it waits, keeps what it likes, and its try is its error handling") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val closed = new java.util.concurrent.atomic.AtomicInteger(0)
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .map(value => if value == 'X' then throw new IllegalStateException("bad byte") else value)
                .direct[Int] { values =>
                  var count = 0
                  try
                    for value <- values do {
                      // An op for each, as a call elsewhere would be.
                      io.perform(io.ops.timer(1))
                      count += 1
                      emit(value + count)
                    }
                  catch case _: IllegalStateException => emit('!'.toInt)
                  finally closed.incrementAndGet(): Unit
                }
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array[Byte](10, 20, 30))
          assertEquals(receive(io, client, 3).toList, List[Byte](11, 22, 33))
          // What failed before the stage is caught in it, answered, and the connection ended as if it had ended.
          send(io, client, Array[Byte](40, 'X', 50))
          assertEquals(receive(io, client, 4).toList, List[Byte](44, '!'))
          close(io, client)
          // And a connection cancelled with the stage waiting: its finally is run.
          val idle = connect(io, server)
          send(io, idle, Array[Byte](1))
          assertEquals(receive(io, idle, 1).toList, List[Byte](2))
          served.cancel()
          served.awaitResult: Unit
          assertEquals(receive(io, idle, 1).length, 0)
          close(io, idle)
          // Scala Native skips a `finally` whose `catch` calls a method that throws (`DirectStageFinallyBehavior`): the
          // cancel is caught as the `IllegalStateException` it is, and the handler's emit throws it again.
          if machine && System.getProperty("java.vm.name") == "Scala Native" then assert(closed.get() >= 1)
          else assertEquals(closed.get(), 2)
        } finally server.close()
      }
    }

    test(s"a stage that is code with failures as values, served $how: raised, handled, and cleaned up in a finally") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val closed = new java.util.concurrent.atomic.AtomicInteger(0)
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .directResult[Int, String] { values =>
                  var count = 0
                  // No `catch`: a failure is a value, and the `finally` is for what must be let go.
                  try
                    for value <- values do {
                      if value == 'X' then raise("bad byte")
                      io.perform(io.ops.timer(1))
                      count += 1
                      emit(value + count)
                    }
                  finally closed.incrementAndGet(): Unit
                }
                .handleErr(_ => '!'.toInt)
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array[Byte](10, 20, 30))
          assertEquals(receive(io, client, 3).toList, List[Byte](11, 22, 33))
          // The failure is answered, and the connection ended as if it had ended.
          send(io, client, Array[Byte](40, 'X', 50))
          assertEquals(receive(io, client, 4).toList, List[Byte](44, '!'))
          close(io, client)
          // And a connection cancelled with the stage waiting: its finally is run, on every platform.
          val idle = connect(io, server)
          send(io, idle, Array[Byte](1))
          assertEquals(receive(io, idle, 1).toList, List[Byte](2))
          served.cancel()
          served.awaitResult: Unit
          assertEquals(receive(io, idle, 1).length, 0)
          close(io, idle)
          assertEquals(closed.get(), 2)
        } finally server.close()
      }
    }

    test(s"a failure in one key's pipeline, served $how, recovered there, ends that key and leaves the others") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .groupThrough(_ & 1) { (key, values) =>
                  values
                    .mapAsync(1) { value =>
                      io.perform(io.ops.timer(1))
                      if value == 4 then throw new IllegalStateException("four") else value
                    }
                    .recover { case _: IllegalStateException => 100 + key }
                }
                .map(value => ByteBuffer.wrap(Array[Byte](value.toByte))),
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array.tabulate(10)(_.toByte))
          // Key 0: 0, 2, then its failure's answer and no more; key 1: all of its own.
          val got = receive(io, client, 8).map(_.toInt).toList
          assertEquals(got.filter(v => v % 2 == 0), List(0, 2, 100))
          assertEquals(got.filter(v => v % 2 == 1), List(1, 3, 5, 7, 9))
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"a stage that waits in each key's pipeline, served $how, answers every element, a key's in order") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .groupThrough(_ & 3) { (key, values) =>
                  values.mapAsync(1) { value =>
                    // The slower the lower, so that a key's order is the stage's doing, not the clock's.
                    io.perform(io.ops.timer(if value < 32 then 2 else 0))
                    ByteBuffer.wrap(Array[Byte](key.toByte, value.toByte))
                  }
                },
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array.tabulate(64)(_.toByte))
          val answers = receive(io, client, 64 * 2).grouped(2).map(pair => (pair(0).toInt, pair(1) & 0xff)).toList
          assertEquals(answers.length, 64)
          for key <- 0 until 4 do
            assertEquals(answers.filter(_._1 == key).map(_._2), (0 until 64).filter(_ % 4 == key).toList)
          // And the connection is closed when the last of it is done, the client having ended first.
          close(io, client)
        } finally server.close()
        assertEquals(failureOf(served), null)
      }
    }

    test(s"a stage that waits and fails, served $how, fails its connection") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes
                .process[Int](emit => chunk => while chunk.hasRemaining() do emit(chunk.get() & 0xff))
                .mapAsync(2) { value =>
                  io.perform(io.ops.timer(1))
                  if value == 'X' then throw new IllegalStateException("work failed")
                  ByteBuffer.wrap(Array[Byte](value.toByte))
                },
            machine
          )
        }
        try {
          val client = connect(io, server)
          send(io, client, Array[Byte]('X'))
          assertEquals(receive(io, client, 1).length, 0)
          close(io, client)
        } finally server.close()
        assert(failureOf(served).isInstanceOf[IllegalStateException], s"${failureOf(served)}")
      }
    }

    test(s"a stage that fails, served $how, closes its connection, leaves the others, and is thrown") {
      running { io ?=>
        val server = Server.tcp(Loopback, sizes())
        val served = Future {
          server.serveFlow(
            connection =>
              connection.bytes.map { chunk =>
                if chunk.get(chunk.position()) == 'X' then throw new IllegalStateException("stage failed")
                chunk
              },
            machine
          )
        }
        try {
          val failing = connect(io, server)
          val fine = connect(io, server)
          send(io, failing, Array[Byte]('X'))
          assertEquals(receive(io, failing, 1).length, 0)
          send(io, fine, Array[Byte]('a', 'b'))
          assertEquals(receive(io, fine, 2).toList, List[Byte]('a', 'b'))
          close(io, failing)
          close(io, fine)
        } finally server.close()
        val failure = failureOf(served)
        assert(failure.isInstanceOf[IllegalStateException], s"serveFlow ended with $failure")
      }
    }
  }

  test("stages are the same run as stepped: process and groupThrough under any Async") {
    running {
      val seen = Flow
        .from(1 to 12)
        .process[Int](emit => value => { emit(value); if value % 6 == 0 then emit(-value) })
        .groupThrough(value => math.abs(value) % 3)((key, values) => values.map(value => s"$key:$value"))
        .toList
      assertEquals(seen.count(_.startsWith("0:")), 6)
      assertEquals(seen.filter(_.startsWith("1:")), List("1:1", "1:4", "1:7", "1:10"))
    }
  }

  test("every stage does the same stepped as run") {
    running {
      val pipes = List[(String, Flow[Int] => Flow[Any])](
        "take" -> (_.take(5)),
        "take none" -> (_.take(0)),
        "take more than there are" -> (_.take(500)),
        "takeWhile" -> (_.takeWhile(_ < 7)),
        "drop" -> (_.drop(3)),
        "dropWhile" -> (_.dropWhile(_ < 4)),
        "scan" -> (_.scan(0)(_ + _)),
        "zipWithIndex" -> (_.zipWithIndex),
        "grouped" -> (_.grouped(4)),
        "grouped, exactly" -> (_.grouped(5)),
        "changes" -> (_.map(_ / 3).changes),
        "intersperse" -> (_.intersperse(-1)),
        "mapStateful" -> (_.mapStateful(1)((product, n) => (product * n % 1000, product))),
        "mapConcat, filter, collect" -> (_.mapConcat(n => List(n, n)).filter(_ % 2 == 0).collect { case n if n > 4 => n.toString }),
        "a stage that is code" -> (_.direct[Int] { elements =>
          var sum = 0
          for element <- elements do {
            sum += element
            if element % 2 == 0 then emit(sum)
          }
          emit(-sum)
        }),
        "a stage that is code, and leaves early" -> (_.direct[Int] { elements =>
          scala.util.boundary {
            for element <- elements do {
              if element > 6 then scala.util.boundary.break()
              emit(element * 2)
              emit(element * 2 + 1)
            }
          }
        }),
        "a stage that is code, between stages that are not" -> (_.scan(0)(_ + _).direct[String] { sums =>
          for sum <- sums do if sum % 3 == 0 then emit(s"<$sum>")
        }.intersperse(",").take(9)),
        "a failure recovered" -> (_.map(n => if n == 7 then throw new IllegalStateException("seven") else n).recover {
          case failed: IllegalStateException => -7
        }),
        "a failure recovered by a stage that is code" -> (_.map(n => if n == 5 then throw new IllegalStateException("five") else n)
          .direct[Int] { elements =>
            try for element <- elements do emit(element)
            catch case _: IllegalStateException => emit(-5)
            emit(0)
          }),
        "a failure of a stage that is code, recovered after it" -> (_.direct[Int] { elements =>
          for element <- elements do if element == 4 then throw new IllegalStateException("four") else emit(element)
        }.map(_ * 10).recover { case _: IllegalStateException => -4 }),
        "a failure of one key recovered in it" -> (_.groupThrough(_ % 2) { (key, values) =>
          values.map(n => if n == 6 then throw new IllegalStateException("six") else n).recover {
            case _: IllegalStateException => -100 - key
          }
        }),
        "a failure before the keys, recovered in each" -> (_.map(n => if n == 9 then throw new IllegalStateException("nine") else n)
          .groupThrough(_ % 3)((key, values) => values.recover { case _: IllegalStateException => -key })
          .recover { case _: IllegalStateException => -99 }),
        "results, as far as the first Err" -> (_.map(n => Result.cond(n != 6, n, s"bad $n")).failFast),
        "results, a stage that may fail and those after it" -> (_.map[Result[Int, String]](Result.Ok(_))
          .flatMapOk(n => if n == 5 then Result.Err("five") else Result.Ok(n * 2))
          .mapOk(_ + 1)
          .mapErr(_.length)),
        "results, a failure handled" -> (_.map[Result[Int, String]](Result.Ok(_))
          .flatMapOk(n => if n == 5 then Result.Err("five") else Result.Ok(n * 2))
          .handleErr(_.length)),
        "results, a stage that is code and raises" -> (_.directResult[Int, String] { elements =>
          for n <- elements do {
            if n == 8 then raise("eight")
            emit(n * 3)
          }
        }),
        "results, a stage that is code and unwraps" -> (_.map(n => Result.cond(n != 4, n, "four")).directResult[String, String] {
          elements =>
            var sum = 0
            for element <- elements do {
              sum += element.ok
              emit(s"sum $sum")
            }
        }),
        "results, a stage that is code over the values" -> (_.map(n => Result.cond(n != 7, n, "seven")).directOk[Int, String] {
          values => for value <- values do emit(value + 1)
        }.handleErr(_ => -7)),
        "results, of what is thrown" -> (_.map(n => if n == 3 then throw new IllegalStateException("three") else n).attempt
          .handleErr(_.getMessage.length)),
        "results, a key's failure handled in the key" -> (_.groupThrough(_ % 2) { (key, values) =>
          values
            .map[Result[Int, String]](Result.Ok(_))
            .flatMapOk(n => if n == 6 then Result.Err("six") else Result.Ok(n))
            .handleErr(_ => -100 - key)
        }),
        "many together" -> (_.drop(1).scan(0)(_ + _).takeWhile(_ < 60).zipWithIndex.grouped(3).intersperse(Nil).take(4))
      )
      for (name, pipe) <- pipes do {
        val run = pipe(Flow.from(1 to 20)).toList
        // One key, so one pipeline of steps for all of it.
        val stepped = Flow.from(1 to 20).groupThrough(_ => 0)((_, values) => pipe(values)).toList
        assertEquals(stepped, run, name)
      }
      // Run for its result: the end, or the first `Err`, at which the flow is left.
      val checked = Flow.from(1 to 20).map(n => Result.cond(n != 6, n, s"bad $n"))
      assertEquals(checked.runResult(), Result.Err("bad 6"))
      assertEquals(checked.toListResult(), Result.Err("bad 6"))
      assertEquals(Flow.from(1 to 4).map[Result[Int, String]](Result.Ok(_)).toListResult(), Result.Ok(List(1, 2, 3, 4)))
      assertEquals(Flow.from(1 to 4).map[Result[Int, String]](Result.Ok(_)).runResult(), Result.done)
      // A stage that is code and raises: what it had to clean up is cleaned up, in a `finally` with no `catch`.
      for stepped <- List(false, true) do {
        var cleaned = 0
        val pipe: Flow[Int] => Flow[Result[Int, String]] = _.directResult[Int, String] { elements =>
          try for n <- elements do if n == 3 then raise("three") else emit(n)
          finally cleaned += 1
        }
        val flow = if stepped then Flow.from(1 to 9).groupThrough(_ => 0)((_, values) => pipe(values)) else pipe(Flow.from(1 to 9))
        assertEquals(flow.toList, List(Result.Ok(1), Result.Ok(2), Result.Err("three")), s"stepped: $stepped")
        assertEquals(cleaned, 1, s"stepped: $stepped")
      }
      // What is not recovered is thrown, the same either way: a failure after `recover`, which is not its to
      // recover; one that no handler has anything for; one of a stage that is code.
      val failing = List[(String, Flow[Int] => Flow[Any])](
        "after recover" -> (_.recover { case _: Throwable => -1 }.map(n => if n == 3 then throw new IllegalStateException("after") else n)),
        "after a stage that is code" -> (_.direct[Int] { elements =>
          try for element <- elements do emit(element)
          catch case _: IllegalStateException => emit(-1)
        }.map(n => if n == 3 then throw new IllegalStateException("after") else n)),
        "not handled" -> (_.map(n => if n == 3 then throw new IllegalStateException("after") else n).recover {
          case _: java.io.IOException => -1
        }),
        "of a stage that is code" -> (_.direct[Int] { elements =>
          for element <- elements do if element == 3 then throw new IllegalStateException("after") else emit(element)
        }),
        "of a key, not recovered" -> (_.groupThrough(_ % 2)((_, values) =>
          values.map(n => if n == 3 then throw new IllegalStateException("after") else n)
        ))
      )
      for (name, pipe) <- failing do {
        val seenRun = new java.util.ArrayList[Any]()
        val run = intercept[IllegalStateException](pipe(Flow.from(1 to 20)).foreach(seenRun.add(_): Unit))
        val seenStepped = new java.util.ArrayList[Any]()
        val stepped = intercept[IllegalStateException] {
          Flow.from(1 to 20).groupThrough(_ => 0)((_, values) => pipe(values)).foreach(seenStepped.add(_): Unit)
        }
        assertEquals(run.getMessage, "after", name)
        assertEquals(stepped.getMessage, "after", name)
        assertEquals(seenStepped, seenRun, name)
      }
      // `onError` is shown a failure of what comes before it, once, and not one of what comes after.
      for stepped <- List(false, true) do {
        val shown = new java.util.ArrayList[String]()
        val pipe: Flow[Int] => Flow[Int] =
          _.map(n => if n == 2 then throw new IllegalStateException("before") else n)
            .onError(thrown => shown.add(s"first: ${thrown.getMessage}"): Unit)
            .recover { case _: IllegalStateException => 50 }
            .onError(thrown => shown.add(s"second: ${thrown.getMessage}"): Unit)
            .map(n => if n == 50 then throw new IllegalStateException("later") else n)
        val flow = if stepped then Flow.from(1 to 5).groupThrough(_ => 0)((_, values) => pipe(values)) else pipe(Flow.from(1 to 5))
        assertEquals(intercept[IllegalStateException](flow.run()).getMessage, "later")
        assertEquals(shown.toString, "[first: before]", s"stepped: $stepped")
      }
      // A key that wants no more is ended; the others go on.
      val dealt = Flow.from(0 until 10).groupThrough(_ & 1)((key, values) => if key == 0 then values.take(2) else values)
      assertEquals(dealt.toList, List(0, 1, 2, 3, 5, 7, 9))
    }
  }

  for strategy <- Server.Strategy.values do {

    test(s"$strategy: a connection's bytes sent to its sink are echoed, and the server ends when closed") {
      val failure = serving(strategy) { connection =>
          connection.bytes.to(connection.sink)
      } { (io, server) =>
        val client = connect(io, server)
        val message = "hello, flows".getBytes("UTF-8")
        send(io, client, message)
        assertEquals(receive(io, client, message.length).toList, message.toList)
        send(io, client, message)
        assertEquals(receive(io, client, message.length).toList, message.toList)
        close(io, client)
      }
      assertServed(failure)
    }

    test(s"$strategy: a server cancelled closes every connection, each handler's finally run, and its listener") {
      running { io ?=>
        if strategy == Server.Strategy.Auto || Server.strategies(io).contains(strategy) then {
          val server = Server.tcp(Loopback, sizes(), strategy)
          val entered = new java.util.concurrent.atomic.AtomicInteger(0)
          val left = new java.util.concurrent.atomic.AtomicInteger(0)
          val served = Future {
            server.serve { connection =>
              entered.incrementAndGet(): Unit
              try connection.bytes.to(connection.sink)
              finally left.incrementAndGet(): Unit
            }
          }
          val clients = List.fill(3)(connect(io, server))
          send(io, clients.head, Array[Byte](7))
          assertEquals(receive(io, clients.head, 1).toList, List[Byte](7))
          while entered.get() < 3 do io.perform(io.ops.timer(2))
          served.cancel()
          served.awaitResult: Unit
          assertEquals(left.get(), 3)
          for client <- clients do assertEquals(receive(io, client, 1).length, 0)
          clients.foreach(close(io, _))
          assert(scala.util.Try(connect(io, server)).isFailure, "the listener is still open")
        }
      }
    }

    test(s"$strategy: a server closed with no connection ends") {
      assertServed(serving(strategy)(_ => ())((_, _) => ()))
    }

    test(s"$strategy: a connection is closed when its handler ends, what it sent written first") {
      val failure = serving(strategy) { connection =>
        connection.sink.send(ByteBuffer.wrap("bye".getBytes("UTF-8")))
      } { (io, server) =>
        val client = connect(io, server)
        // Three bytes, and then the end.
        assertEquals(new String(receive(io, client, 8), "UTF-8"), "bye")
        close(io, client)
      }
      assertServed(failure)
    }

    test(s"$strategy: streams dealt by key answer in their own flows, each send whole and each key's in order") {
      val keys = 4
      val each = 32
      val failure = serving(strategy) { connection =>
          val guard = new Semaphore(1)
        var answered = 0
        // A byte is a frame: its low two bits its stream.
        def frames(bytes: Flow[ByteBuffer]): Flow[Integer] = Flow {
          for chunk <- bytes do while chunk.hasRemaining() do emit(Integer.valueOf(chunk.get() & 0xff))
        }
        connection.bytes.through(frames).groupBy(_.intValue & 3).foreachPar { (key, values) =>
          values.foreach { value =>
            // What the streams share is locked, whatever serves them.
            val held = guard.acquire()
            try answered += 1
            finally held.release()
            connection.sink.send(ByteBuffer.wrap(Array[Byte]('<', key.toByte, value.byteValue, '>')))
          }
        }
        assertEquals(answered, keys * each)
      } { (io, server) =>
        val client = connect(io, server)
        send(io, client, Array.tabulate(keys * each)(_.toByte))
        val answers = receive(io, client, keys * each * 4)
        assertEquals(answers.length, keys * each * 4)
        val seen = Array.fill(keys)(List.empty[Int])
        for at <- 0 until answers.length by 4 do {
          assertEquals(answers(at).toChar, '<')
          assertEquals(answers(at + 3).toChar, '>')
          val key = answers(at + 1).toInt
          val value = answers(at + 2) & 0xff
          assertEquals(value & 3, key)
          seen(key) = value :: seen(key)
        }
        for key <- 0 until keys do assertEquals(seen(key).reverse, (0 until keys * each).filter(_ % keys == key).toList)
        close(io, client)
      }
      assertServed(failure)
    }

    test(s"$strategy: a handler spawns futures and awaits them, others served meanwhile") {
      val failure = serving(strategy) { connection =>
          connection.bytes.foreach { chunk =>
          val bytes = copy(chunk)
          val answer = Async.group(Future(bytes.map(byte => (byte + 1).toByte)).await)
          connection.sink.send(ByteBuffer.wrap(answer))
        }
      } { (io, server) =>
        val clients = List.fill(6)(connect(io, server))
        val asking = clients.zipWithIndex.map { (client, index) =>
          Future {
            for round <- 0 until 8 do {
              send(io, client, Array[Byte](index.toByte, round.toByte))
              assertEquals(receive(io, client, 2).toList, List[Byte]((index + 1).toByte, (round + 1).toByte))
            }
          }
        }
        asking.foreach(_.await)
        clients.foreach(close(io, _))
      }
      assertServed(failure)
    }

    test(s"$strategy: a handler's pipeline of very many stages runs, cut onto stacks of its own") {
      val stages = 1_000
      val failure = serving(strategy) { connection =>
        var numbers: Flow[Int] = Flow[Int] {
          for chunk <- connection.bytes do while chunk.hasRemaining() do emit(chunk.get() & 0xff)
        }
        for _ <- 1 to stages do numbers = numbers.map(_ + 1)
        for number <- numbers do connection.send(ByteBuffer.wrap(Array[Byte](number.toByte)))
      } { (io, server) =>
        val client = connect(io, server)
        val sent = Array.tabulate(48)(_.toByte)
        send(io, client, sent)
        assertEquals(receive(io, client, sent.length).toList, sent.toList.map(byte => (byte + stages).toByte))
        close(io, client)
      }
      assertServed(failure)
    }

    test(s"$strategy: a handler zips, merges and maps in parallel, in flows of its own beside it") {
      val failure = serving(strategy) { connection =>
        val bytes = Flow[Int] {
          for chunk <- connection.bytes do while chunk.hasRemaining() do emit(chunk.get() & 0xff)
        }
        // Each byte with its place, doubled beside the others, and a tick merged in for each.
        val answers = bytes
          .zip(Flow.iterate(0)(_ + 1))
          .mapPar(3)((value, index) => (index, value * 2))
          .merge(Flow.of((-1, 0)))
        for (index, value) <- answers do connection.send(ByteBuffer.wrap(Array[Byte](index.toByte, value.toByte)))
      } { (io, server) =>
        val client = connect(io, server)
        val sent = Array.tabulate(20)(i => (i + 1).toByte)
        send(io, client, sent)
        // The tick, and a pair for each byte, in no order.
        val got = receive(io, client, 2 * (sent.length + 1)).grouped(2).map(pair => (pair(0).toInt, pair(1).toInt)).toSet
        assertEquals(got, (0 until 20).map(i => (i, (i + 1) * 2)).toSet + ((-1, 0)))
        close(io, client)
      }
      assertServed(failure)
    }

    test(s"$strategy: more is sent than the sink holds, and all of it arrives in order") {
      val total = 64 * 1024
      val failure = serving(strategy, sizes(read = 512, write = 1024)) { connection =>
        val chunk = new Array[Byte](700)
        var sent = 0
        while sent < total do {
          val size = math.min(chunk.length, total - sent)
          for at <- 0 until size do chunk(at) = ((sent + at) % 251).toByte
          connection.sink.send(ByteBuffer.wrap(chunk, 0, size))
          sent += size
        }
        connection.sink.flush()
      } { (io, server) =>
        val client = connect(io, server)
        val got = receive(io, client, total)
        assertEquals(got.length, total)
        for at <- 0 until total do if got(at) != (at % 251).toByte then fail(s"byte $at is ${got(at)}")
        close(io, client)
      }
      assertServed(failure)
    }

    test(s"$strategy: a handler's failure closes its connection, leaves the others, and is thrown by serve") {
      val failure = serving(strategy) { connection =>
          connection.bytes.foreach { chunk =>
          if chunk.get(chunk.position()) == 'X' then throw new IllegalStateException("handler failed")
          connection.sink.send(chunk)
        }
      } { (io, server) =>
        val failing = connect(io, server)
        val served = connect(io, server)
        send(io, failing, Array[Byte]('X'))
        // Its connection is closed: nothing more comes.
        assertEquals(receive(io, failing, 1).length, 0)
        send(io, served, Array[Byte]('a', 'b'))
        assertEquals(receive(io, served, 2).toList, List[Byte]('a', 'b'))
        close(io, failing)
        close(io, served)
      }
      if failure ne Refused then {
        assert(failure.isInstanceOf[IllegalStateException], s"serve ended with $failure")
        assertEquals(failure.nn.getMessage, "handler failed")
      }
    }
  }
}

/** What a backend with shards must do: an op performed on a shard's view is the op, whichever shard. */
trait ShardsContract extends ContractSuite with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams & Shards }

  test("ops performed on each shard's view echo as on the Io itself") {
    running { io ?=>
      import Shards.*
      assert(io.shards >= 1)
      val listener = opening[io.ops.Listener](io)(slot => io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), slot))
      val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
      try
        for shard <- 0 until io.shards do {
          val on = io.onShard(shard)
          val (client, server) = pair(io, listener)(opening(io)(slot => io.ops.connect(address, slot)))
          try {
            val out = bytes(s"shard $shard")
            val write = on.ops.write(client, out)
            while out.hasRemaining() do on.perform(write)
            val in = allocate(out.limit())
            // The read waits: the echo's other half is written only once it does.
            val reading = Future {
              val read = on.ops.read(server, in)
              while in.hasRemaining() do on.perform(read)
            }
            reading.await
            assertEquals(text(in), s"shard $shard")
            // And one that has to wait on the shard.
            val later = allocate(4)
            val waiting = Future(on.perform(on.ops.read(client, later)))
            settle(io)
            val reply = bytes("pong")
            val answer = on.ops.write(server, reply)
            while reply.hasRemaining() do on.perform(answer)
            waiting.await
            assert(later.position() > 0)
          } finally {
            close(io, client)
            close(io, server)
          }
        }
      finally close(io, listener)
    }
  }

  test("a shard there is not is refused") {
    running { io ?=>
      import Shards.*
      intercept[IllegalArgumentException](io.onShard(io.shards)): Unit
      intercept[IllegalArgumentException](io.onShard(-1)): Unit
    }
  }
}

/** What a backend with submission queues must do. */
trait SubmissionQueueContract extends ContractSuite with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams & SubmissionQueues }

  private def paired[T](body: (Io[backend.Interface], AnyRef, AnyRef) => Async.Spawn ?=> T): T =
    running { io ?=>
      val listener = opening[io.ops.Listener](io)(slot => io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), slot))
      val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
      val (client, server) = pair(io, listener)(opening(io)(slot => io.ops.connect(address, slot)))
      try body(io, client, server)
      finally {
        close(io, client)
        close(io, server)
        close(io, listener)
      }
    }

  test("ops submitted are performed and come back completed, each with its tag") {
    paired { (io, c, s) =>
      import SubmissionQueues.*
      val client = c.asInstanceOf[io.ops.Connection]
      val server = s.asInstanceOf[io.ops.Connection]
      val out = bytes("queued")
      val in = allocate(6)
      val ops = new SubmissionQueue[io.Op]
      val completed = new CompletionQueue[io.Op]
      ops.submit(io.ops.write(client, out), "write")
      ops.submit(io.ops.read(server, in), "read")
      assertEquals(ops.submitted, 2)
      var tags = Set.empty[AnyRef | Null]
      while tags.size < 2 do {
        io.performQueue(ops, completed)
        for i <- 0 until completed.count do {
          assertEquals(completed.failure(i), null)
          tags += completed.tag(i)
        }
        completed.clear()
      }
      assertEquals(tags, Set[AnyRef | Null]("write", "read"))
      assert(ops.isEmpty)
      assert(in.position() > 0)
    }
  }

  test("an op that has to wait stays in flight, and is completed by a later call") {
    paired { (io, c, s) =>
      import SubmissionQueues.*
      val client = c.asInstanceOf[io.ops.Connection]
      val server = s.asInstanceOf[io.ops.Connection]
      val in = allocate(4)
      val ops = new SubmissionQueue[io.Op]
      val completed = new CompletionQueue[io.Op]
      ops.submit(io.ops.read(server, in), "read")
      // A timer among the ops is what bounds the wait.
      ops.submit(io.ops.timer(20), "timer")
      io.performQueue(ops, completed)
      assertEquals(completed.count, 1)
      assertEquals(completed.tag(0), "timer")
      assertEquals(ops.inFlight, 1)
      completed.clear()
      writeAll(io, client, bytes("late"))
      io.performQueue(ops, completed)
      assertEquals(completed.count, 1)
      assertEquals(completed.tag(0), "read")
      assertEquals(completed.failure(0), null)
      assertEquals(ops.inFlight, 0)
    }
  }

  test("an op's time limit is its completion's failure, not thrown") {
    paired { (io, _, s) =>
      import SubmissionQueues.*
      import scala.concurrent.duration.*
      val server = s.asInstanceOf[io.ops.Connection]
      val ops = new SubmissionQueue[io.Op]
      val completed = new CompletionQueue[io.Op]
      ops.submit(io.ops.read(server, allocate(4)), "read", 20.millis)
      io.performQueue(ops, completed)
      assertEquals(completed.count, 1)
      assert(completed.failure(0).isInstanceOf[java.util.concurrent.TimeoutException], s"${completed.failure(0)}")
    }
  }

  test("a performQueue with nothing submitted and nothing in flight returns") {
    running { io ?=>
      import SubmissionQueues.*
      val ops = new SubmissionQueue[io.Op]
      val completed = new CompletionQueue[io.Op]
      io.performQueue(ops, completed)
      assertEquals(completed.count, 0)
    }
  }

  test("a caller cancelled as it waits has its ops in flight cancelled, and they can be performed again") {
    paired { (io, c, s) =>
      import SubmissionQueues.*
      val client = c.asInstanceOf[io.ops.Connection]
      val server = s.asInstanceOf[io.ops.Connection]
      val in = allocate(4)
      val read = io.ops.read(server, in)
      val waiting = Future {
        val ops = new SubmissionQueue[io.Op]
        val completed = new CompletionQueue[io.Op]
        ops.submit(read)
        io.performQueue(ops, completed)
      }
      settle(io)
      waiting.cancel()
      assert(failureOf(waiting).isInstanceOf[java.util.concurrent.CancellationException])
      writeAll(io, client, bytes("more"))
      io.perform(read)
      assert(in.position() > 0)
    }
  }
}
