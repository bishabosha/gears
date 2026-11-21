package gears.async.io.oxflow

import gears.async.Async
import gears.async.default.given
import gears.async.io.Flow

import _root_.ox.flow.Flow as OxFlow

class OxFlowSuite extends munit.FunSuite {

  test("an Ox flow is a gears flow: run where the gears flow is, through gears' stages") {
    Async.blocking {
      val numbers = OxFlow.fromValues(1, 2, 3, 4, 5, 6).filter(_ % 2 == 0)
      assertEquals(numbers.toGears.map(_ * 10).toList, List(20, 40, 60))
      // And Ox's own concurrency inside it.
      assertEquals(OxFlow.fromIterable(1 to 20).mapPar(4)(_ + 1).toGears.fold(0)(_ + _), (2 to 21).sum)
    }
  }

  test("a gears flow is an Ox flow: run where the Ox flow is, through Ox's stages") {
    Async.blocking {
      val numbers = Flow.from(1 to 6).filter(_ % 2 == 1)
      assertEquals(numbers.toOx.map(_ * 10).runToList(), List(10, 30, 50))
      assertEquals(numbers.toOx.take(2).runToList(), List(1, 3))
    }
  }

  test("a gears flow run by Ox on threads of its own") {
    val numbers = Flow.from(1 to 50).map(_ * 2)
    assertEquals(numbers.toOxOnAnyThread.mapPar(4)(_ + 1).runToList(), (1 to 50).map(_ * 2 + 1).toList)
    assertEquals(numbers.toOxOnAnyThread.buffer()(using _root_.ox.channels.BufferCapacity(8)).runFold(0)(_ + _), (1 to 50).map(_ * 2).sum)
  }

  test("there and back again, and a generator of each kind inside the other") {
    Async.blocking {
      val ours = Flow[Int] { for i <- 1 to 3 do Flow.emit(i) }
      val theirs = OxFlow.usingEmit[String] { emit => for value <- List("a", "b") do emit(value) }
      assertEquals(ours.toOx.toGears.toOx.runToList(), List(1, 2, 3))
      val pairs = for { n <- ours; s <- theirs.toGears } yield s"$n$s"
      assertEquals(pairs.toList, List("1a", "1b", "2a", "2b", "3a", "3b"))
    }
  }

  test("the same pipeline under the names the two share") {
    Async.blocking {
      val theirs = OxFlow
        .fromValues(1, 2, 3, 4, 5, 6, 7)
        .filter(_ > 1)
        .mapStateful(0)((sum, n) => (sum + n, sum + n))
        .mapConcat(n => List(n, -n))
        .intersperse(0)
        .take(7)
        .runToList()
      val ours = Flow
        .fromValues(1, 2, 3, 4, 5, 6, 7)
        .filter(_ > 1)
        .mapStateful(0)((sum, n) => (sum + n, sum + n))
        .mapConcat(n => List(n, -n))
        .intersperse(0)
        .take(7)
        .runToList()
      assertEquals(ours, theirs)
      assertEquals(
        Flow.fromIterable(1 to 12).groupBy(4, _ % 3)(key => _.map(n => s"$key:$n")).runToList().sorted,
        OxFlow.fromIterable(1 to 12).groupBy(4, _ % 3)(key => _.map(n => s"$key:$n")).runToList().sorted
      )
      assertEquals(Flow.fromIterable(1 to 9).mapPar(3)(_ * 2).runToList(), OxFlow.fromIterable(1 to 9).mapPar(3)(_ * 2).runToList())
    }
  }

  test("a failure in either is the other's") {
    Async.blocking {
      val failing = OxFlow.usingEmit[Int] { emit => emit(1); throw new IllegalStateException("ox failed") }
      assertEquals(intercept[IllegalStateException](failing.toGears.run()).getMessage, "ox failed")
      val ours = Flow[Int] { Flow.emit(1); throw new IllegalStateException("gears failed") }
      assertEquals(intercept[IllegalStateException](ours.toOx.runDrain()).getMessage, "gears failed")
    }
  }
}
