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

  test("a gears stage in the middle of an Ox flow") {
    // No gears `Async` in sight: the stage is given one where Ox runs it.
    val evens: Flow[Int] => Flow[String] = _.filter(_ % 2 == 0).mapStateful(0)((sum, n) => (sum + n, s"$n:${sum + n}"))
    assertEquals(
      OxFlow.fromIterable(1 to 8).map(_ + 1).throughGears(evens).map(_.toUpperCase).runToList(),
      List("2:2", "4:6", "6:12", "8:20")
    )
    // With Ox's forks on either side of it, and gears' own beside it within.
    val doubled = OxFlow
      .fromIterable(1 to 40)
      .mapPar(4)(_ + 1)
      .throughGears(_.mapPar(3)(_ * 2).groupThrough(_ % 4)((key, values) => values.map(n => (key, n))))
      .mapPar(4)(identity)
      .runToList()
    assertEquals(doubled.map(_._2).sorted, (1 to 40).map(n => (n + 1) * 2).toList)
    assert(doubled.forall((key, n) => n % 4 == key))
    // Left early from the Ox side.
    assertEquals(OxFlow.iterate(1)(_ + 1).throughGears(_.map(_ * 3)).take(3).runToList(), List(3, 6, 9))
  }

  test("an Ox stage in the middle of a gears flow") {
    Async.blocking {
      val windows: OxFlow[Int] => OxFlow[Int] = _.sliding(2).map(_.sum)
      assertEquals(Flow.from(1 to 5).map(_ * 10).throughOx(windows).filter(_ > 30).toList, List(50, 70, 90))
      // An Ox stage that runs what is above it on threads of its own: the gears flow is run there.
      val par = Flow.from(1 to 30).filter(_ % 3 != 0).throughOx(_.mapPar(4)(_ + 100).buffer()(using _root_.ox.channels.BufferCapacity(4)))
      assertEquals(par.toList, (1 to 30).filter(_ % 3 != 0).map(_ + 100).toList)
      // Left early from the gears side, and stages of each kind one inside another.
      assertEquals(Flow.iterate(1)(_ + 1).throughOx(_.map(_ * 2)).take(4).toList, List(2, 4, 6, 8))
      val nested = Flow.from(1 to 6).throughOx(_.throughGears(_.filter(_ % 2 == 0)).map(_ + 1)).toList
      assertEquals(nested, List(3, 5, 7))
    }
  }

  test("nothing of either runs until the whole is, and a stage's failure is the whole's") {
    Async.blocking {
      var ran = 0
      val counted = Flow[Int] { ran += 1; Flow.emit(1) }
      val whole = counted.throughOx(_.map(_ + 1))
      assertEquals(ran, 0)
      assertEquals(whole.toList, List(2))
      assertEquals(ran, 1)
      val failing = Flow.from(1 to 3).throughOx(_.map(n => if n == 2 then throw new IllegalStateException("ox stage") else n))
      assertEquals(intercept[IllegalStateException](failing.run()).getMessage, "ox stage")
    }
    val failing = OxFlow.fromValues(1, 2, 3).throughGears(_.map(n => if n == 2 then throw new IllegalStateException("gears stage") else n))
    assertEquals(intercept[IllegalStateException](failing.runDrain()).getMessage, "gears stage")
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
