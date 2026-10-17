package gearsexample

import gears.async.Async
import gears.async.Future
import gears.async.Future.awaitAll

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import scala.scalanative.unsafe.Zone

import asyncio.Address
import asyncio.Completion
import asyncio.HandleSlot
import asyncio.Reactor
import asyncio.unsafe.NativeBuffer
import ReactorFutures.*

/** Stress tests for a reactor and its gears integration, to measure each change against. An echo server runs on one
  * reactor thread and the clients on another, so both sides go through the reactor; each scenario is timed on its own
  * and stresses a different cost (see `asyncio/kqueue/TODO.md`). Every line it prints for a scenario starts with
  * `bench:` so that `kqueue-demo/bench/compare.py` can collect the results.
  */
object GearsBench {

  /** The size of each message a client sends and the server echoes back. */
  inline val MessageSize = 64

  /** A scenario: its name, what it stresses, how many operations it performs at scale 1, and the client code that
    * performs `count` of them against the echo server at `address`.
    */
  final case class Scenario(name: String, stresses: String, baseCount: Int, body: Client)

  trait Client {
    def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit
  }

  val scenarios: Seq[Scenario] = Seq(
    Scenario(
      "pingpong",
      "one message in flight: a resume and a readiness wait per message on each side",
      50_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = false)
      }
    ),
    Scenario(
      "pingpong-fresh-ops",
      "as pingpong, but building the write and read ops anew for every message, as code that does not keep them does",
      50_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = false, freshOps = true)
      }
    ),
    Scenario(
      "pingpong-deadline",
      "as pingpong, also arming and cancelling a reactor timer per round trip, like a timeout",
      50_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = true)
      }
    ),
    Scenario(
      "concurrent",
      "many connections ping-ponging at once: many events and registrations per turn",
      100_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(r, address, each, deadline = false))).awaitAll
        }
      }
    ),
    Scenario(
      "connections",
      "a new connection for each round trip: connect, one message and its echo, close; the server accepts each",
      20_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit = {
          var i = 0
          while i < count do {
            pingPong(r, address, 1, deadline = false)
            i += 1
          }
        }
      }
    ),
    Scenario(
      "pipelined",
      "one connection, each message its own write while replies are read at the same time",
      200_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          pipelined(r, address, count)
      }
    ),
    Scenario(
      "blocking",
      "trivial blocking tasks one after another: a wake-up from a worker thread for each",
      100_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit = {
          var i = 0
          while i < count do {
            perform(r, r.ops.blocking(() => ()))
            i += 1
          }
        }
      }
    ),
    Scenario(
      "timers",
      s"timers armed $TimerBatch at a time, half firing at once and half cancelled: the cost of many pending timers",
      200_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          timerStress(r, count)
      }
    ),
    Scenario(
      "fairness",
      "pipelined traffic while a ticker on the same thread performs zero-length timers; also reports fairness-tick, "
        + "the time between ticks, which rises if busy futures stop yielding",
      200_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          fairness(r, address, count)
      }
    ),
    Scenario(
      "mixed",
      "pingpong while a compute future on the same thread works in slices, yielding between them; also reports "
        + "mixed-compute, the time per slice meanwhile",
      50_000,
      new Client {
        def apply(r: Reactor, address: Address, count: Int, connections: Int)(using Async.Spawn): Unit =
          mixed(r, address, count)
      }
    )
  )

  /** How many timers the `timers` scenario has pending at once. */
  inline val TimerBatch = 1000

  /** Arms timers `TimerBatch` at a time: half for 0 ms, which fire, and half for a minute, which are cancelled once the
    * others have fired. Each timer, armed and then fired or cancelled, is one operation.
    */
  private def timerStress(r: Reactor, count: Int)(using Async): Unit = {
    var done = 0
    while done < count do {
      val batch = math.min(TimerBatch, count - done)
      var fired = 0
      val counter: Completion[r.Op] = _ => fired += 1
      val toCancel = scala.collection.mutable.ArrayBuffer.empty[r.Op]
      var i = 0
      while i < batch do {
        if i % 2 == 0 then r.submit(r.ops.timer(0), counter)
        else {
          val timer = r.ops.timer(60_000)
          r.submit(timer, counter)
          toCancel += timer
        }
        i += 1
      }
      val firing = (batch + 1) / 2
      while fired < firing do perform(r, r.ops.timer(0)) // lets the loop turn until the short timers have fired
      toCancel.foreach(r.cancel)
      done += batch
    }
  }

  /** Prints one result line, in the format `kqueue-demo/bench/compare.py` reads. */
  def report(name: String, count: Long, seconds: Double): Unit =
    println(f"bench: $name%-18s $count%10d $seconds%9.3f ${count / seconds}%12.0f ${seconds * 1e6 / count}%8.2f")

  // Where compute slices leave their results, so that the work cannot be optimised away.
  @volatile private var computeSink = 0L

  /** About ten thousand steps of arithmetic: a slice of CPU-bound work. */
  private def computeSlice(seed: Long): Long = {
    var x = seed | 1L
    var i = 0
    while i < 10_000 do {
      x ^= x << 13
      x ^= x >>> 7
      x ^= x << 17
      i += 1
    }
    x
  }

  /** Ping-pongs `count` messages while a compute future on the same reactor thread runs slices of work until they are
    * done. Between slices it yields to the scheduler, awaiting each slice as its own future, so that the ping-pong
    * continuations run in between. Reports the compute side as `mixed-compute`: the time per slice over the same
    * period.
    */
  private def mixed(r: Reactor, address: Address, count: Int)(using Async.Spawn): Unit = {
    var pingPongDone = false
    var slices = 0L
    val start = System.nanoTime()
    val compute = Future {
      var result = 0L
      while !pingPongDone do {
        result += Future(computeSlice(slices)).await
        slices += 1
      }
      computeSink = result
    }
    pingPong(r, address, count, deadline = false)
    pingPongDone = true
    val seconds = (System.nanoTime() - start) / 1e9
    compute.await
    report("mixed-compute", slices, seconds)
  }

  /** Connects to `address`, runs `body` on the connection, and closes it. */
  private def withConnection(r: Reactor, address: Address)(body: Int => Unit)(using Async): Unit = {
    val fd = r.handles.connect(address)
    try {
      perform(r, r.ops.connect(fd))
      body(fd)
    } finally r.handles.close(fd)
  }

  /** Sends `count` messages one at a time, waiting for each echo before sending the next. With `deadline`, each round
    * trip also arms a reactor timer that is cancelled once the echo is back.
    */
  private def pingPong(r: Reactor, address: Address, count: Int, deadline: Boolean, freshOps: Boolean = false)(using
      Async
  ): Unit =
    withConnection(r, address) { fd =>
      val zone = Zone.open()
      try {
        val buf = NativeBuffer.allocate(MessageSize)(using zone)
        val write = r.ops.write(fd, buf)
        val read = r.ops.read(fd, buf)
        val timer = r.ops.timer(60_000)
        var i = 0
        while i < count do {
          if deadline then r.submit(timer, _ => throw new IOException("A round trip timed out"))
          buf.clear()
          while buf.hasRemaining() do perform(r, if freshOps then r.ops.write(fd, buf) else write)
          buf.clear()
          while buf.hasRemaining() do {
            val before = buf.position()
            perform(r, if freshOps then r.ops.read(fd, buf) else read)
            if buf.position() == before then throw new IOException("The server closed the connection")
          }
          if deadline then r.cancel(timer)
          i += 1
        }
      } finally zone.close()
    }

  /** Writes `count` messages, each as its own write, from one future while reading the echoes in another. */
  private def pipelined(r: Reactor, address: Address, count: Int)(using Async.Spawn): Unit =
    withConnection(r, address) { fd =>
      val zone = Zone.open()
      try {
        val total = count.toLong * MessageSize
        val out = NativeBuffer.allocate(MessageSize)(using zone)
        val in = NativeBuffer.allocate(8192)(using zone)
        val writer = Future {
          val write = r.ops.write(fd, out)
          var i = 0
          while i < count do {
            out.clear()
            while out.hasRemaining() do perform(r, write)
            i += 1
          }
        }
        val read = r.ops.read(fd, in)
        var received = 0L
        while received < total do {
          in.clear()
          perform(r, read)
          if in.position() == 0 then throw new IOException("The server closed the connection")
          received += in.position()
        }
        writer.await
      } finally zone.close()
    }

  /** Runs `pipelined` while a ticker future on the same reactor thread performs zero-length timers one after another,
    * and reports the ticker as `fairness-tick`: the time between ticks over the same period. Futures whose operations
    * rarely wait must still let it run.
    */
  private def fairness(r: Reactor, address: Address, count: Int)(using Async.Spawn): Unit = {
    var trafficDone = false
    var ticks = 0L
    val start = System.nanoTime()
    val ticker = Future {
      while !trafficDone do {
        perform(r, r.ops.timer(0))
        ticks += 1
      }
    }
    pipelined(r, address, count)
    trafficDone = true
    val seconds = (System.nanoTime() - start) / 1e9
    ticker.await
    report("fairness-tick", math.max(ticks, 1L), seconds)
  }

  /** Echoes everything a connection sends until it closes. */
  private def echo(r: Reactor, client: Int)(using Async): Unit = {
    val zone = Zone.open()
    try {
      val buf = NativeBuffer.allocate(8192)(using zone)
      val read = r.ops.read(client, buf)
      val write = r.ops.write(client, buf)
      var open = true
      while open do {
        buf.clear()
        perform(r, read)
        if buf.position() == 0 then open = false // end of stream
        else {
          buf.flip()
          while buf.hasRemaining() do perform(r, write)
        }
      }
    } catch case _: IOException => () // a client that goes away mid-write ends only its own connection
    finally {
      zone.close()
      r.handles.close(client)
    }
  }

  /** Starts the echo server on its own reactor thread and returns once it is listening. */
  private def startServer(address: Address)(using Reactor.Factory[Reactor]): Unit = {
    val listening = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable | Null](null)
    val thread = new Thread(() => serve(address, listening, failure), "bench-server")
    thread.setDaemon(true) // it serves until the process exits
    thread.start()
    listening.await()
    val failed = failure.get()
    if failed != null then throw failed
  }

  /** Listens on `address`, counts down `listening`, then echoes every connection until the process exits. A failure to
    * listen is recorded in `failure`.
    */
  private def serve(address: Address, listening: CountDownLatch, failure: AtomicReference[Throwable | Null])(using
      Reactor.Factory[Reactor]
  ): Unit =
    try
      ReactorFutures.run { r =>
        val server =
          try r.handles.listen(address)
          finally listening.countDown()
        val accepted = HandleSlot()
        val accept = r.ops.accept(server, accepted)
        while true do {
          perform(r, accept)
          val client = accepted.clear()
          Future(echo(r, client))
        }
      }
    catch {
      case t: Throwable =>
        failure.set(t)
        listening.countDown()
    }

  /** Runs the named scenarios, or all of them, each with its base count times `scale`. */
  def run(
      names: Seq[String],
      scale: Double,
      connections: Int,
      address: Address
  )(using Reactor.Factory[Reactor]): Unit = {
    val selected =
      if names.isEmpty then scenarios
      else
        names.map(name =>
          scenarios
            .find(_.name == name)
            .getOrElse(
              throw new IllegalArgumentException(
                s"Unknown scenario `$name`; try ${scenarios.map(_.name).mkString(", ")}"
              )
            )
        )
    address match {
      case Address.Unix(path) => Files.deleteIfExists(Paths.get(path))
      case _                  => ()
    }
    try {
      startServer(address)
      ReactorFutures.run { r =>
        println(f"bench: ${"scenario"}%-18s ${"ops"}%10s ${"seconds"}%9s ${"ops/s"}%12s ${"us/op"}%8s")
        for scenario <- selected do {
          val count = math.max(1, (scenario.baseCount * scale).toInt)
          val start = System.nanoTime()
          scenario.body(r, address, count, connections)
          val seconds = (System.nanoTime() - start) / 1e9
          report(scenario.name, count, seconds)
        }
      }
    } finally
      address match {
        case Address.Unix(path) => Files.deleteIfExists(Paths.get(path))
        case _                  => ()
      }
  }
}
