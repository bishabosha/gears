package gearsexample

import gears.async.Async
import gears.async.Future
import gears.async.Future.awaitAll

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import asyncio.Backend
import asyncio.BlockingBackend
import asyncio.ReactorBackend
import asyncio.UnixAddress
import asyncio.UnixStreamBackend
import asyncio.Completion
import asyncio.HandleSlot
import asyncio.testkit.BufferArena
import asyncio.gears.ReactorFutures
import asyncio.gears.ReactorFutures.*
import asyncio.gears.TakePolicy

/** Stress tests for a reactor and its gears integration, to measure each change against. An echo server runs on one
  * reactor thread and the clients on another, so both sides go through the reactor; each scenario is timed on its own
  * and stresses a different cost (see `asyncio/kqueue/TODO.md`). Every line it prints for a scenario starts with
  * `bench:` so that `kqueue-demo/bench/compare.py` can collect the results.
  */
object GearsBench {

  /** The size of each message a client sends and the server echoes back. */
  inline val MessageSize = 64

  /** A scenario: its name, what it stresses, how many operations it performs at scale 1, and the client code that
    * performs `count` of them against the echo server at `address`; and whether a backend has what it needs, beyond
    * Unix stream sockets. A backend without it skips the scenario.
    */
  final case class Scenario(
      name: String,
      stresses: String,
      baseCount: Int,
      body: Client,
      runsOn: Backend => Boolean = _ => true
  )

  trait Client {
    def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit
  }

  val scenarios: Seq[Scenario] = Seq(
    Scenario(
      "pingpong",
      "one message in flight: a resume and a readiness wait per message on each side",
      50_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = false)
      }
    ),
    Scenario(
      "pingpong-fresh-ops",
      "as pingpong, but building the write and read ops anew for every message, as code that does not keep them does",
      50_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = false, freshOps = true)
      }
    ),
    Scenario(
      "pingpong-deadline",
      "as pingpong, also arming and cancelling a reactor timer per round trip, like a timeout",
      50_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = true)
      },
      _.isInstanceOf[ReactorBackend]
    ),
    Scenario(
      "concurrent",
      "many connections ping-ponging at once: many events and registrations per turn",
      100_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(r, address, each, deadline = false))).awaitAll
        }
      }
    ),
    Scenario(
      "concurrent-compute",
      "as concurrent, with a slice of computation after each round trip, in the connection's own future",
      100_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(r, address, each, deadline = false, work = Work.Inline))).awaitAll
        }
      }
    ),
    Scenario(
      "concurrent-offload",
      "as concurrent-compute, with each slice in a future of its own, awaited before the next round trip",
      100_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(r, address, each, deadline = false, work = Work.Offload))).awaitAll
        }
      }
    ),
    Scenario(
      "connections",
      "a new connection for each round trip: connect, one message and its echo, close; the server accepts each",
      20_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit = {
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
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          pipelined(r, address, count)
      }
    ),
    Scenario(
      "blocking",
      "trivial blocking tasks one after another: a wake-up from a worker thread for each",
      100_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit = {
          val b = blocking(r)
          var i = 0
          while i < count do {
            perform(b, b.ops.blocking(() => ()))
            i += 1
          }
        }
      },
      _.isInstanceOf[BlockingBackend]
    ),
    Scenario(
      "blocking-io",
      "a trivial blocking task and a round trip in turn on one connection: moving between blocking work and I/O",
      50_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          pingPong(r, address, count, deadline = false, blockingFirst = true)
      },
      _.isInstanceOf[BlockingBackend]
    ),
    Scenario(
      "timers",
      s"timers armed $TimerBatch at a time, half firing at once and half cancelled: the cost of many pending timers",
      200_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          timerStress(reactors(r), count)
      },
      _.isInstanceOf[ReactorBackend]
    ),
    Scenario(
      "fairness",
      "pipelined traffic while a ticker on the same thread performs zero-length timers; also reports fairness-tick, "
        + "the time between ticks, which rises if busy futures stop yielding",
      200_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          fairness(r, address, count)
      }
    ),
    Scenario(
      "mixed",
      "pingpong while a compute future on the same thread works in slices, yielding between them; also reports "
        + "mixed-compute, the time per slice meanwhile",
      50_000,
      new Client {
        def apply(r: UnixStreamBackend, address: StreamAddress, count: Int, connections: Int)(using Async.Spawn): Unit =
          mixed(r, address, count)
      }
    )
  )

  /** `r` as the reactor backend a scenario that runs only on one needs. */
  private def reactors(r: Backend): ReactorBackend = r match {
    case reactors: ReactorBackend => reactors
    case _                        => throw new IllegalArgumentException(s"$r has no reactors")
  }

  /** `r` as the backend with blocking ops a scenario that runs only on one needs. */
  private def blocking(r: Backend): BlockingBackend = r match {
    case blocking: BlockingBackend => blocking
    case _                         => throw new IllegalArgumentException(s"$r has no blocking ops")
  }

  /** How many timers the `timers` scenario has pending at once. */
  inline val TimerBatch = 1000

  /** Arms timers `TimerBatch` at a time: half for 0 ms, which fire, and half for a minute, which are cancelled once the
    * others have fired. Each timer, armed and then fired or cancelled, is one operation.
    */
  private def timerStress(r: ReactorBackend, count: Int)(using Async): Unit = {
    var done = 0
    while done < count do {
      val batch = math.min(TimerBatch, count - done)
      val fired = new java.util.concurrent.atomic.AtomicInteger // counted on the worker the timers went to
      val counter: Completion[r.Op] = _ => fired.incrementAndGet()
      val toCancel = scala.collection.mutable.ArrayBuffer.empty[Submission]
      var i = 0
      while i < batch do {
        if i % 2 == 0 then submit(r, r.ops.timer(0), counter)
        else toCancel += submit(r, r.ops.timer(60_000), counter)
        i += 1
      }
      val firing = (batch + 1) / 2
      while fired.get() < firing do perform(r, r.ops.timer(0)) // lets the loops turn until the short timers fire
      toCancel.foreach(_.cancel())
      done += batch
    }
  }

  /** Prints one result line, in the format `kqueue-demo/bench/compare.py` reads. */
  def report(name: String, count: Long, seconds: Double): Unit =
    println(f"bench: $name%-18s $count%10d $seconds%9.3f ${count / seconds}%12.0f ${seconds * 1e6 / count}%8.2f")

  // Where compute slices leave their results, so that the work cannot be optimised away.
  @volatile private var computeSink = 0L

  /** About ten thousand steps of arithmetic, by default: a slice of CPU-bound work. */
  private def computeSlice(seed: Long, steps: Int = 10_000): Long = {
    var x = seed | 1L
    var i = 0
    while i < steps do {
      x ^= x << 13
      x ^= x >>> 7
      x ^= x << 17
      i += 1
    }
    x
  }

  /** Ping-pongs `count` messages while a compute future on the same reactor thread runs slices of work until they are
    * done. Between slices it yields to the scheduler with `yieldNow`, so that the ping-pong continuations run in
    * between. Reports the compute side as `mixed-compute`: the time per slice over the same period.
    */
  private def mixed(r: UnixStreamBackend, address: StreamAddress, count: Int)(using Async.Spawn): Unit = {
    val pingPongDone = new java.util.concurrent.atomic.AtomicBoolean // read by the other future, maybe on another worker
    var slices = 0L
    val start = System.nanoTime()
    val compute = Future {
      var result = 0L
      while !pingPongDone.get() do {
        result += computeSlice(slices)
        yieldNow()
        slices += 1
      }
      computeSink = result
    }
    pingPong(r, address, count, deadline = false)
    pingPongDone.set(true)
    val seconds = (System.nanoTime() - start) / 1e9
    compute.await
    report("mixed-compute", slices, seconds)
  }

  /** Connects to `address`, runs `body` on the connection, and closes it. */  /** Connects to `address`, runs `body` on the connection, and closes it. */
  private def withConnection(r: UnixStreamBackend, address: StreamAddress)(body: Int => Unit)(using Async): Unit = {
    val fd = Sockets.connect(r, address)
    try {
      perform(r, r.ops.connect(fd))
      body(fd)
    } finally r.handles.close(fd)
  }

  /** Computation done after each round trip by the compute variants of `concurrent`. */
  enum Work { case NoWork, Inline, Offload }

  /** Steps of `computeSlice` done after each round trip by the compute variants of `concurrent`: a few microseconds. */
  inline val WorkSteps = 2_000

  /** Sends `count` messages one at a time, waiting for each echo before sending the next. With `deadline`, each round
    * trip also arms a reactor timer that is cancelled once the echo is back; with `blockingFirst`, a trivial blocking
    * task runs before each round trip; `work` says whether a slice of computation follows each, and where.
    */
  private def pingPong(
      r: UnixStreamBackend,
      address: StreamAddress,
      count: Int,
      deadline: Boolean,
      freshOps: Boolean = false,
      blockingFirst: Boolean = false,
      work: Work = Work.NoWork
  )(using Async.Spawn): Unit =
    withConnection(r, address) { fd =>
      val arena = BufferArena.open()
      try {
        val buf = arena.allocate(MessageSize)
        val write = r.ops.write(fd, buf)
        val read = r.ops.read(fd, buf)
        val timeout = if deadline then new Deadline(reactors(r)) else null
        val blocker = if blockingFirst then blocking(r) else null
        var worked = 0L
        var i = 0
        while i < count do {
          if blocker != null then perform(blocker, blocker.ops.blocking(() => ()))
          if timeout != null then timeout.arm()
          buf.clear()
          while buf.hasRemaining() do perform(r, if freshOps then r.ops.write(fd, buf) else write)
          buf.clear()
          while buf.hasRemaining() do {
            val before = buf.position()
            perform(r, if freshOps then r.ops.read(fd, buf) else read)
            if buf.position() == before then throw new IOException("The server closed the connection")
          }
          if timeout != null then timeout.disarm()
          work match {
            case Work.NoWork  => ()
            case Work.Inline  => worked += computeSlice(i, WorkSteps)
            case Work.Offload => worked += Future(computeSlice(i, WorkSteps)).await
          }
          i += 1
        }
        if worked == 1L then computeSink = worked // only so that the work cannot be optimised away
      } finally arena.close()
    }

  /** A reactor timer armed before each round trip and cancelled once the echo is back, like a timeout. */
  private final class Deadline(r: ReactorBackend)(using Async) {
    private var timer = r.ops.timer(60_000)
    private var pending: Submission | Null = null

    def arm(): Unit = pending = submit(r, timer, _ => throw new IOException("A round trip timed out"))

    /** Cancelled at once if this task is still on the worker the timer went to; otherwise the cancel runs there soon,
      * and the timer cannot be submitted again before it has, so the next round trip gets a new one.
      */
    def disarm(): Unit = if !pending.nn.cancel() then timer = r.ops.timer(60_000)
  }

  /** Writes `count` messages, each as its own write, from one future while reading the echoes in another. */
  private def pipelined(r: UnixStreamBackend, address: StreamAddress, count: Int)(using Async.Spawn): Unit =
    withConnection(r, address) { fd =>
      val arena = BufferArena.open()
      try {
        val total = count.toLong * MessageSize
        val out = arena.allocate(MessageSize)
        val in = arena.allocate(8192)
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
      } finally arena.close()
    }

  /** Runs `pipelined` while a ticker future on the same reactor thread performs zero-length timers one after another,
    * and reports the ticker as `fairness-tick`: the time between ticks over the same period. Futures whose operations
    * rarely wait must still let it run.
    */
  private def fairness(r: UnixStreamBackend, address: StreamAddress, count: Int)(using Async.Spawn): Unit = {
    val trafficDone = new java.util.concurrent.atomic.AtomicBoolean // read by the ticker, maybe on another worker
    var ticks = 0L
    val start = System.nanoTime()
    val ticker = Future {
      while !trafficDone.get() do {
        perform(r, r.ops.timer(0))
        ticks += 1
      }
    }
    pipelined(r, address, count)
    trafficDone.set(true)
    val seconds = (System.nanoTime() - start) / 1e9
    ticker.await
    report("fairness-tick", math.max(ticks, 1L), seconds)
  }

  /** Echoes everything a connection sends until it closes. */
  private def echo(r: UnixStreamBackend, client: Int)(using Async): Unit = {
    val arena = BufferArena.open()
    try {
      val buf = arena.allocate(8192)
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
      arena.close()
      r.handles.close(client)
    }
  }

  /** Starts the echo server on a pool of `workers` reactor threads of its own and returns once it is listening. */
  private def startServer(address: StreamAddress, workers: Int, policy: TakePolicy)(using UnixStreamBackend): Unit = {
    val listening = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable | Null](null)
    val thread = new Thread(() => serve(address, listening, failure, workers, policy), "bench-server")
    thread.setDaemon(true) // it serves until the process exits
    thread.start()
    listening.await()
    val failed = failure.get()
    if failed != null then throw failed
  }

  /** Listens on `address`, counts down `listening`, then echoes every connection until the process exits. A failure to
    * listen is recorded in `failure`.
    */
  private def serve(
      address: StreamAddress,
      listening: CountDownLatch,
      failure: AtomicReference[Throwable | Null],
      workers: Int,
      policy: TakePolicy
  )(using UnixStreamBackend): Unit =
    try
      ReactorFutures.run(workers, policy) { r =>
        val server =
          try Sockets.listen(r, address)
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

  /** Runs the named scenarios, or all of them, each with its base count times `scale`, the clients on a pool of
    * `workers` workers, and the echo server on another of the same size, both taking tasks by `policy`; with `warmup`,
    * each runs first at a fifth of its count, for the JVM's JIT.
    */
  def run(
      names: Seq[String],
      scale: Double,
      connections: Int,
      address: StreamAddress,
      workers: Int = 1,
      policy: TakePolicy = TakePolicy.Affine,
      warmup: Boolean = false
  )(using UnixStreamBackend): Unit = {
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
    val backend = summon[UnixStreamBackend]
    for scenario <- selected if !scenario.runsOn(backend) do
      System.err.println(s"skipping ${scenario.name}: $backend does not have what it needs")
    val runnable = selected.filter(_.runsOn(backend))
    address match {
      case UnixAddress(path) => Files.deleteIfExists(Paths.get(path))
      case _                  => ()
    }
    try {
      startServer(address, workers, policy)
      ReactorFutures.run(workers, policy) { r =>
        // On the JVM, a first pass at a fifth of each count, so the JIT has compiled what is measured; its results,
        // the few a scenario prints itself, come before the measured ones, which `compare.py` keeps.
        if warmup then
          for scenario <- runnable do scenario.body(r, address, math.max(1, (scenario.baseCount * scale / 5).toInt), connections)
        println(f"bench: ${"scenario"}%-18s ${"ops"}%10s ${"seconds"}%9s ${"ops/s"}%12s ${"us/op"}%8s")
        for scenario <- runnable do {
          val count = math.max(1, (scenario.baseCount * scale).toInt)
          val start = System.nanoTime()
          scenario.body(r, address, count, connections)
          val seconds = (System.nanoTime() - start) / 1e9
          report(scenario.name, count, seconds)
        }
      }
    } finally
      address match {
        case UnixAddress(path) => Files.deleteIfExists(Paths.get(path))
        case _                  => ()
      }
  }
}
