package gearsexample

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.Future.awaitAll

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import gears.async.io.Backend
import gears.async.io.BlockingBackend
import gears.async.io.ReactorBackend
import gears.async.io.SocketBackend
import gears.async.io.IpSockets
import gears.async.io.Blocking
import gears.async.io.UnixAddress
import gears.async.io.Received
import gears.async.io.Slot
import gears.async.io.testkit.BufferArena
import gears.async.io.Io
import gears.async.io.ReactorFutures
import gears.async.io.TakePolicy

/** Stress tests for a reactor and its gears integration, to measure each change against. An echo server runs on one
  * reactor thread and the clients on another, so both sides go through the reactor; each scenario is timed on its own
  * and stresses a different cost (see `io-backends/kqueue/TODO.md`). Every line it prints for a scenario starts with
  * `bench:` so that `bench/compare.py` can collect the results.
  */
object GearsBench {

  /** The size of each message a client sends and the server echoes back. */
  inline val MessageSize = 64

  /** The size of the `large` scenarios' messages: 128 KB, or `GEARS_BENCH_LARGE` in the environment, in bytes. */
  val LargeMessage: Int = sys.env.get("GEARS_BENCH_LARGE").map(_.toInt).getOrElse(128 * 1024)

  /** A scenario: its name, what it stresses, how many operations it performs at scale 1, and the client code that
    * performs `count` of them against the echo server, on connections `connect` opens; and whether a backend has what
    * it needs, beyond stream sockets. A backend without it skips the scenario.
    */
  final case class Scenario(
      name: String,
      stresses: String,
      baseCount: Int,
      body: Client,
      runsOn: Backend => Boolean = _ => true
  )

  /** Opens a connection to the echo server. */
  trait Connect[H] {
    def apply()(using Async): H
  }

  trait Client {
    def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using Async.Spawn): Unit
  }

  val scenarios: Seq[Scenario] = Seq(
    Scenario(
      "pingpong",
      "one message in flight: a resume and a readiness wait per message on each side",
      50_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false)
      }
    ),
    Scenario(
      "pingpong-fresh-ops",
      "as pingpong, but building the write and read ops anew for every message, as code that does not keep them does",
      50_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false, freshOps = true)
      }
    ),
    Scenario(
      "pingpong-pooled",
      "as pingpong, reading each echo into a buffer of the backend's pool, which a backend that receives into it hands "
        + "over without a copy",
      50_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false, pooled = true)
      }
    ),
    Scenario(
      "large",
      s"as pipelined, with messages of $LargeMessage bytes and reads into a buffer that fits one: a read's copies, "
        + "at a size where they count",
      2_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pipelined(io, connect, count, messageSize = LargeMessage)
      }
    ),
    Scenario(
      "large-pooled",
      "as large, reading into the backend's pool, whose buffers fit one message too",
      2_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pipelined(io, connect, count, messageSize = LargeMessage, pooled = true)
      }
    ),
    Scenario(
      "pingpong-deadline",
      "as pingpong, with a time limit on every op of the round trip, as a request with a timeout has",
      50_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = true)
      }
    ),
    Scenario(
      "concurrent",
      "many connections ping-ponging at once: many events and registrations per turn",
      100_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(io, connect, each, deadline = false))).awaitAll
        }
      }
    ),
    Scenario(
      "concurrent-compute",
      "as concurrent, with a slice of computation after each round trip, in the connection's own future",
      100_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections)
            .map(_ => Future(pingPong(io, connect, each, deadline = false, work = Work.Inline)))
            .awaitAll
        }
      }
    ),
    Scenario(
      "concurrent-offload",
      "as concurrent-compute, with each slice in a future of its own, awaited before the next round trip",
      100_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(io, connect, each, deadline = false, work = Work.Offload))).awaitAll
        }
      }
    ),
    Scenario(
      "connections",
      "a new connection for each round trip: connect, one message and its echo, close; the server accepts each",
      20_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          var i = 0
          while i < count do {
            pingPong(io, connect, 1, deadline = false)
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
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pipelined(io, connect, count)
      }
    ),
    Scenario(
      "blocking",
      "trivial blocking tasks one after another: a wake-up from a worker thread for each",
      100_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val b: Blocking & io.backend.type = blocking(io)
          var i = 0
          while i < count do {
            io.perform(b.ops.blocking(() => ()))
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
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false, blockingFirst = true)
      },
      _.isInstanceOf[BlockingBackend]
    ),
    Scenario(
      "timers",
      s"timers armed $TimerBatch at a time, each by a future performing it, half firing at once and half cancelled: "
        + "the cost of many pending timers",
      200_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          timerStress(io, count)
      }
    ),
    Scenario(
      "fairness",
      "pipelined traffic while a ticker on the same thread performs zero-length timers; also reports fairness-tick, "
        + "the time between ticks, which rises if busy futures stop yielding",
      200_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          fairness(io, connect, count)
      }
    ),
    Scenario(
      "starvation",
      "concurrent's round trips while a compute future per core works in slices, yielding between them, and a checker "
        + "sleeps 1 ms over and over, as cats-effect's CPU starvation check does; also reports starvation-p50, -p99 "
        + "and -max: how late, in µs, the checker woke",
      100_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          starvation(io, connect, count, connections)
      }
    ),
    Scenario(
      "mixed",
      "pingpong while a compute future on the same thread works in slices, yielding between them; also reports "
        + "mixed-compute, the time per slice meanwhile",
      50_000,
      new Client {
        def apply(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          mixed(io, connect, count)
      }
    )
  )

  /** Runs `program` over the backend with `workers` workers and `policy`, which only a reactor backend has a use for;
    * a caller backend runs it on gears' own threads.
    */
  private def runOn(workers: Int, policy: TakePolicy)(using backend: SocketBackend)(
      program: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> Unit
  ): Unit = backend match {
    case reactors: (ReactorBackend & backend.type) => ReactorFutures.run(workers, policy)(using reactors)(program)
    case _                                         => ReactorFutures.run(using backend)(program)
  }

  /** `io`'s backend as one with blocking ops, which a scenario that runs only on one needs: still `io`'s, so `io`
    * performs its ops.
    */
  private def blocking(io: Io[Backend]): Blocking & io.backend.type = io.backend match {
    case blocking: (Blocking & io.backend.type) => blocking
    case _                                     => throw new IllegalArgumentException(s"$io has no blocking ops")
  }

  /** How many timers the `timers` scenario has pending at once. */
  inline val TimerBatch = 1000

  /** Arms timers `TimerBatch` at a time, each by a future performing it: half for 0 ms, which fire, and half for a
    * minute, whose futures are cancelled once the others' have finished. Each timer, armed and then fired or
    * cancelled, is one operation.
    */
  private def timerStress(io: Io[Backend], count: Int)(using Async.Spawn): Unit = {
    var done = 0
    while done < count do {
      val batch = math.min(TimerBatch, count - done)
      val firing = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
      val cancelled = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
      var i = 0
      while i < batch do {
        if i % 2 == 0 then firing += Future(io.perform(io.backend.ops.timer(0)))
        else cancelled += Future(io.perform(io.backend.ops.timer(60_000)))
        i += 1
      }
      firing.foreach(_.await)
      cancelled.foreach(_.cancel())
      cancelled.foreach(_.awaitResult)
      done += batch
    }
  }

  /** Prints one result line, in the format `bench/compare.py` reads. */
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
  private def mixed(io: Io[IpSockets], connect: Connect[io.Handle], count: Int)(using Async.Spawn): Unit = {
    val pingPongDone = new java.util.concurrent.atomic.AtomicBoolean // read by the other future, maybe on another worker
    var slices = 0L
    val start = System.nanoTime()
    val compute = Future {
      var result = 0L
      while !pingPongDone.get() do {
        result += computeSlice(slices)
        io.`yield`()
        slices += 1
      }
      computeSink = result
    }
    pingPong(io, connect, count, deadline = false)
    pingPongDone.set(true)
    val seconds = (System.nanoTime() - start) / 1e9
    compute.await
    report("mixed-compute", slices, seconds)
  }

  /** How long the `starvation` checker sleeps each time: cats-effect's checker sleeps a second, too long for a bench. */
  inline val StarvationCheckMillis = 1

  /** Runs `concurrent`'s round trips while a compute future per core works in slices, yielding between them, and a
    * checker future sleeps `StarvationCheckMillis` over and over, recording how late it wakes, as cats-effect's CPU
    * starvation check does (`CpuStarvationCheck`): a new event waits for a thread as the checker's wake-up does, so its
    * lateness is how long the program takes to respond, whatever else it is doing. Reports the median, 99th
    * percentile and worst lateness, in µs, as `starvation-p50`, `-p99` and `-max`.
    */
  private def starvation(io: Io[IpSockets], connect: Connect[io.Handle], count: Int, connections: Int)(using
      Async.Spawn
  ): Unit = {
    val done = new java.util.concurrent.atomic.AtomicBoolean // read by the other futures, maybe on other threads
    val interval = StarvationCheckMillis * 1_000_000L
    val late = scala.collection.mutable.ArrayBuffer.empty[Long]
    val checker = Future {
      val timer = io.backend.ops.timer(StarvationCheckMillis)
      while !done.get() do {
        val started = System.nanoTime()
        io.perform(timer)
        late += math.max(0L, System.nanoTime() - started - interval)
      }
    }
    val computes = (1 to Runtime.getRuntime.availableProcessors).map { core =>
      Future {
        var result = 0L
        var slice = core.toLong
        while !done.get() do {
          result += computeSlice(slice)
          io.`yield`()
          slice += 1
        }
        if result == 1L then computeSink = result // only so that the work cannot be optimised away
      }
    }
    val each = math.max(1, count / connections)
    (1 to connections).map(_ => Future(pingPong(io, connect, each, deadline = false))).awaitAll
    done.set(true)
    checker.await
    computes.awaitAll
    val sorted = late.sorted
    def at(fraction: Double): Long = if sorted.isEmpty then 0L else sorted(((sorted.length - 1) * fraction).toInt)
    // As one op taking the lateness, so that `compare.py` shows it in µs.
    report("starvation-p50", 1, at(0.5) / 1e9)
    report("starvation-p99", 1, at(0.99) / 1e9)
    report("starvation-max", 1, at(1.0) / 1e9)
  }

  /** Opens a connection with `connect`, runs `body` on it, and closes it. */
  private def withConnection(io: Io[IpSockets], connect: Connect[io.Handle])(body: io.Handle => Unit)(using
      Async
  ): Unit = {
    val fd = connect()
    try {
      body(fd)
    } finally io.perform(io.backend.ops.close(fd))
  }

  /** Computation done after each round trip by the compute variants of `concurrent`. */
  enum Work { case NoWork, Inline, Offload }

  /** Steps of `computeSlice` done after each round trip by the compute variants of `concurrent`: a few microseconds. */
  inline val WorkSteps = 2_000

  /** Sends `count` messages one at a time, waiting for each echo before sending the next. With `deadline`, every op
    * of a round trip has a time limit, as a request with a timeout has; with `blockingFirst`, a trivial blocking task
    * runs before each round trip; `work` says whether a slice of computation follows each, and where.
    */
  private def pingPong(
      io: Io[IpSockets],
      connect: Connect[io.Handle],
      count: Int,
      deadline: Boolean,
      freshOps: Boolean = false,
      blockingFirst: Boolean = false,
      work: Work = Work.NoWork,
      pooled: Boolean = false,
      messageSize: Int = MessageSize
  )(using Async.Spawn): Unit = {
    // The pool outlives the connection: a receive into one of its buffers may be outstanding until the close op is
    // done, which gives the buffer back.
    val pool = if pooled then io.backend.pool(messageSize, 2) else null
    try withConnection(io, connect) { fd =>
      val arena = BufferArena.open()
      try {
        val buf = arena.allocate(messageSize)
        val write = io.backend.ops.write(fd, buf)
        val read = io.backend.ops.read(fd, buf)
        val into = Slot[Received]()
        val pooledRead = if pooled then io.backend.ops.read(fd, pool.nn, into) else null
        // The time limit, where the scenario has one: long enough never to be reached, so it costs only its arming.
        val limit = if deadline then 60_000 else Backend.NoTimeout
        val blocker: Option[Blocking & io.backend.type] = if blockingFirst then Some(blocking(io)) else None
        var worked = 0L
        var i = 0
        while i < count do {
          blocker match {
            case Some(b) => io.perform(b.ops.blocking(() => ()))
            case None    => ()
          }
          buf.clear()
          while buf.hasRemaining() do io.perform(if freshOps then io.backend.ops.write(fd, buf) else write, limit)
          if pooled then {
            var got = 0
            while got < messageSize do {
              io.perform(pooledRead.nn, limit)
              val received = into.clear()
              val n = received.buffer.remaining()
              received.release()
              if n == 0 then throw new IOException("The server closed the connection")
              got += n
            }
          } else {
            buf.clear()
            while buf.hasRemaining() do {
              val before = buf.position()
              io.perform(if freshOps then io.backend.ops.read(fd, buf) else read, limit)
              if buf.position() == before then throw new IOException("The server closed the connection")
            }
          }
          work match {
            case Work.NoWork  => ()
            case Work.Inline  => worked += computeSlice(i, WorkSteps)
            case Work.Offload => worked += Future(computeSlice(i, WorkSteps)).await
          }
          i += 1
        }
        if worked == 1L then computeSink = worked // only so that the work cannot be optimised away
      } finally arena.close()
    } finally if pool != null then pool.close()
  }

  /** Writes `count` messages of `messageSize` bytes, each as its own write, from one future while reading the echoes
    * in another, into a buffer of 8 KB, or, with `pooled`, into the backend's pool of buffers the size of a message.
    */
  private def pipelined(
      io: Io[IpSockets],
      connect: Connect[io.Handle],
      count: Int,
      messageSize: Int = MessageSize,
      pooled: Boolean = false
  )(using Async.Spawn): Unit = {
    val pool = if pooled then io.backend.pool(messageSize, 2) else null // outlives the connection: see `pingPong`
    try withConnection(io, connect) { fd =>
      val arena = BufferArena.open()
      try {
        val total = count.toLong * messageSize
        val out = arena.allocate(messageSize)
        val in = arena.allocate(if messageSize > 8192 then messageSize else 8192)
        val writer = Future {
          val write = io.backend.ops.write(fd, out)
          var i = 0
          while i < count do {
            out.clear()
            while out.hasRemaining() do io.perform(write)
            i += 1
          }
        }
        var received = 0L
        if pooled then {
          val into = Slot[Received]()
          val read = io.backend.ops.read(fd, pool.nn, into)
          while received < total do {
            io.perform(read)
            val got = into.clear()
            val n = got.buffer.remaining()
            got.release()
            if n == 0 then throw new IOException("The server closed the connection")
            received += n
          }
        } else {
          val read = io.backend.ops.read(fd, in)
          while received < total do {
            in.clear()
            io.perform(read)
            if in.position() == 0 then throw new IOException("The server closed the connection")
            received += in.position()
          }
        }
        writer.await
      } finally arena.close()
    } finally if pool != null then pool.close()
  }

  /** Runs `pipelined` while a ticker future on the same reactor thread performs zero-length timers one after another,
    * and reports the ticker as `fairness-tick`: the time between ticks over the same period. Futures whose operations
    * rarely wait must still let it run.
    */
  private def fairness(io: Io[IpSockets], connect: Connect[io.Handle], count: Int)(using Async.Spawn): Unit = {
    val trafficDone = new java.util.concurrent.atomic.AtomicBoolean // read by the ticker, maybe on another worker
    var ticks = 0L
    val start = System.nanoTime()
    val ticker = Future {
      while !trafficDone.get() do {
        io.perform(io.backend.ops.timer(0))
        ticks += 1
      }
    }
    pipelined(io, connect, count)
    trafficDone.set(true)
    val seconds = (System.nanoTime() - start) / 1e9
    ticker.await
    report("fairness-tick", math.max(ticks, 1L), seconds)
  }

  /** Echoes everything a connection sends until it closes. */
  private def echo(io: Io[IpSockets], client: io.Handle)(using Async): Unit = {
    val arena = BufferArena.open()
    try {
      val buf = arena.allocate(8192)
      val read = io.backend.ops.read(client, buf)
      val write = io.backend.ops.write(client, buf)
      var open = true
      while open do {
        buf.clear()
        io.perform(read)
        if buf.position() == 0 then open = false // end of stream
        else {
          buf.flip()
          while buf.hasRemaining() do io.perform(write)
        }
      }
    } catch case _: IOException => () // a client that goes away mid-write ends only its own connection
    finally {
      arena.close()
      io.perform(io.backend.ops.close(client))
    }
  }

  /** Starts the echo server on a pool of `workers` reactor threads of its own and returns once it is listening. */
  private def startServer[B <: IpSockets](endpoint: Endpoint[B], workers: Int, policy: TakePolicy)(using B): Unit = {
    val listening = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable | Null](null)
    val thread = new Thread(() => serve(endpoint, listening, failure, workers, policy), "bench-server")
    thread.setDaemon(true) // it serves until the process exits
    thread.start()
    listening.await()
    val failed = failure.get()
    if failed != null then throw failed
  }

  /** Listens on `endpoint`, counts down `listening`, then echoes every connection until the process exits. A failure to
    * listen is recorded in `failure`.
    */
  private def serve[B <: IpSockets](
      endpoint: Endpoint[B],
      listening: CountDownLatch,
      failure: AtomicReference[Throwable | Null],
      workers: Int,
      policy: TakePolicy
  )(using B): Unit =
    try
      runOn(workers, policy) { io ?=>
        val server =
          try endpoint.listen()
          finally listening.countDown()
        Sockets.serve(server)(client => echo(io, client))
      }
    catch {
      case t: Throwable =>
        failure.set(t)
        listening.countDown()
    }

  /** Runs the echo server alone, on a pool of `workers` workers, until the process ends: for a bench on another
    * machine, which connects to it over the network (`run` with `server = false`).
    */
  def serveForever[B <: IpSockets](endpoint: Endpoint[B], workers: Int = 1, policy: TakePolicy = TakePolicy.Affine)(using
      B
  ): Unit = {
    val listening = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable | Null](null)
    serve(endpoint, listening, failure, workers, policy)
    val failed = failure.get()
    if failed != null then throw failed
  }

  /** Runs the named scenarios, or all of them, each with its base count times `scale`, the clients on a pool of
    * `workers` workers, and the echo server on another of the same size, both taking tasks by `policy`; with `warmup`,
    * each runs first at a fifth of its count, for the JVM's JIT. Without `server`, the echo server is expected to be
    * listening at `endpoint` already, such as `serveForever` on another machine.
    */
  def run[B <: IpSockets](
      names: Seq[String],
      scale: Double,
      connections: Int,
      endpoint: Endpoint[B],
      workers: Int = 1,
      policy: TakePolicy = TakePolicy.Affine,
      warmup: Boolean = false,
      server: Boolean = true
  )(using backend: B): Unit = {
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
    for scenario <- selected if !scenario.runsOn(backend) do
      System.err.println(s"skipping ${scenario.name}: $backend does not have what it needs")
    val runnable = selected.filter(_.runsOn(backend))
    Sockets.removeFile(endpoint.address)
    try {
      if server then startServer(endpoint, workers, policy)
      runOn(workers, policy) { io ?=>
        val connect = new Connect[io.Handle] { def apply()(using Async): io.Handle = endpoint.connect() }
        // On the JVM, a first pass at a fifth of each count, so the JIT has compiled what is measured; its results,
        // the few a scenario prints itself, come before the measured ones, which `compare.py` keeps.
        if warmup then
          for scenario <- runnable do scenario.body(io, connect, math.max(1, (scenario.baseCount * scale / 5).toInt), connections)
        println(f"bench: ${"scenario"}%-18s ${"ops"}%10s ${"seconds"}%9s ${"ops/s"}%12s ${"us/op"}%8s")
        for scenario <- runnable do {
          val count = math.max(1, (scenario.baseCount * scale).toInt)
          val start = System.nanoTime()
          scenario.body(io, connect, count, connections)
          val seconds = (System.nanoTime() - start) / 1e9
          report(scenario.name, count, seconds)
        }
      }
    } finally Sockets.removeFile(endpoint.address)
  }
}
