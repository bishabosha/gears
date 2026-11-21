package gears.async.io

import gears.async.Async
import gears.async.Future

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

/** EXPERIMENT. A connection as a server's handler has it: what it brings, and where what is sent on it goes. */
final class Connection private[io] (
    /** What the connection brings, as it brings it: its read buffer, lent until what consumes it returns. Ends when
      * the peer has. To be run once.
      */
    val bytes: Flow[ByteBuffer],
    /** What is sent on the connection: taken at once, and written, with whatever else was sent meanwhile by any flow
      * of the handler's, with one write when none of them has more to do.
      */
    val sink: Sink[ByteBuffer]
) {

  /** Sends what remains of `bytes` on the connection: `sink.send`. */
  def send(bytes: ByteBuffer)(using Async): Unit = sink.send(bytes)
}

/** EXPERIMENT. A listener and what serves the connections it accepts: a handler that is run, a future for each
  * connection (`serve`), or a pipeline of stages that is stepped (`serveFlow`). Where the backend has shards, the
  * as a flow, and with it whatever flows it begins.
  */
final class Server private (
    val io: Io[IpStreams],
    listening: AnyRef,
    sizes: Server.Sizes,
    asked: Server.Strategy
) {
  import Server.Strategy

  /** How it serves: what was asked for, or, asked for nothing (`Auto`), the best that the backend's capabilities and
    * the platform's flows allow.
    */
  val strategy: Strategy = {
    val sharded = io.ops.isInstanceOf[Shards]
    asked match {
      case Strategy.Auto =>
        // A connection's futures gain by its ops being on one shard, where the backend has shards.
        if sharded then Strategy.ShardFutures else Strategy.Futures
      case other =>
        if !Server.strategies(io).contains(other) then
          throw new IllegalArgumentException(s"The backend cannot serve by $other: ${Server.strategies(io).mkString(", ")}")
        other
    }
  }
  private val listener = listening.asInstanceOf[io.ops.Listener]
  private val stopping = new AtomicReference[() => Unit | Null](null)
  @volatile private var closed = false
  @volatile private var failure: Throwable | Null = null

  /** Where it listens. */
  def address: IpSocketAddress = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]

  /** Serves every connection that comes with `handler`, each in a future of its own, until `close`: then returns
    * once the connections there are have been served, with the first failure a handler had thrown. A connection is
    * closed as its handler ends, what it sent written first.
    */
  /** Serves every connection that comes by a pipeline: `pipeline` is called once for a connection, and gives the flow
    * of what is to be sent on it, made of the connection's `bytes` by stages (`map`, `filter`, `collect`,
    * `mapConcat`, `process`, `groupThrough`, `take`, `scan`, `grouped` and the like, and `mapAsync`, the one that
    * waits). Nothing of a connection is then run: as its bytes come
    * they are pushed through its stages, on one thread, one chunk after another, and what comes out is written.
    *
    * Where the backend has submission queues that is done by a machine, one for each shard, with no future, flow or
    * stack for a connection at all. Elsewhere, or with `machine` false, the same pipeline is run by a future for each
    * connection, as `serve` would run `pipeline(connection).to(connection.sink)`. So is a connection whose pipeline
    * turns out not to be of stages alone, whatever the rest are: how it is served is a matter of speed, not of what
    * it does. `pipeline` only makes a flow, and may be called more than once for a connection.
    */
  def serveFlow(pipeline: Connection => Flow[ByteBuffer], machine: Boolean = true)(using Async): Unit =
    if machine && io.ops.isInstanceOf[SubmissionQueues] then serveByMachine(pipeline)
    else serve(connection => Flow.to(pipeline(connection))(connection.sink))

  private def serveByMachine(pipeline: Connection => Flow[ByteBuffer])(using Async): Unit = {
    Async.group {
      val shards = io.submissionShards
      val drivers = Array.tabulate(shards)(shard => new Driver(shard, pipeline))
      val running = drivers.toList.map(driver => Future(driver.run()))
      val accepting = Future {
        val accepted = Slot[io.ops.Connection]()
        val accept = io.ops.accept(listener, accepted)
        var next = 0
        while true do {
          io.perform(accept)
          drivers(next % shards).hand(accepted.clear())
          next += 1
        }
      }
      stopping.set { () =>
        accepting.cancel()
        drivers.foreach(_.close())
      }
      if closed then stopping.get().nn.apply()
      running.foreach(_.await)
      io.perform(io.ops.close(listener))
    }
    val thrown = failure
    if thrown != null then throw thrown
  }

  /** A shard's machine: every connection handed to it is a few fields and a read in flight on the shard's submission
    * queue. One future runs it, and waits once for whatever its connections wait for.
    */
  private final class Driver(shard: Int, pipeline: Connection => Flow[ByteBuffer]) {
    private val ops = new SubmissionQueue[io.Op](shard)
    private val completed = new CompletionQueue[io.Op]
    private val handed = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]()
    @volatile private var open = true
    private var live = 0
    // What a stage that waits has begun and not been given back, and what it has made, to be run here.
    private val working = new java.util.concurrent.atomic.AtomicInteger(0)
    private val returned = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()

    /** From any thread. */
    def hand(connection: io.ops.Connection): Unit = {
      handed.add(connection.asInstanceOf[AnyRef]): Unit
      ops.signal()
    }

    def close(): Unit = {
      open = false
      ops.signal()
    }

    def run()(using spawning: Async.Spawn): Unit =
      while open || live > 0 || !handed.isEmpty() do {
        var next = handed.poll()
        while next != null do {
          live += 1
          val served = new Served(next.asInstanceOf[io.ops.Connection])
          served.begin()
          next = handed.poll()
        }
        // With no connection yet, or none with anything in flight, it waits to be handed one, or closed, or for
        // what a stage's work has made.
        ops.lingers = open || working.get() > 0
        if returned.isEmpty() then io.performSubmitted(ops, completed)
        var i = 0
        while i < completed.count do {
          completed.tag(i).asInstanceOf[Served].done(completed.op(i), completed.failure(i))
          i += 1
        }
        completed.clear()
        var back = returned.poll()
        while back != null do {
          back.run()
          back = returned.poll()
        }
      }

    /** A connection: what is read into, what is written from, the read and the write of them, and the step its bytes
      * are pushed to.
      */
    private final class Served(connection: io.ops.Connection)(using spawning: Async.Spawn) extends Flow.Drive {
      private val in = sizes.buffers(sizes.read)
      private val read = io.ops.read(connection, in)
      // One fills while the other is written.
      private var filling = sizes.buffers(sizes.write)
      private var sending = sizes.buffers(sizes.write)
      private var fillWrite = io.ops.write(connection, filling)
      private var sendWrite = io.ops.write(connection, sending)
      private var reading = false
      private var writing = false
      // No more will be read; and the pipeline has ended, which with a stage that waits may be later.
      private var ended = false
      private var finished = false
      private var closing = false
      private var held = 0
      private var step: Flow.Step[Any] | Null = null

      // ---- for a stage that waits

      def spawn(work: Async ?=> Unit): Unit = {
        working.incrementAndGet(): Unit
        Future {
          try work
          finally {
            working.decrementAndGet(): Unit
            ops.signal()
          }
        }(using spawning): Unit
      }

      def later(run: Runnable): Unit = {
        returned.add { () =>
          if !closing then
            try {
              run.run()
              flush()
              rearm()
              leave(now = false)
            } catch case thrown: Throwable => fail(thrown)
        }: Unit
        ops.signal()
      }

      def hold(): Unit = held += 1

      /** A stage wants no more: no more is read, and the connection is closed when what was made has been written. */
      def stop(): Unit = ended = true

      def release(): Unit = {
        held -= 1
        rearm()
      }

      /** Reads again, unless a read is under way, there is no more to read, a stage holds the source, or more waits
        * to be written than a write's worth: its peer is not taking it.
        */
      private def rearm(): Unit =
        if !reading && !ended && !closing && held == 0 && !(writing && filling.position() >= sizes.write) then arm()

      private def same(a: Any, b: Any): Boolean = a.asInstanceOf[AnyRef] eq b.asInstanceOf[AnyRef]

      /** What comes out of the pipeline is copied in here, to be written when this chunk's stages have returned. */
      private def take(bytes: ByteBuffer): Unit = {
        val length = bytes.remaining()
        if filling.remaining() < length then {
          val larger = sizes.buffers(math.max(filling.capacity() * 2, filling.position() + length))
          filling.flip()
          larger.put(filling)
          filling = larger
          fillWrite = io.ops.write(connection, larger)
        }
        filling.put(bytes)
      }

      def begin(): Unit =
        try {
          filling.clear()
          sending.clear()
          val port = new Flow.Port[ByteBuffer]
          val sink = new Sink[ByteBuffer] {
            def send(element: ByteBuffer)(using Async): Unit = take(element)
            def flush()(using Async): Unit = ()
          }
          val out = new Flow.Step[ByteBuffer] {
            def push(element: ByteBuffer): Unit = take(element)
            def end(): Unit = finished = true
          }
          val made = Flow.compile(pipeline(new Connection(port, sink)), port, out, this)
          if made == null then alone()
          else {
            step = made
            rearm()
          }
        } catch case thrown: Throwable => fail(thrown)

      /** The pipeline is not one of stages alone, so cannot be stepped: this connection is run, by a future of its
        * own, its ops performed on this shard, and the machine has only to hear that it is done.
        */
      private def alone(): Unit = {
        closing = true
        working.incrementAndGet(): Unit
        val on: Io[io.ops.type] =
          if io.ops.isInstanceOf[Shards] then
            Shards.onShard(io.asInstanceOf[Io[Shards]])(shard).asInstanceOf[Io[io.ops.type]]
          else io.asInstanceOf[Io[io.ops.type]]
        Future {
          try serveAlone(on, connection, served => Flow.to(pipeline(served))(served.sink), -1)
          finally {
            returned.add(() => live -= 1): Unit
            working.decrementAndGet(): Unit
            ops.signal()
          }
        }(using spawning): Unit
      }

      private def arm(): Unit = {
        in.clear()
        ops.submit(read, this)
        reading = true
      }

      private def flush(): Unit =
        if !writing && filling.position() > 0 then {
          val buffer = filling
          filling = sending
          sending = buffer
          val write = fillWrite
          fillWrite = sendWrite
          sendWrite = write
          sending.flip()
          filling.clear()
          ops.submit(sendWrite, this)
          writing = true
        }

      private def fail(thrown: Throwable): Unit = {
        if failure == null then failure = thrown
        ended = true
        finished = true
        leave(now = true)
      }

      /** The connection is closed once it has ended and what it sent is written; `now`, whatever is in flight. */
      private def leave(now: Boolean): Unit =
        // A read still in flight when a stage stopped the pipeline is left to the close.
        if !closing && finished && (now || (!writing && (!reading || ended) && filling.position() == 0)) then {
          closing = true
          ops.submit(io.ops.close(connection), this)
        }

      def done(op: io.Op, thrown: Throwable | Null): Unit =
        if closing then {
          // Its close, or what the close cut short.
          if !same(op, read) && !same(op, sendWrite) then live -= 1
        } else if same(op, read) then {
          reading = false
          if thrown != null then fail(thrown)
          else if ended then leave(now = false)
          else
            try {
              in.flip()
              if in.hasRemaining() then step.nn.push(in)
              else {
                ended = true
                step.nn.end()
              }
              flush()
              rearm()
              leave(now = false)
            } catch case failed: Throwable => fail(failed)
        } else {
          if thrown != null then {
            writing = false
            fail(thrown)
          } else if sending.hasRemaining() then ops.submit(sendWrite, this)
          else {
            writing = false
            flush()
            rearm()
            leave(now = false)
          }
        }
    }
  }

  def serve(handler: Connection => Async ?=> Unit)(using Async): Unit = {
    serveWithFutures(handler)
    val thrown = failure
    if thrown != null then throw thrown
  }

  /** A future for each connection, and one that writes what it sends. */
  private def serveWithFutures(handler: Connection => Async ?=> Unit)(using Async): Unit =
    Async.group { outer ?=>
      val serving = new java.util.concurrent.ConcurrentLinkedQueue[Future[Unit]]()
      val accepting = Future {
        val accepted = Slot[io.ops.Connection]()
        val accept = io.ops.accept(listener, accepted)
        val shards = io.submissionShards
        var next = 0
        while true do {
          io.perform(accept)
          val connection = accepted.clear()
          // Pinned, a connection's ops are all performed on one shard, in turn: its `Io` is the view on it.
          val shard = next % shards
          val on: Io[io.ops.type] =
            if strategy == Strategy.Futures then io.asInstanceOf[Io[io.ops.type]]
            else Shards.onShard(io.asInstanceOf[Io[Shards]])(shard).asInstanceOf[Io[io.ops.type]]
          next += 1
          // The server's, not this future's: a connection goes on being served when the accepting stops.
          serving.add(Future {
            // On its shard from the first, so that what it begins is there too, and what it sends in a turn of that
            // thread's is written at the turn's end.
            if strategy == Strategy.ShardFutures then on.moveTo(shard)
            serveAlone(on, connection, handler, if strategy == Strategy.ShardFutures then shard else -1)
          }(using outer)): Unit
        }
      }
      stopping.set(() => accepting.cancel())
      if closed then accepting.cancel()
      accepting.awaitResult: Unit
      var next = serving.poll()
      while next != null do {
        next.awaitResult: Unit
        next = serving.poll()
      }
      io.perform(io.ops.close(listener))
    }

  private def serveAlone(
      on: Io[io.ops.type],
      connection: io.ops.Connection,
      handler: Connection => Async ?=> Unit,
      turnsOf: Int
  )(
      using Async
  ): Unit =
    try
      Async.group {
        val buf = sizes.buffers(sizes.read)
        val read = on.ops.read(connection, buf)
        val first = sizes.buffers(sizes.write)
        val second = sizes.buffers(sizes.write)
        val writeFirst = on.ops.write(connection, first)
        val writeSecond = on.ops.write(connection, second)
        val outbox =
          new Server.SharedOutbox(first, second, if turnsOf < 0 then _.run() else on.afterTurn(turnsOf)(_))
        val writing = Future {
          outbox.run { which =>
            val out = if which == 0 then first else second
            val write = if which == 0 then writeFirst else writeSecond
            while out.hasRemaining() do on.perform(write)
          }
        }
        val bytes = Server.reading(buf)(on.perform(read))
        val sink = new Sink[ByteBuffer] {
          def send(element: ByteBuffer)(using Async): Unit = outbox.put(element)
          def flush()(using Async): Unit = outbox.drain()
        }
        // The writer is parked where no cancel reaches it: it is told to end, however the handler did.
        try {
          handler(new Connection(bytes, sink))
          outbox.drain()
        } finally outbox.close()
        writing.awaitResult: Unit
      }
    catch {
      case thrown: Throwable => if failure == null then failure = thrown
    } finally
      try on.perform(on.ops.close(connection))
      catch case _: java.io.IOException => ()

  /** From any thread: no more connections are accepted, and `serve` returns once those there are have been served. */
  def close(): Unit = {
    closed = true
    val stop = stopping.get()
    if stop != null then stop()
  }
}

object Server {

  /** How large a connection's buffers are, the one it reads into and the two it writes from, and how they are made. */
  final case class Sizes(
      read: Int = 64 * 1024,
      write: Int = 64 * 1024,
      buffers: Int => ByteBuffer = ByteBuffer.allocateDirect
  )

  /** How a server serves. A handler is the same under each, and must not count on what only some give: under
    * `Futures` a connection's flows run on any thread and in no order among themselves, so what they share is locked
    * (with a lock that awaits, `gears.async.Semaphore`), which costs little when nothing else holds it.
    */
  enum Strategy {

    /** Whichever of the others is best for the backend and the platform. */
    case Auto

    /** A future for each connection, a future that writes what it sends, channels and futures for its streams. */
    case Futures

    /** `Futures`, each connection's ops performed on one shard, in turn (`Shards.onShard`). */
    case PinnedFutures

    /** `PinnedFutures`, with the scheduler's hooks: a connection's future begins on its shard, and what its flows
      * send in a turn of that shard's thread is written when the turn's work is done.
      */
    case ShardFutures
  }

  /** A connection's bytes: `read` fills `buf`, which is lent to the consumer until it returns, until a read brings
    * nothing, which is the peer's end.
    */
  private def reading(buf: ByteBuffer)(read: Async ?=> Unit): Flow[ByteBuffer] = Flow {
    var open = true
    while open do {
      buf.clear()
      read
      buf.flip()
      if buf.hasRemaining() then Flow.emit(buf) else open = false
    }
  }

  /** The strategies `io`'s backend can serve by. */
  def strategies(io: Io[IpStreams]): List[Strategy] = {
    val sharded = io.ops.isInstanceOf[Shards]
    List(Strategy.Futures) ++ (if sharded then List(Strategy.PinnedFutures, Strategy.ShardFutures) else Nil)
  }

  /** A server listening at `address`, a port of 0 for any. It serves by `strategy`.
    */
  def tcp(address: IpSocketAddress, sizes: Sizes = Sizes(), strategy: Strategy = Strategy.Auto)(using
      io: Io[IpStreams],
      async: Async
  ): Server = {
    val into = Slot[io.ops.Listener]()
    io.perform(io.ops.listen(address, into))
    new Server(io, into.clear(), sizes, strategy)
  }

  /** What futures send on one connection, gathered under a lock for the one future that writes: it writes all that
    * was put since its last write, so what is put while it writes, or before it is run, goes with one op.
    */
  private[io] final class SharedOutbox(first: ByteBuffer, second: ByteBuffer, telling: Runnable => Unit) {
    // The writer, parked until there is something to write (`Async.park`): woken by this, at once or at a turn's end.
    private val parked = new java.util.concurrent.atomic.AtomicReference[Runnable | Null](null)
    private val pending = new java.util.concurrent.atomic.AtomicBoolean(false)
    @volatile private var closing = false
    private val tellWriter: Runnable = () => {
      pending.set(true)
      val resume = parked.getAndSet(null)
      if resume != null then resume.run()
    }
    private val bufs = Array(first, second)
    private var filling = 0
    private var writing = false
    private var told = false
    private var failure: Throwable | Null = null
    private var changed: Future.Promise[Unit] | Null = null
    first.clear()
    second.clear()

    def put(bytes: ByteBuffer)(using Async): Unit = {
      val length = bytes.remaining()
      require(length <= first.capacity(), s"$length bytes are more than the outbox holds")
      var done = false
      while !done do {
        var tell = false
        var wait: Future.Promise[Unit] | Null = null
        synchronized {
          val failed = failure
          if failed != null then throw failed
          if bufs(filling).remaining() >= length then {
            bufs(filling).put(bytes)
            done = true
            if !told then {
              told = true
              tell = true
            }
          } else {
            if changed == null then changed = Future.Promise[Unit]()
            wait = changed
          }
        }
        // At once, or, where its shard's thread has turns, at the end of this one: with all the turn's sends.
        if tell then telling(tellWriter)
        if wait != null then wait.asFuture.await
      }
    }

    def drain()(using Async): Unit = {
      var done = false
      while !done do {
        var wait: Future.Promise[Unit] | Null = null
        synchronized {
          val failed = failure
          if failed != null then throw failed
          if !writing && !told && bufs(filling).position() == 0 then done = true
          else {
            if changed == null then changed = Future.Promise[Unit]()
            wait = changed
          }
        }
        if wait != null then wait.asFuture.await
      }
    }

    def close(): Unit = {
      closing = true
      tellWriter.run()
    }

    /** The writer: `write` writes the buffer of the number it is given, whole. Until `close`. */
    def run(write: Int => Unit)(using async: Async): Unit = {
      var open = true
      while open do {
        if !pending.get() then
          async.park { resume =>
            parked.set(resume)
            // Told meanwhile: nobody was there to wake.
            if pending.get() then {
              val mine = parked.getAndSet(null)
              if mine != null then mine.run()
            }
          }
        pending.set(false)
        if closing then open = false
        {
            var which = -1
            synchronized {
              told = false
              if bufs(filling).position() > 0 then {
                bufs(filling).flip()
                which = filling
                filling = 1 - filling
                bufs(filling).clear()
                writing = true
              }
            }
            if which >= 0 then
              try write(which)
              catch case thrown: Throwable => synchronized { failure = thrown }
            val waiting = synchronized {
              writing = false
              val waiting = changed
              changed = null
              waiting
            }
            if waiting != null then waiting.complete(scala.util.Success(()))
        }
      }
    }
  }
}
