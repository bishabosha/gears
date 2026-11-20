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

/** EXPERIMENT. A listener and the reactors that serve what it accepts, one for each shard of the backend's queues:
  * a connection is accepted by a future of its own and handed to a shard's reactor, in turn, where the handler runs
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
    val queued = io.ops.isInstanceOf[SubmissionQueues]
    asked match {
      case Strategy.Auto =>
        // A reactor is worth having where its flows are continuations; where each is a thread, futures do as well.
        // Futures gain by a connection's ops being on one shard, as reactors do.
        if !io.flowsAreContinuations then (if sharded then Strategy.PinnedFutures else Strategy.Futures)
        // Pinned, a reactor gains nothing more by a queue, as far as has been measured.
        else if sharded then Strategy.PinnedReactors
        else if queued then Strategy.QueuedReactors
        else Strategy.Reactor
      case other =>
        if !Server.strategies(io).contains(other) then
          throw new IllegalArgumentException(s"The backend cannot serve by $other: ${Server.strategies(io).mkString(", ")}")
        other
    }
  }
  private val reactors =
    strategy != Strategy.Futures && strategy != Strategy.PinnedFutures && strategy != Strategy.ShardFutures
  private val mode = strategy match {
    case Strategy.PinnedReactors => Reactor.Pinned
    case Strategy.QueuedReactors => Reactor.Queued
    case _                       => Reactor.Unqueued
  }

  private val listener = listening.asInstanceOf[io.ops.Listener]
  private val stopping = new AtomicReference[() => Unit | Null](null)
  @volatile private var closed = false
  @volatile private var failure: Throwable | Null = null

  /** Where it listens. */
  def address: IpSocketAddress = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]

  /** Serves every connection that comes with `handler`, each in a flow of a reactor's, until `close`: then returns
    * once the connections there are have been served, with the first failure a handler had thrown. A connection is
    * closed as its handler ends, what it sent written first.
    */
  def serve(handler: Connection => Async ?=> Unit)(using Async): Unit = {
    if !reactors then serveWithFutures(handler)
    else Async.group {
      // One reactor where there is nothing to pin one to.
      val shards = if strategy == Strategy.Reactor then 1 else io.submissionShards
      val made = Array.fill(shards)(Future.Promise[Reactor[io.type]]())
      val running = (0 until shards).toList.map { shard =>
        Future {
          val reactor = Reactor(io, shard, mode)
          reactor.keepOpen()
          made(shard).complete(scala.util.Success(reactor))
          reactor.run()
        }
      }
      val reactors = made.map(_.asFuture.await)
      val accepting = Future {
        val accepted = Slot[io.ops.Connection]()
        val accept = io.ops.accept(listener, accepted)
        var next = 0
        while true do {
          io.perform(accept)
          val connection = accepted.clear()
          val reactor = reactors(next % shards)
          next += 1
          // From this future's thread to the reactor's: the handler begins there, at its next turn.
          reactor.submit(serveOne(reactor, connection, handler))
        }
      }
      stopping.set { () =>
        accepting.cancel()
        reactors.foreach(_.close())
      }
      if closed then stopping.get().nn.apply()
      running.foreach(_.await)
      io.perform(io.ops.close(listener))
    }
    val thrown = failure
    if thrown != null then throw thrown
  }

  /** The same with no reactor and no queue: a future for each connection, and one that writes what it sends. */
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
        handler(new Connection(bytes, sink))
        outbox.drain()
        outbox.close()
        writing.awaitResult: Unit
      }
    catch {
      case thrown: Throwable => if failure == null then failure = thrown
    } finally
      try on.perform(on.ops.close(connection))
      catch case _: java.io.IOException => ()

  private def serveOne(reactor: Reactor[io.type], connection: io.ops.Connection, handler: Connection => Async ?=> Unit)(
      using Async
  ): Unit = {
    val on = reactor.io
    try {
      val buf = sizes.buffers(sizes.read)
      val read = on.ops.read(connection, buf)
      val outbox = reactor.outbox(sizes.buffers(sizes.write), sizes.buffers(sizes.write))(out =>
        on.ops.write(connection, out)
      )
      val bytes = Server.reading(buf)(on.perform(read))
      val sink = new Sink[ByteBuffer] {
        def send(element: ByteBuffer)(using Async): Unit = outbox.put(element)
        def flush()(using Async): Unit = outbox.drain()
      }
      handler(new Connection(bytes, sink))
      outbox.drain()
    } catch {
      case thrown: Throwable => if failure == null then failure = thrown
    } finally
      try on.perform(on.ops.close(connection))
      catch case _: java.io.IOException => ()
  }

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
    * (with a lock that awaits, `gears.async.Semaphore`), which costs a flow of a reactor's nothing to speak of.
    */
  enum Strategy {

    /** Whichever of the others is best for the backend and the platform. */
    case Auto

    /** A future for each connection, a future that writes what it sends, channels and futures for its streams. */
    case Futures

    /** `Futures`, each connection's ops performed on one shard, in turn (`Shards.onShard`). */
    case PinnedFutures

    /** `PinnedFutures`, with the scheduler's hooks: a connection's future begins on its shard, and what its flows
      * send in a turn of that shard's thread is written when the turn's work is done. A reactor's two gains,
      * affinity and one write a turn, with futures for its flows.
      */
    case ShardFutures

    /** One reactor: a connection's flows on one thread, their writes gathered. For a backend with no capability. */
    case Reactor

    /** A reactor for each shard, a connection's ops performed on its reactor's (`Shards`). */
    case PinnedReactors

    /** A reactor for each shard, its flows' ops on a submission queue (`SubmissionQueues`). */
    case QueuedReactors
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
    val queued = io.ops.isInstanceOf[SubmissionQueues]
    // A reactor with no queue has its flows wait for their ops as futures do, with their `Async`.
    val unqueued = io.opsAwaitThroughAsync
    List(Strategy.Futures) ++
      (if sharded then List(Strategy.PinnedFutures, Strategy.ShardFutures) else Nil) ++
      (if unqueued then List(Strategy.Reactor) else Nil) ++
      (if unqueued && sharded then List(Strategy.PinnedReactors) else Nil) ++
      (if queued then List(Strategy.QueuedReactors) else Nil)
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
    private val tellWriter: Runnable = () => dirty.sendImmediately(())
    private val bufs = Array(first, second)
    private var filling = 0
    private var writing = false
    private var told = false
    private var failure: Throwable | Null = null
    private var changed: Future.Promise[Unit] | Null = null
    private val dirty = gears.async.UnboundedChannel[Unit]()
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

    def close(): Unit = dirty.close()

    /** The writer: `write` writes the buffer of the number it is given, whole. Until `close`. */
    def run(write: Int => Unit)(using Async): Unit = {
      var open = true
      while open do
        dirty.read() match {
          case Left(_)  => open = false
          case Right(_) =>
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
