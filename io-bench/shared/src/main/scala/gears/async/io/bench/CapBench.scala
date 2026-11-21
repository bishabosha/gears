package gears.async.io
package bench

import gears.async.Async
import gears.async.Future

import java.nio.ByteBuffer

/** Scenarios over every capability of the interface, written against the interface alone, so that the same source runs
  * over any backend, and over another branch's: what a backend's `BenchMain` runs. Each prints one line, `bench: <name>
  * <ops> ops <seconds> s <microseconds> us/op`, which `compare.Compare` reads.
  *
  * A scenario that needs a capability the backend lacks, FIFOs or Unix datagram sockets on the JVM, is skipped.
  */
object CapBench {

  /** What the scenarios are written over; a backend runs those it has the capability for (`supports`). */
  type All = Pipes & IpStreams & UnixStreams & IpDatagrams & RegularFiles & Blocking & Resolving

  def buffer(size: Int): ByteBuffer = BenchPlatform.buffer(size)

  /** A buffer the scenario reads and fills by way of arrays (see `BenchPlatform.arrayBuffer`). */
  def arrayBuffer(size: Int): ByteBuffer = BenchPlatform.arrayBuffer(size)

  final case class Args(scenarios: Seq[String], scale: Double, connections: Int, repeat: Int, serve: Int = 0)

  val allScenarios: Seq[String] = Seq(
    "pipe-pingpong",
    "pipe-concurrent",
    "pipe-stream",
    "tcp-pingpong",
    "tcp-concurrent",
    "tcp-stream",
    "tcp-connect",
    "tcp-realistic",
    "tcp-realistic-bulk",
    "tcp-realistic-gather",
    "queue-concurrent",
    "queue-realistic",
    "queue-realistic-bulk",
    "work-flow",
    "work-iterator",
    "work-flow-long",
    "flow-mux",
    "flow-mux-futures",
    "flow-mux-pinned-futures",
    "flow-mux-shard-futures",
    "flow-mux-machine",
    "flow-mux-staged",
    "flow-mux-vm",
    "flow-mux-hand-loop",
    "flow-mux-hand-pinned",
    "flow-mux-hand-queue",
    "work-steps",
    "work-vm",
    "work-setup-steps",
    "work-setup-vm",
    "flow-mux-await",
    "flow-mux-machine-await",
    "flow-mux-staged-await",
    "work-realistic",
    "unix-pingpong",
    "unix-concurrent",
    "unix-stream",
    "unix-connect",
    "udp-pingpong",
    "udp-concurrent",
    "udp-connected",
    "udg-pingpong",
    "udg-concurrent",
    "fifo-pingpong",
    "file-read",
    "file-write",
    "file-concurrent",
    "file-open",
    "blocking",
    "blocking-concurrent",
    "resolve",
    "timer-zero",
    "timers"
  )

  def parse(args: Array[String]): (Int, String, Args) = {
    var workers = 1
    var policy = "affine"
    var scenarios = allScenarios
    var scale = 1.0
    var connections = 64
    var repeat = 1
    var serve = 0
    var i = 0
    while i < args.length do
      args(i) match
        case "--workers"     => workers = args(i + 1).toInt
        case "--policy"      => policy = args(i + 1)
        case "--scenarios"   => scenarios = args(i + 1).split(',').toSeq
        case "--scale"       => scale = args(i + 1).toDouble
        case "--connections" => connections = args(i + 1).toInt
        case "--repeat"      => repeat = args(i + 1).toInt
        case "--serve"       => serve = args(i + 1).toInt
        case other           => throw new IllegalArgumentException(other)
      i += 2
    (workers, policy, Args(scenarios, scale, connections, repeat, serve))
  }

  private inline val MessageSize = 64
  private inline val Chunk = 64 * 1024
  private inline val Block = 4096

  private val unique = new java.util.concurrent.atomic.AtomicInteger(0)
  private val tmp = s"/tmp/gcb-${System.nanoTime() % 100000000L}"
  private def path(kind: String): String = {
    BenchFiles.makeDirectory(tmp)
    s"$tmp/$kind-${unique.incrementAndGet()}"
  }
  private def remove(path: String): Unit = BenchFiles.remove(path)

  private def opening[K <: AnyRef](io: Io[Interface])(build: Slot[K] => io.Op)(using Async): K = {
    val into = Slot[K]()
    io.perform(build(into))
    into.clear()
  }

  // ---- byte streams, whatever they are over

  /** An echo of `rounds` messages from `in` to `out`, and its client: each message echoed back before the next. */
  /** How the servers of a scenario are run: a future each. */
  private final class Servers {
    def start[T <: Io[?]](io: T)(body: T => Async ?=> Unit)(using Async.Spawn): List[Future[Unit]] =
      List(Future(body(io)))
    def launch()(using Async.Spawn): List[Future[Unit]] = Nil
  }

  private def echoPair(io: Io[ByteStreams], rounds: Int, servers: Servers = new Servers)(
      serverIn: io.ops.Readable,
      serverOut: io.ops.Writable,
      clientIn: io.ops.Readable,
      clientOut: io.ops.Writable
  )(using Async.Spawn): List[Future[Unit]] = {
    val echo = servers.start[io.type](io) { io =>
      val buf = buffer(MessageSize)
      val read = io.ops.read(serverIn, buf)
      val write = io.ops.write(serverOut, buf)
      var i = 0
      while i < rounds do
        buf.clear()
        while buf.hasRemaining() do io.perform(read)
        buf.flip()
        while buf.hasRemaining() do io.perform(write)
        i += 1
    }
    val client = Future {
      val out = buffer(MessageSize)
      val in = buffer(MessageSize)
      val write = io.ops.write(clientOut, out)
      val read = io.ops.read(clientIn, in)
      var i = 0
      while i < rounds do
        out.clear()
        while out.hasRemaining() do io.perform(write)
        in.clear()
        while in.hasRemaining() do io.perform(read)
        i += 1
    }
    client :: echo
  }

  /** `chunks` chunks written to `out`, which `end` then ends, and read from `in` until its end. With GEARS_TRACE set,
    * how the two took turns is printed: one worker runs them, so the plain fields are safe there.
    */
  private def streaming(io: Io[ByteStreams], chunks: Int)(in: io.ops.Readable, out: io.ops.Writable)(
      end: Async ?=> Unit
  )(using Async.Spawn): List[Future[Unit]] = {
    var lastWasWrite = false
    var switches = 0L
    var writeOps = 0L
    var partialWrites = 0L
    var readOps = 0L
    var fullReads = 0L
    var readBytes = 0L
    var inFlight = 0L
    var maxInFlight = 0L
    var sumInFlightAtRead = 0L
    val writer = Future {
      val buf = buffer(Chunk)
      val write = io.ops.write(out, buf)
      var i = 0
      while i < chunks do
        buf.clear()
        while buf.hasRemaining() do
          val before = buf.position()
          io.perform(write)
          writeOps += 1
          inFlight += buf.position() - before
          if inFlight > maxInFlight then maxInFlight = inFlight
          if buf.hasRemaining() then partialWrites += 1
          if !lastWasWrite then { lastWasWrite = true; switches += 1 }
        i += 1
      end
    }
    val reader = Future {
      val buf = buffer(Chunk)
      val read = io.ops.read(in, buf)
      var ended = false
      while !ended do
        buf.clear()
        sumInFlightAtRead += inFlight
        io.perform(read)
        readOps += 1
        readBytes += buf.position()
        inFlight -= buf.position()
        if !buf.hasRemaining() then fullReads += 1
        if lastWasWrite then { lastWasWrite = false; switches += 1 }
        ended = buf.position() == 0
      if BenchFiles.environment("GEARS_TRACE") != null then
        System.err.println(
          s"stream: writes $writeOps (partial $partialWrites) reads $readOps (full $fullReads, mean ${readBytes / readOps} B) " +
            s"turn changes $switches, mean run ${(writeOps + readOps) / math.max(1, switches)} ops, " +
            s"in flight: max ${maxInFlight / 1024} KiB, mean as a read begins ${sumInFlightAtRead / readOps / 1024} KiB"
        )
    }
    List(writer, reader)
  }

  private def closeAll(io: Io[Handles], handles: Seq[io.ops.Handle])(using Async): Unit =
    handles.foreach(handle => io.perform(io.ops.close(handle)))

  private def pipe(io: Io[Pipes])(using Async): (io.ops.Readable, io.ops.Writable) = {
    val read = Slot[io.ops.Readable]()
    val write = Slot[io.ops.Writable]()
    io.perform(io.ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  private def pipeEcho(io: Io[Pipes], rounds: Int)(using Async.Spawn): List[Future[Unit]] = {
    val (requestIn, requestOut) = pipe(io)
    val (replyIn, replyOut) = pipe(io)
    val both = echoPair(io, rounds)(requestIn, replyOut, replyIn, requestOut)
    List(Future { both.foreach(_.await); closeAll(io, Seq(requestIn, requestOut, replyIn, replyOut)) })
  }

  // ---- stream sockets: `listen` and `connect` are the address family's

  private final class Sockets[I <: StreamSockets](val io: Io[I])(
      val listen: Slot[io.ops.Listener] => io.Op,
      val connect: io.ops.Listener => Slot[io.ops.Connection] => io.Op,
      val cleanup: () => Unit
  )

  private def tcp(io: Io[IpStreams]): Sockets[IpStreams] =
    new Sockets[IpStreams](io)(
      into => io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), into),
      listener => {
        val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
        into => io.ops.connect(address, into)
      },
      () => ()
    )

  private def unix(io: Io[UnixStreams]): Sockets[UnixStreams] = {
    val address = UnixAddress(path("sock"))
    new Sockets[UnixStreams](io)(
      into => io.ops.listen(address, into),
      _ => into => io.ops.connect(address, into),
      () => remove(address.path)
    )
  }

  /** `count` connections through one listener, each as (client end, server end). */
  private def connected(sockets: Sockets[? <: StreamSockets], count: Int)(using
      Async.Spawn
  ): (sockets.io.ops.Listener, List[(sockets.io.ops.Connection, sockets.io.ops.Connection)]) = {
    val io: sockets.io.type = sockets.io
    val listener = opening[io.ops.Listener](io)(sockets.listen)
    val connect = sockets.connect(listener)
    val accepted = Slot[io.ops.Connection]()
    val accept = io.ops.accept(listener, accepted)
    val pairs = List.fill(count) {
      val connecting = Future(opening[io.ops.Connection](io)(connect))
      io.perform(accept)
      (connecting.await, accepted.clear())
    }
    (listener, pairs)
  }

  private def socketEcho(
      sockets: Sockets[? <: StreamSockets],
      connections: Int,
      rounds: Int,
      name: String,
      servers: Servers = new Servers
  )(using Async.Spawn): Unit = {
    val io: sockets.io.type = sockets.io
    val (listener, pairs) = connected(sockets, connections)
    timed(name, rounds.toLong * connections)(
      pairs.flatMap((client, server) => echoPair(io, rounds, servers)(server, server, client, client)) ++ servers.launch()
    )
    closeAll(io, pairs.flatMap((client, server) => Seq(client, server)) :+ listener)
    sockets.cleanup()
  }

  // ---- a session cache that speaks Redis's protocol, and a client of it (see `io-bench/stacks/README.md`)

  /** RESP, the protocol of Redis: values written into arrays of bytes, and found in them. */
  private object Resp {
    private val Latin1 = java.nio.charset.StandardCharsets.ISO_8859_1

    def bytes(text: String): Array[Byte] = text.getBytes(Latin1)
    def text(from: Array[Byte], at: Int, length: Int): String = new String(from, at, length, Latin1)

    /** Writes `text` at `at`: where it ends. */
    def put(into: Array[Byte], at: Int, text: Array[Byte]): Int = {
      System.arraycopy(text, 0, into, at, text.length)
      at + text.length
    }

    /** Writes `number` in decimal and the end of a line. */
    def putLine(into: Array[Byte], at: Int, number: Int): Int = {
      var end = put(into, at, bytes(Integer.toString(number)))
      into(end) = '\r'
      into(end + 1) = '\n'
      end + 2
    }

    /** Writes a bulk string: `$`, its length, a line's end, its bytes, a line's end. */
    def putBulk(into: Array[Byte], at: Int, from: Array[Byte], start: Int, length: Int): Int = {
      into(at) = '$'
      val body = putLine(into, at + 1, length)
      System.arraycopy(from, start, into, body, length)
      into(body + length) = '\r'
      into(body + length + 1) = '\n'
      body + length + 2
    }

    /** Where the line that begins at `at` ends, past its `\r\n`; -1 if it has not all come. */
    def lineEnd(in: Array[Byte], at: Int, until: Int): Int = {
      var i = at
      while i + 1 < until do {
        if in(i) == '\r' && in(i + 1) == '\n' then return i + 2
        i += 1
      }
      -1
    }

    /** The decimal number from `at` to the end of its line. */
    def number(in: Array[Byte], at: Int): Int = {
      var i = at
      val negative = in(i) == '-'
      if negative then i += 1
      var value = 0
      while in(i) != '\r' do {
        value = value * 10 + (in(i) - '0')
        i += 1
      }
      if negative then -value else value
    }

    /** Where the value that begins at `at` ends; -1 if it has not all come. */
    def end(in: Array[Byte], at: Int, until: Int): Int = {
      val line = lineEnd(in, at, until)
      if line < 0 then -1
      else
        in(at) match {
          case '$' =>
            val length = number(in, at + 1)
            if length < 0 then line else if line + length + 2 <= until then line + length + 2 else -1
          case '*' =>
            var left = number(in, at + 1)
            var next = line
            while left > 0 && next >= 0 do {
              next = end(in, next, until)
              left -= 1
            }
            next
          case _ => line // a status, an error or an integer: its line
        }
    }
  }

  /** The cache: what it keeps for one connection, and how it answers `SET`, `GET` and `INCR`. */
  private final class CacheServer(
      values: java.util.Map[String, Array[Byte]] = new java.util.HashMap[String, Array[Byte]]()
  ) {
    private val Ok = Resp.bytes("+OK\r\n")
    private val Nothing = Resp.bytes("$-1\r\n")
    private val Unknown = Resp.bytes("-ERR unknown command\r\n")

    /** Answers the command that begins at `from` in `request`, an array of bulk strings, into `response` at `at`:
      * where the answer ends.
      */
    def handle(request: Array[Byte], from: Int, response: Array[Byte], at: Int): Int = {
      // *<n>\r\n $<l>\r\n <name>\r\n $<l>\r\n <key>\r\n [ $<l>\r\n <value>\r\n ]
      val until = request.length
      val parts = Resp.number(request, from + 1)
      var next = Resp.lineEnd(request, from, until)
      val nameLength = Resp.number(request, next + 1)
      val name = Resp.lineEnd(request, next, until)
      next = name + nameLength + 2
      val keyLength = Resp.number(request, next + 1)
      val keyAt = Resp.lineEnd(request, next, until)
      val key = Resp.text(request, keyAt, keyLength)
      next = keyAt + keyLength + 2
      if nameLength == 3 && request(name) == 'S' && parts == 3 then {
        val length = Resp.number(request, next + 1)
        val valueAt = Resp.lineEnd(request, next, until)
        values.put(key, java.util.Arrays.copyOfRange(request, valueAt, valueAt + length))
        Resp.put(response, at, Ok)
      } else if nameLength == 3 && request(name) == 'G' then {
        val value = values.get(key)
        if value == null then Resp.put(response, at, Nothing) else Resp.putBulk(response, at, value, 0, value.length)
      } else if nameLength == 4 && request(name) == 'I' then {
        // Kept as Redis keeps a count, its digits; counted in one step, for a table that connections share.
        val kept = values.compute(
          key,
          (_, held) => Resp.bytes(Integer.toString((if held == null then 0 else Resp.number(held, 0)) + 1) + "\r")
        )
        response(at) = ':'
        Resp.putLine(response, at + 1, Resp.number(kept, 0))
      } else Resp.put(response, at, Unknown)
    }
  }

  /** A client of the cache, for the user `id`: it keeps the user's sessions there, reads them back, and counts the
    * user's requests. What it asks follows from the request's number, and it knows what each answer must be.
    */
  private final class CacheClient(id: Int) {
    private val kept = new java.util.HashMap[String, Array[Byte]]()
    private var hits = 0
    private var expected: Array[Byte] | Null = null
    private val Set = Resp.bytes("*3\r\n$3\r\nSET\r\n")
    private val Get = Resp.bytes("*2\r\n$3\r\nGET\r\n")
    private val Incr = Resp.bytes("*2\r\n$4\r\nINCR\r\n")
    private val Ok = Resp.bytes("+OK\r\n")
    private val Nothing = Resp.bytes("$-1\r\n")

    /** A session as a program would keep one: a document of JSON, some 500 bytes of it. */
    private def session(i: Int): Array[Byte] = {
      val text = new java.lang.StringBuilder(640)
      text.append("{\"user\":").append(id).append(",\"seq\":").append(i)
      text.append(",\"token\":\"").append(Integer.toHexString(id * 0x9e3779b1 + i)).append("-")
      text.append(Integer.toHexString(i * 0x85ebca6b + id)).append("\",\"expires\":").append(1795000000L + i)
      text.append(",\"cart\":[")
      var item = 0
      while item < 8 do {
        if item > 0 then text.append(',')
        text.append("{\"sku\":\"SKU-").append(100000 + (id * 131 + i * 17 + item * 7919) % 900000)
        text.append("\",\"qty\":").append(1 + (i + item) % 5).append(",\"price\":").append(199 + (i * 37 + item * 101) % 9800)
        text.append('}')
        item += 1
      }
      text.append("],\"locale\":\"en-GB\",\"theme\":\"").append(if i % 2 == 0 then "dark" else "light").append("\"}")
      Resp.bytes(text.toString)
    }

    /** What the answer to the last request written must be. */
    def answer: Array[Byte] = expected.nn

    /** Writes request `i` into `request` at `at`: where it ends. One in five keeps a session, one in five counts a
      * request, and the rest read a session back.
      */
    def request(i: Int, request: Array[Byte], at: Int): Int =
      if i % 5 == 3 then {
        hits += 1
        expected = Resp.bytes(":" + hits + "\r\n")
        val key = Resp.bytes("hits:" + id)
        Resp.putBulk(request, Resp.put(request, at, Incr), key, 0, key.length)
      } else {
        val name = "session:" + id + ":" + (i * 7 % 50)
        val key = Resp.bytes(name)
        if i % 5 == 0 then {
          val document = session(i)
          kept.put(name, document)
          expected = Ok
          val value = Resp.putBulk(request, Resp.put(request, at, Set), key, 0, key.length)
          Resp.putBulk(request, value, document, 0, document.length)
        } else {
          val document = kept.get(name)
          expected =
            if document == null then Nothing
            else {
              val whole = new Array[Byte](document.length + 16)
              java.util.Arrays.copyOf(whole, Resp.putBulk(whole, 0, document, 0, document.length))
            }
          Resp.putBulk(request, Resp.put(request, at, Get), key, 0, key.length)
        }
      }

    /** Fails unless the bytes of `response` from `from` to `until` are `right`, the answer to request `i`. */
    def check(i: Int, right: Array[Byte], response: Array[Byte], from: Int, until: Int): Unit = {
      var same = right.length == until - from
      var at = 0
      while same && at < right.length do {
        same = right(at) == response(from + at)
        at += 1
      }
      if !same then throw new IllegalStateException(s"request $i of client $id was answered wrongly")
    }
  }

  private inline val LargestValue = 2048

  /** How many requests go at once in `tcp-realistic-bulk`, and room for them and for their answers. */
  private inline val Batch = 16
  private inline val LargestBatch = Batch * LargestValue

  /** One end of a connection that carries RESP values. */
  private final class RespEnd(val io: Io[ByteStreams], size: Int)(in: io.ops.Readable, out: io.ops.Writable) {
    private val buf = arrayBuffer(size)
    private val read = io.ops.read(in, buf)
    private val write = io.ops.write(out, buf)

    def send(values: Array[Byte], length: Int)(using Async): Unit = {
      buf.clear()
      buf.put(values, 0, length)
      buf.flip()
      while buf.hasRemaining() do io.perform(write)
    }

    /** Reads once, whatever has come, into `into` at `have`: how many bytes. */
    def more(into: Array[Byte], have: Int)(using Async): Int = {
      buf.clear()
      if buf.remaining() > into.length - have then buf.limit(into.length - have)
      io.perform(read)
      buf.flip()
      val count = buf.remaining()
      if count == 0 then throw new java.io.EOFException("The connection ended within a value")
      buf.get(into, have, count)
      count
    }

    /** Receives one value into `value`, however many reads it comes in: how long it is. */
    def receive(value: Array[Byte])(using Async): Int = {
      var have = 0
      var end = -1
      while end < 0 do {
        have += more(value, have)
        end = Resp.end(value, 0, have)
      }
      end
    }
  }

  private def realistic(
      sockets: Sockets[? <: StreamSockets],
      connections: Int,
      rounds: Int,
      name: String,
      servers: Servers = new Servers
  )(using Async.Spawn): Unit = {
    val io: sockets.io.type = sockets.io
    val (listener, pairs) = connected(sockets, connections)
    timed(name, rounds.toLong * connections)(pairs.zipWithIndex.flatMap { case ((client, server), id) =>
      val serving = servers.start[io.type](io) { io =>
        val end = new RespEnd(io, LargestValue)(server, server)
        val cache = new CacheServer
        val request = new Array[Byte](LargestValue)
        val response = new Array[Byte](LargestValue)
        var i = 0
        while i < rounds do {
          end.receive(request): Unit
          end.send(response, cache.handle(request, 0, response, 0))
          i += 1
        }
      }
      val asking = Future {
        val end = new RespEnd(io, LargestValue)(client, client)
        val user = new CacheClient(id)
        val request = new Array[Byte](LargestValue)
        val response = new Array[Byte](LargestValue)
        var i = 0
        while i < rounds do {
          end.send(request, user.request(i, request, 0))
          user.check(i, user.answer, response, 0, end.receive(response))
          i += 1
        }
      }
      asking :: serving
    } ++ servers.launch())
    closeAll(io, pairs.flatMap((client, server) => Seq(client, server)) :+ listener)
    sockets.cleanup()
  }

  /** The cache as `tcp-realistic-gather` keeps it: a value is a buffer of its own, and an answer is buffers, a line
    * for its length, the value's own buffer, and a line's end, for a gathering write to send with no copy of the value.
    */
  private final class GatheringCache {
    private val values = new java.util.HashMap[String, ByteBuffer]()
    private val counts = new java.util.HashMap[String, Integer]()
    private val scratch = new Array[Byte](16)

    private def fixed(text: String): ByteBuffer = {
      val buf = arrayBuffer(text.length)
      buf.put(Resp.bytes(text))
      buf.flip()
      buf
    }
    private val Ok = fixed("+OK\r\n")
    private val Nothing = fixed("$-1\r\n")
    private val LineEnd = fixed("\r\n")
    private val Unknown = fixed("-ERR unknown command\r\n")

    /** A line of `mark` and `number`, written into `heads`: the part of `heads` that holds it. */
    private def line(mark: Char, number: Int, heads: ByteBuffer): ByteBuffer = {
      scratch(0) = mark.toByte
      val length = Resp.putLine(scratch, 1, number)
      val part = heads.duplicate()
      heads.put(scratch, 0, length)
      part.limit(heads.position())
      part
    }

    /** Answers the command that begins at `from` in `request`, whose bytes are those of `source` at the same places,
      * with buffers put into `out` from `count`: how many there are then. Lines made for the answer go into `heads`.
      */
    def handle(
        request: Array[Byte],
        from: Int,
        source: ByteBuffer,
        heads: ByteBuffer,
        out: Array[ByteBuffer],
        count: Int
    ): Int = {
      val until = request.length
      val parts = Resp.number(request, from + 1)
      var next = Resp.lineEnd(request, from, until)
      val nameLength = Resp.number(request, next + 1)
      val name = Resp.lineEnd(request, next, until)
      next = name + nameLength + 2
      val keyLength = Resp.number(request, next + 1)
      val keyAt = Resp.lineEnd(request, next, until)
      val key = Resp.text(request, keyAt, keyLength)
      next = keyAt + keyLength + 2
      if nameLength == 3 && request(name) == 'S' && parts == 3 then {
        val length = Resp.number(request, next + 1)
        val valueAt = Resp.lineEnd(request, next, until)
        // The value goes from the buffer it was read into to one of its own, buffer to buffer. The last one kept
        // under the key is used again if it is large enough: nothing is being written from it by now.
        val held = values.get(key)
        val kept = if held != null && held.capacity() >= length then held else arrayBuffer(length)
        kept.clear()
        val value = source.duplicate()
        value.limit(valueAt + length)
        value.position(valueAt)
        kept.put(value)
        kept.flip()
        values.put(key, kept)
        out(count) = Ok.duplicate()
        count + 1
      } else if nameLength == 3 && request(name) == 'G' then {
        val value = values.get(key)
        if value == null then {
          out(count) = Nothing.duplicate()
          count + 1
        } else {
          out(count) = line('$', value.remaining(), heads)
          out(count + 1) = value.duplicate()
          out(count + 2) = LineEnd.duplicate()
          count + 3
        }
      } else if nameLength == 4 && request(name) == 'I' then {
        val held = counts.get(key)
        val now = (if held == null then 0 else held.intValue) + 1
        counts.put(key, now)
        out(count) = line(':', now, heads)
        count + 1
      } else {
        out(count) = Unknown.duplicate()
        count + 1
      }
    }
  }

  private val NoBytes: ByteBuffer = ByteBuffer.allocate(0)

  /** Serves a connection of `tcp-realistic-gather`: every request a read brought is answered in one gathering write,
    * of buffers that are the cache's own.
    */
  private def serveGathering(io: Io[ByteStreams], rounds: Int)(in: io.ops.Readable, out: io.ops.Writable)(using
      Async
  ): Unit = {
    val cache = new GatheringCache
    val requests = arrayBuffer(LargestBatch)
    val mirror = new Array[Byte](LargestBatch) // the same bytes, where the protocol is read
    val read = io.ops.read(in, requests)
    val heads = arrayBuffer(Batch * 16)
    val answers = Array.fill[ByteBuffer](Batch * 3)(NoBytes)
    val write = io.ops.write(out, answers)
    var have = 0
    var answered = 0
    while answered < rounds do {
      requests.limit(requests.capacity())
      requests.position(have)
      io.perform(read)
      val now = requests.position()
      if now == have then throw new java.io.EOFException("The connection ended within a request")
      val came = requests.duplicate()
      came.limit(now)
      came.position(have)
      came.get(mirror, have, now - have)
      have = now
      var from = 0
      var count = 0
      var next = Resp.end(mirror, from, have)
      while next >= 0 && count + 3 <= answers.length do {
        count = cache.handle(mirror, from, requests, heads, answers, count)
        answered += 1
        from = next
        next = Resp.end(mirror, from, have)
      }
      // What is left is the beginning of a request, the rest of which has yet to come: kept in both.
      if from > 0 then {
        System.arraycopy(mirror, from, mirror, 0, have - from)
        requests.limit(have)
        requests.position(from)
        requests.compact()
        have -= from
      }
      var i = count
      while i < answers.length do {
        answers(i) = NoBytes
        i += 1
      }
      while answers(count - 1).hasRemaining() do io.perform(write)
      heads.clear()
    }
  }

  /** As `realistic`, the requests sent `Batch` at once, as a client that pipelines sends them: the server answers every
    * request a read brought, in one write, and the client checks every answer a read brought. A wait for each read
    * there is, not for each request.
    */
  private def realisticBulk(
      sockets: Sockets[? <: StreamSockets],
      connections: Int,
      rounds: Int,
      name: String,
      gathering: Boolean,
      servers: Servers = new Servers
  )(using Async.Spawn): Unit = {
    val io: sockets.io.type = sockets.io
    val (listener, pairs) = connected(sockets, connections)
    timed(name, rounds.toLong * connections)(pairs.zipWithIndex.flatMap { case ((client, server), id) =>
      val serving =
        if gathering then List(Future(serveGathering(io, rounds)(server, server)))
        else
          servers.start[io.type](io) { io =>
            val end = new RespEnd(io, LargestBatch)(server, server)
            val cache = new CacheServer
            val requests = new Array[Byte](LargestBatch)
            val responses = new Array[Byte](LargestBatch)
            var have = 0
            var answered = 0
            while answered < rounds do {
              have += end.more(requests, have)
              var from = 0
              var written = 0
              var next = Resp.end(requests, from, have)
              while next >= 0 do {
                written = cache.handle(requests, from, responses, written)
                answered += 1
                from = next
                next = Resp.end(requests, from, have)
              }
              // What is left is the beginning of a request, the rest of which has yet to come.
              if from > 0 then {
                System.arraycopy(requests, from, requests, 0, have - from)
                have -= from
              }
              if written > 0 then end.send(responses, written)
            }
          }
      val asking = Future {
        val end = new RespEnd(io, LargestBatch)(client, client)
        val user = new CacheClient(id)
        val requests = new Array[Byte](LargestBatch)
        val responses = new Array[Byte](LargestBatch)
        val answers = new Array[Array[Byte]](Batch)
        var i = 0
        while i < rounds do {
          val batch = math.min(Batch, rounds - i)
          var length = 0
          var k = 0
          while k < batch do {
            length = user.request(i + k, requests, length)
            answers(k) = user.answer
            k += 1
          }
          end.send(requests, length)
          var have = 0
          var from = 0
          var checked = 0
          while checked < batch do {
            have += end.more(responses, have)
            var next = Resp.end(responses, from, have)
            while next >= 0 && checked < batch do {
              user.check(i + checked, answers(checked), responses, from, next)
              checked += 1
              from = next
              next = Resp.end(responses, from, have)
            }
          }
          i += batch
        }
      }
      asking :: serving
    } ++ servers.launch())
    closeAll(io, pairs.flatMap((client, server) => Seq(client, server)) :+ listener)
    sockets.cleanup()
  }

  /** The cache as a server of its own, for a client that is not this program's, `redis-benchmark` say: it listens on
    * `port`, and serves every connection that comes, answering every request a read brought in one write, until the
    * connection ends. Its table is one for every connection, as Redis's is. It never returns.
    */
  private def serveCache(io: Io[IpStreams], port: Int)(using Async.Spawn): Unit = {
    val table = new java.util.concurrent.ConcurrentHashMap[String, Array[Byte]]()
    val listener =
      opening[io.ops.Listener](io)(into => io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, port), into))
    println(s"serving on 127.0.0.1:$port")
    val accepted = Slot[io.ops.Connection]()
    val accept = io.ops.accept(listener, accepted)
    while true do {
      io.perform(accept)
      val connection = accepted.clear()
      Future {
        try {
          val end = new RespEnd(io, LargestBatch)(connection, connection)
          val cache = new CacheServer(table)
          val requests = new Array[Byte](LargestBatch)
          val responses = new Array[Byte](LargestBatch)
          var have = 0
          while true do {
            have += end.more(requests, have)
            var from = 0
            var written = 0
            var next = Resp.end(requests, from, have)
            // An answer is no longer than its request, but for a `GET`'s, which is a value: room is kept for one.
            while next >= 0 && written + LargestValue <= responses.length do {
              written = cache.handle(requests, from, responses, written)
              from = next
              next = Resp.end(requests, from, have)
            }
            if from > 0 then {
              System.arraycopy(requests, from, requests, 0, have - from)
              have -= from
            }
            if written > 0 then end.send(responses, written)
          }
        } catch case _: java.io.IOException => () // the client has gone
        finally
          try io.perform(io.ops.close(connection))
          catch case _: java.io.IOException => ()
      }
    }
  }

  // ---- one future serving every connection, through a queue of ops (`SubmissionQueues`)

  /** `count` connections over TCP, through one listener, each as (client end, server end). */
  private def connectedTcp(io: Io[IpStreams], count: Int)(using
      Async.Spawn
  ): (io.ops.Listener, List[(io.ops.Connection, io.ops.Connection)]) = {
    val listener =
      opening[io.ops.Listener](io)(into => io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), into))
    val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
    val accepted = Slot[io.ops.Connection]()
    val accept = io.ops.accept(listener, accepted)
    val pairs = List.fill(count) {
      val connecting = Future(opening[io.ops.Connection](io)(into => io.ops.connect(address, into)))
      io.perform(accept)
      (connecting.await, accepted.clear())
    }
    (listener, pairs)
  }

  /** What the queue's server keeps for a connection: its buffer, the read and the write of it, and how much is left. */
  private final class Served[R, W](val buf: java.nio.ByteBuffer, val read: R, val write: W, var left: Long) {
    val cache = new CacheServer
    val requests = new Array[Byte](LargestBatch)
    val responses = new Array[Byte](LargestBatch)
    var have = 0
  }

  /** `tcp-concurrent` with its server one future: a read is in flight on every connection, what one brings is written
    * back by an op that is most often done as it is started, and the future waits once for however many reads a turn
    * of the backend completes. The clients are `tcp-concurrent`'s, a future each.
    */
  private def queueEcho(io: Io[IpStreams & SubmissionQueues], connections: Int, rounds: Int, name: String)(using
      Async.Spawn
  ): Unit = {
    import SubmissionQueues.performQueue
    val (listener, pairs) = connectedTcp(io, connections)
    timed(name, rounds.toLong * connections) {
      val shards = io.queueShards
      val serving = (0 until shards).toList.map { shard => Future {
        val ops = new SubmissionQueue[io.Op](shard)
        val completed = new CompletionQueue[io.Op]
        var open = 0
        for ((_, server), index) <- pairs.zipWithIndex if index % shards == shard do {
          val buf = buffer(MessageSize)
          val served = new Served(buf, io.ops.read(server, buf), io.ops.write(server, buf), rounds.toLong * MessageSize)
          ops.submit(served.read, served)
          open += 1
        }
        var waits = 0L
        var taken = 0L
        while open > 0 do {
          io.performQueue(ops, completed)
          waits += 1
          taken += completed.count
          var i = 0
          while i < completed.count do {
            val failure = completed.failure(i)
            if failure != null then throw failure
            val served = completed.tag(i).asInstanceOf[Served[io.Op, io.Op]]
            val buf = served.buf
            if completed.op(i).asInstanceOf[AnyRef] eq served.read.asInstanceOf[AnyRef] then {
              served.left -= buf.position()
              buf.flip()
              ops.submit(served.write, served)
            } else if buf.hasRemaining() then ops.submit(served.write, served)
            else if served.left > 0 then {
              buf.clear()
              ops.submit(served.read, served)
            } else open -= 1
            i += 1
          }
          completed.clear()
        }
        if BenchFiles.environment("GEARS_TRACE") != null then
          System.err.println(s"queue: $waits calls of performQueue, $taken completions, ${taken.toDouble / waits} each")
      } }
      val asking = pairs.map { (client, _) =>
        Future {
          val out = buffer(MessageSize)
          val in = buffer(MessageSize)
          val write = io.ops.write(client, out)
          val read = io.ops.read(client, in)
          var i = 0
          while i < rounds do {
            out.clear()
            while out.hasRemaining() do io.perform(write)
            in.clear()
            while in.hasRemaining() do io.perform(read)
            i += 1
          }
        }
      }
      serving ++ asking
    }
    closeAll(io, pairs.flatMap((client, server) => Seq(client, server)) :+ listener)
  }

  /** A frame of `flow-mux`'s, its bytes its own. */
  private final class MuxFrame(val stream: Int, val bytes: Array[Byte])

  /** The session cache with its connection carrying `Batch` streams, as HTTP/2's does: a request and its answer are
    * frames, a stream's number, a length, and the bytes, and the client sends a request on every stream at once. The
    * server is written with `Server`, `Flow` and `Sink`: a handler that is run, a connection's bytes decoded into
    * frames, dealt by stream, each stream's flow answering into the connection's sink; or, `staged`, a pipeline of
    * stages that is stepped. A frame's bytes are an array of its own.
    */
  private def flowMux(
      io: Io[IpStreams],
      connections: Int,
      rounds: Int,
      name: String,
      strategy: Server.Strategy,
      /** None for a handler that is run; else a pipeline of stages, served by the machine or, false, by futures. */
      staged: Option[Boolean] = None,
      /** Whether each request is answered by work that waits: a future of its own, as a call elsewhere would be. */
      waits: Boolean = false,
      /** The pipeline as a program made once (`Pipe`), stepped by the machine. */
      vm: Boolean = false,
      /** The same server with no flow in it, written out over `perform`: "loop", a future for each connection that
        * reads, answers and writes in a loop; "pinned", that with a connection's ops on one shard; "queue", a future
        * for each shard that performs a submission queue.
        */
      hand: String | Null = null
  )(using Async.Spawn): Unit = {
    if hand == "pinned" && !io.ops.isInstanceOf[Shards] then return
    if hand == "queue" && !io.ops.isInstanceOf[SubmissionQueues] then return
    // A strategy the backend has not is no scenario of its.
    if strategy != Server.Strategy.Auto && !Server.strategies(io).contains(strategy) then return
    val Frame = 4
    val Most = LargestBatch + Frame * Batch
    def length(in: Array[Byte], at: Int): Int = ((in(at + 2) & 0xff) << 8) | (in(at + 3) & 0xff)
    def head(in: Array[Byte], at: Int, stream: Int, length: Int): Unit = {
      in(at) = stream.toByte
      in(at + 1) = 0
      in(at + 2) = (length >> 8).toByte
      in(at + 3) = length.toByte
    }
    /** Bytes as they come into frames: what is left of one read begins the next frame. */
    def frames(bytes: Flow[ByteBuffer]): Flow[MuxFrame] = Flow {
      val kept = new Array[Byte](2 * Most)
      var have = 0
      for chunk <- bytes do {
        val came = chunk.remaining()
        chunk.get(kept, have, came)
        have += came
        var from = 0
        while have - from >= Frame && have - from >= Frame + length(kept, from) do {
          val size = length(kept, from)
          Flow.emit(new MuxFrame(kept(from), java.util.Arrays.copyOfRange(kept, from + Frame, from + Frame + size)))
          from += Frame + size
        }
        if from > 0 then {
          System.arraycopy(kept, from, kept, 0, have - from)
          have -= from
        }
      }
    }

    val paired = if hand != null then Some(connectedTcp(io, connections)) else None
    val server =
      if hand != null then null
      else
        Server.tcp(
          IpSocketAddress(IpAddress.IPv4Loopback, 0),
          Server.Sizes(read = Most, write = Most, buffers = buffer),
          strategy
        )(using io)
    if server != null && BenchFiles.environment("GEARS_TRACE") != null then System.err.println(s"$name: ${server.strategy}")
    val address = if server != null then server.address else null

    /** What a read brought, joined to what was kept at `kept`: every whole frame in it answered into `responses`,
      * what is left of the last kept. How much was kept, and how much was answered, as `kept << 32 | answered`.
      */
    def answerAll(cache: CacheServer, kept: Array[Byte], held: Int, responses: Array[Byte]): Long = {
      var have = held
      var from = 0
      var written = 0
      while have - from >= Frame && have - from >= Frame + length(kept, from) do {
        val size = length(kept, from)
        val end = cache.handle(kept, from + Frame, responses, written + Frame)
        head(responses, written, kept(from), end - written - Frame)
        written = end
        from += Frame + size
      }
      if from > 0 then {
        System.arraycopy(kept, from, kept, 0, have - from)
        have -= from
      }
      (have.toLong << 32) | written
    }

    /** A connection served by a loop of its own: read, answer what came, write it, again, until the peer ends. */
    def serveInLoop(on: Io[io.ops.type], connection: io.ops.Connection)(using Async): Unit = {
      val cache = new CacheServer
      val in = buffer(Most)
      val read = on.ops.read(connection, in)
      val out = buffer(2 * Most)
      val write = on.ops.write(connection, out)
      val kept = new Array[Byte](2 * Most)
      val responses = new Array[Byte](2 * Most)
      var have = 0
      var open = true
      while open do {
        in.clear()
        on.perform(read)
        in.flip()
        val came = in.remaining()
        if came == 0 then open = false
        else {
          in.get(kept, have, came)
          val both = answerAll(cache, kept, have + came, responses)
          have = (both >>> 32).toInt
          val written = both.toInt
          if written > 0 then {
            out.clear()
            out.put(responses, 0, written)
            out.flip()
            while out.hasRemaining() do on.perform(write)
          }
        }
      }
    }

    final class ByQueue(val connection: io.ops.Connection) {
      val cache = new CacheServer
      val in: ByteBuffer = buffer(Most)
      val read = io.ops.read(connection, in)
      val out: ByteBuffer = buffer(2 * Most)
      val write = io.ops.write(connection, out)
      val kept = new Array[Byte](2 * Most)
      val responses = new Array[Byte](2 * Most)
      var have = 0
    }

    /** A shard's connections served by one future and a submission queue: the machine, written out for this server. */
    def serveByQueue(shard: Int, connections: List[io.ops.Connection])(using Async): Unit = {
      val ops = new SubmissionQueue[io.Op](shard)
      val completed = new CompletionQueue[io.Op]
      var open = 0
      for connection <- connections do {
        val served = new ByQueue(connection)
        ops.submit(served.read, served)
        open += 1
      }
      while open > 0 do {
        io.performSubmitted(ops, completed)
        var i = 0
        while i < completed.count do {
          val failure = completed.failure(i)
          if failure != null then throw failure
          val served = completed.tag(i).asInstanceOf[ByQueue]
          if completed.op(i).asInstanceOf[AnyRef] eq served.read.asInstanceOf[AnyRef] then {
            served.in.flip()
            val came = served.in.remaining()
            if came == 0 then open -= 1
            else {
              served.in.get(served.kept, served.have, came)
              val both = answerAll(served.cache, served.kept, served.have + came, served.responses)
              served.have = (both >>> 32).toInt
              val written = both.toInt
              served.in.clear()
              if written > 0 then {
                served.out.clear()
                served.out.put(served.responses, 0, written)
                served.out.flip()
                ops.submit(served.write, served)
              } else ops.submit(served.read, served)
            }
          } else if served.out.hasRemaining() then ops.submit(served.write, served)
          else ops.submit(served.read, served)
          i += 1
        }
        completed.clear()
      }
    }
    /** `frames` as a stage that is stepped: the same loop, called with each chunk as it comes. */
    def decoding(emit: MuxFrame => Unit): ByteBuffer => Unit = {
      val kept = new Array[Byte](2 * Most)
      var have = 0
      chunk => {
        val came = chunk.remaining()
        chunk.get(kept, have, came)
        have += came
        var from = 0
        while have - from >= Frame && have - from >= Frame + length(kept, from) do {
          val size = length(kept, from)
          emit(new MuxFrame(kept(from), java.util.Arrays.copyOfRange(kept, from + Frame, from + Frame + size)))
          from += Frame + size
        }
        if from > 0 then {
          System.arraycopy(kept, from, kept, 0, have - from)
          have -= from
        }
      }
    }
    val serving = Future {
      if hand != null then {
        val servers = paired.get._2.map(_._2)
        val shards = if io.ops.isInstanceOf[Shards] then io.submissionShards else 1
        if hand == "queue" then
          (0 until shards).toList
            .map(shard => Future(serveByQueue(shard, servers.zipWithIndex.collect { case (c, at) if at % shards == shard => c })))
            .foreach(_.await)
        else
          servers.zipWithIndex.map { (connection, at) =>
            val on: Io[io.ops.type] =
              if hand == "pinned" then Shards.onShard(io.asInstanceOf[Io[Shards]])(at % shards).asInstanceOf[Io[io.ops.type]]
              else io.asInstanceOf[Io[io.ops.type]]
            Future(serveInLoop(on, connection))
          }.foreach(_.await)
        closeAll(io, servers :+ paired.get._1)
      } else if vm then
        server.serveProgram(
          Pipe
            .local[CacheServer, ByteBuffer](() => new CacheServer)
            .process[MuxFrame](decoding)
            .groupThrough(_.stream)(_.mapWith[CacheServer, ByteBuffer] { cache =>
              val response = new Array[Byte](LargestValue + Frame)
              val out = ByteBuffer.wrap(response)
              request => {
                val end = cache.handle(request.bytes, 0, response, Frame)
                head(response, 0, request.stream, end - Frame)
                out.clear().limit(end)
              }
            })
            .program
        )
      else if staged.isDefined then
        // No handler is run: a connection's pipeline is described once, and its stages called as its bytes come, one
        // chunk after another on one thread, so what its streams share needs no lock.
        server.serveFlow(
          connection => {
            val cache = new CacheServer
            connection.bytes.process[MuxFrame](decoding).groupThrough(_.stream) { (stream, requests) =>
              val response = new Array[Byte](LargestValue + Frame)
              val out = ByteBuffer.wrap(response)
              if waits then
                // One at a time for a stream, so its buffer is its own; beside the other streams', so the cache they
                // share is locked.
                requests.mapAsync(1) { request =>
                  val end = cache.synchronized(cache.handle(request.bytes, 0, response, Frame))
                  head(response, 0, stream, end - Frame)
                  out.clear().limit(end)
                }
              else
                requests.map { request =>
                  val end = cache.handle(request.bytes, 0, response, Frame)
                  head(response, 0, stream, end - Frame)
                  out.clear().limit(end)
                }
            }
          },
          machine = staged.get
        )
      else server.serve { connection =>
        val cache = new CacheServer
        val guard = new gears.async.Semaphore(1)
        frames(connection.bytes).groupBy(_.stream).foreachPar { (stream, requests) =>
          val response = new Array[Byte](LargestValue + Frame)
          val out = ByteBuffer.wrap(response)
          for request <- requests do {
            // The handler does not know how it is served: made of futures, a connection's streams are on any
            // thread, so the cache they share is locked, with a lock that awaits and is no thread's.
            def answer()(using Async): Int = {
              val held = guard.acquire()
              try cache.handle(request.bytes, 0, response, Frame)
              finally held.release()
            }
            val end = if waits then Async.group(Future(answer()).await) else answer()
            head(response, 0, stream, end - Frame)
            connection.send(out.clear().limit(end))
          }
        }
      }
    }
    val clients =
      if hand != null then paired.get._2.map(_._1)
      else List.tabulate(connections)(_ => opening[io.ops.Connection](io)(into => io.ops.connect(address.nn, into)))
    timed(name, rounds.toLong * connections) {
      clients.zipWithIndex.map { (client, id) =>
        Future {
          val end = new RespEnd(io, Most)(client, client)
          // A client for each stream, with keys of its own: streams are answered in no order among themselves.
          val users = Array.tabulate(Batch)(k => new CacheClient(id * Batch + k))
          val requests = new Array[Byte](Most)
          val responses = new Array[Byte](2 * Most)
          val answers = new Array[Array[Byte]](Batch)
          var i = 0
          while i < rounds do {
            val batch = math.min(Batch, rounds - i)
            var size = 0
            var k = 0
            while k < batch do {
              val start = size
              size = users(k).request(i / Batch, requests, start + Frame)
              head(requests, start, k, size - start - Frame)
              answers(k) = users(k).answer
              k += 1
            }
            end.send(requests, size)
            var have = 0
            var from = 0
            var checked = 0
            while checked < batch do {
              have += end.more(responses, have)
              while have - from >= Frame && have - from >= Frame + length(responses, from) do {
                val stream = responses(from)
                val until = from + Frame + length(responses, from)
                users(stream).check(i / Batch, answers(stream), responses, from + Frame, until)
                from = until
                checked += 1
              }
            }
            i += batch
          }
        }
      }
    }
    closeAll(io, clients)
    if server != null then server.close()
    serving.await
  }

  /** `tcp-realistic`, or with `together` `tcp-realistic-bulk`, with its server one future over a queue of ops, as
    * `queueEcho`'s is: each connection's requests are answered from what the server keeps for it.
    */
  private def queueRealistic(
      io: Io[IpStreams & SubmissionQueues],
      connections: Int,
      rounds: Int,
      name: String,
      together: Boolean
  )(using Async.Spawn): Unit = {
    import SubmissionQueues.performQueue
    val (listener, pairs) = connectedTcp(io, connections)
    timed(name, rounds.toLong * connections) {
      val shards = io.queueShards
      val serving = (0 until shards).toList.map { shard => Future {
        val ops = new SubmissionQueue[io.Op](shard)
        val completed = new CompletionQueue[io.Op]
        var open = 0
        for ((_, server), index) <- pairs.zipWithIndex if index % shards == shard do {
          val buf = arrayBuffer(LargestBatch)
          val served = new Served(buf, io.ops.read(server, buf), io.ops.write(server, buf), rounds.toLong)
          ops.submit(served.read, served)
          open += 1
        }
        while open > 0 do {
          io.performQueue(ops, completed)
          var i = 0
          while i < completed.count do {
            val failure = completed.failure(i)
            if failure != null then throw failure
            val served = completed.tag(i).asInstanceOf[Served[io.Op, io.Op]]
            val buf = served.buf
            if completed.op(i).asInstanceOf[AnyRef] eq served.read.asInstanceOf[AnyRef] then {
              // What the read brought joins what was kept, and every whole request in it is answered.
              buf.flip()
              val came = buf.remaining()
              if came == 0 then throw new java.io.EOFException("The connection ended within a request")
              buf.get(served.requests, served.have, came)
              var have = served.have + came
              var from = 0
              var written = 0
              var next = Resp.end(served.requests, from, have)
              while next >= 0 do {
                written = served.cache.handle(served.requests, from, served.responses, written)
                served.left -= 1
                from = next
                next = Resp.end(served.requests, from, have)
              }
              if from > 0 then {
                System.arraycopy(served.requests, from, served.requests, 0, have - from)
                have -= from
              }
              served.have = have
              buf.clear()
              if written > 0 then {
                buf.put(served.responses, 0, written)
                buf.flip()
                ops.submit(served.write, served)
              } else ops.submit(served.read, served)
            } else if buf.hasRemaining() then ops.submit(served.write, served)
            else if served.left > 0 then {
              buf.clear()
              ops.submit(served.read, served)
            } else open -= 1
            i += 1
          }
          completed.clear()
        }
      } }
      val asking = pairs.zipWithIndex.map { case ((client, _), id) =>
        Future {
          if together then askTogether(io, id, rounds)(client)
          else {
            val end = new RespEnd(io, LargestValue)(client, client)
            val user = new CacheClient(id)
            val request = new Array[Byte](LargestValue)
            val response = new Array[Byte](LargestValue)
            var i = 0
            while i < rounds do {
              end.send(request, user.request(i, request, 0))
              user.check(i, user.answer, response, 0, end.receive(response))
              i += 1
            }
          }
        }
      }
      serving ++ asking
    }
    closeAll(io, pairs.flatMap((client, server) => Seq(client, server)) :+ listener)
  }

  /** The client of `tcp-realistic-bulk`: its requests sent `Batch` at once, their answers checked as they come. */
  private def askTogether(io: Io[ByteStreams], id: Int, rounds: Int)(client: io.ops.Readable & io.ops.Writable)(using
      Async
  ): Unit = {
    val end = new RespEnd(io, LargestBatch)(client, client)
    val user = new CacheClient(id)
    val requests = new Array[Byte](LargestBatch)
    val responses = new Array[Byte](LargestBatch)
    val answers = new Array[Array[Byte]](Batch)
    var i = 0
    while i < rounds do {
      val batch = math.min(Batch, rounds - i)
      var length = 0
      var k = 0
      while k < batch do {
        length = user.request(i + k, requests, length)
        answers(k) = user.answer
        k += 1
      }
      end.send(requests, length)
      var have = 0
      var from = 0
      var checked = 0
      while checked < batch do {
        have += end.more(responses, have)
        var next = Resp.end(responses, from, have)
        while next >= 0 && checked < batch do {
          user.check(i + checked, answers(checked), responses, from, next)
          checked += 1
          from = next
          next = Resp.end(responses, from, have)
        }
      }
      i += batch
    }
  }

  /** The work of `tcp-realistic` with nothing between the two sides: what the scenario costs beside its I/O. */
  private def realisticWork(connections: Int, rounds: Int)(using Async.Spawn): Future[Unit] = Future {
    val request = new Array[Byte](LargestValue)
    val response = new Array[Byte](LargestValue)
    var id = 0
    while id < connections do {
      val cache = new CacheServer
      val user = new CacheClient(id)
      var i = 0
      while i < rounds do {
        user.request(i, request, 0): Unit
        user.check(i, user.answer, response, 0, cache.handle(request, 0, response, 0))
        i += 1
      }
      id += 1
    }
  }

  private def socketStream(sockets: Sockets[? <: StreamSockets], chunks: Int, name: String)(using Async.Spawn): Unit = {
    val io: sockets.io.type = sockets.io
    val (listener, pairs) = connected(sockets, 1)
    val (client, server) = pairs.head
    timed(name, chunks)(streaming(io, chunks)(server, client)(io.perform(io.ops.shutdownOutput(client))))
    closeAll(io, Seq(client, server, listener))
    sockets.cleanup()
  }

  /** A connection made, accepted and closed, `count` times over. */
  private def socketConnect(sockets: Sockets[? <: StreamSockets], count: Int, name: String)(using Async.Spawn): Unit = {
    val io: sockets.io.type = sockets.io
    val listener = opening[io.ops.Listener](io)(sockets.listen)
    val connect = sockets.connect(listener)
    val accepted = Slot[io.ops.Connection]()
    val accept = io.ops.accept(listener, accepted)
    timed(name, count) {
      val server = Future {
        var i = 0
        while i < count do
          io.perform(accept)
          io.perform(io.ops.close(accepted.clear()))
          i += 1
      }
      val client = Future {
        val buf = buffer(8)
        var i = 0
        while i < count do
          val connection = opening[io.ops.Connection](io)(connect)
          // Until the server has closed its end, so that the next connect finds the last accepted.
          buf.clear()
          io.perform(io.ops.read(connection, buf))
          io.perform(io.ops.close(connection))
          i += 1
      }
      List(server, client)
    }
    io.perform(io.ops.close(listener))
    sockets.cleanup()
  }

  // ---- datagrams

  private def udpEcho(io: Io[IpDatagrams], rounds: Int)(using Async.Spawn): List[Future[Unit]] = {
    val any = IpSocketAddress(IpAddress.IPv4Loopback, 0)
    val server = opening[io.ops.BoundDatagram](io)(into => io.ops.bindDatagram(any, into))
    val client = opening[io.ops.BoundDatagram](io)(into => io.ops.bindDatagram(any, into))
    val serverAddress = io.ops.localAddress(server)
    val clientAddress = io.ops.localAddress(client)
    val echo = Future {
      val buf = buffer(MessageSize)
      val from = Slot[IpSocketAddress]()
      val receive = io.ops.receive(server, buf, from)
      val send = io.ops.send(server, buf, clientAddress)
      var i = 0
      while i < rounds do
        io.perform(receive)
        from.clear()
        io.perform(send)
        i += 1
    }
    val asking = Future {
      val out = buffer(MessageSize)
      val in = buffer(MessageSize)
      val from = Slot[IpSocketAddress]()
      val send = io.ops.send(client, out, serverAddress)
      val receive = io.ops.receive(client, in, from)
      var i = 0
      while i < rounds do
        out.clear()
        io.perform(send)
        io.perform(receive)
        from.clear()
        i += 1
    }
    List(Future { echo.await; asking.await; closeAll(io, Seq(server, client)) })
  }

  private def udpConnectedEcho(io: Io[IpDatagrams], rounds: Int)(using Async.Spawn): List[Future[Unit]] = {
    val any = IpSocketAddress(IpAddress.IPv4Loopback, 0)
    val bound = opening[io.ops.BoundDatagram](io)(into => io.ops.bindDatagram(any, into))
    val serverAddress = io.ops.localAddress(bound)
    val client = opening[io.ops.ConnectedDatagram](io)(into => io.ops.connectDatagram(any, serverAddress, into))
    val clientAddress = io.ops.localAddress(client)
    io.perform(io.ops.close(bound))
    val server =
      opening[io.ops.ConnectedDatagram](io)(into => io.ops.connectDatagram(serverAddress, clientAddress, into))
    val echo = Future {
      val buf = buffer(MessageSize)
      val receive = io.ops.receive(server, buf)
      val send = io.ops.send(server, buf)
      var i = 0
      while i < rounds do
        io.perform(receive)
        io.perform(send)
        i += 1
    }
    val asking = Future {
      val out = buffer(MessageSize)
      val in = buffer(MessageSize)
      val send = io.ops.send(client, out)
      val receive = io.ops.receive(client, in)
      var i = 0
      while i < rounds do
        out.clear()
        io.perform(send)
        io.perform(receive)
        i += 1
    }
    List(Future { echo.await; asking.await; closeAll(io, Seq(server, client)) })
  }

  private def udgEcho(io: Io[UnixDatagrams], rounds: Int)(using Async.Spawn): List[Future[Unit]] = {
    val serverAddress = UnixAddress(path("udg"))
    val clientAddress = UnixAddress(path("udg"))
    val server = opening[io.ops.UnixBoundDatagram](io)(into => io.ops.bindDatagram(serverAddress, into))
    val client = opening[io.ops.UnixBoundDatagram](io)(into => io.ops.bindDatagram(clientAddress, into))
    val echo = Future {
      val buf = buffer(MessageSize)
      val from = Slot[UnixAddress]()
      val receive = io.ops.receive(server, buf, from)
      val send = io.ops.send(server, buf, clientAddress)
      var i = 0
      while i < rounds do
        io.perform(receive)
        from.clear()
        io.perform(send)
        i += 1
    }
    val asking = Future {
      val out = buffer(MessageSize)
      val in = buffer(MessageSize)
      val from = Slot[UnixAddress]()
      val send = io.ops.send(client, out, serverAddress)
      val receive = io.ops.receive(client, in, from)
      var i = 0
      while i < rounds do
        out.clear()
        io.perform(send)
        io.perform(receive)
        from.clear()
        i += 1
    }
    List(Future {
      echo.await; asking.await
      closeAll(io, Seq(server, client))
      remove(serverAddress.path); remove(clientAddress.path)
    })
  }

  // ---- FIFOs

  private def mkfifo(path: String): Unit = BenchFiles.makeFifo(path)

  private def fifo(io: Io[Fifos])(using Async.Spawn): (String, io.ops.Readable, io.ops.Writable) = {
    val at = path("fifo")
    mkfifo(at)
    val read = opening[io.ops.Readable](io)(into => io.ops.openFifoForReading(at, into))
    val write = opening[io.ops.Writable](io)(into => io.ops.openFifoForWriting(at, into))
    (at, read, write)
  }

  private def fifoEcho(io: Io[Fifos], rounds: Int)(using Async.Spawn): List[Future[Unit]] = {
    val (requestPath, requestIn, requestOut) = fifo(io)
    val (replyPath, replyIn, replyOut) = fifo(io)
    val both = echoPair(io, rounds)(requestIn, replyOut, replyIn, requestOut)
    List(Future {
      both.foreach(_.await)
      closeAll(io, Seq(requestIn, requestOut, replyIn, replyOut))
      remove(requestPath); remove(replyPath)
    })
  }

  // ---- files

  private def newFile(blocks: Int): String = {
    val at = path("file")
    BenchFiles.writeZeros(at, Block * blocks)
    at
  }

  /** `count` blocks read, or written, at the start of the file: one op, performed again and again. */
  private def fileBlocks(io: Io[RegularFiles], at: String, count: Int, write: Boolean)(using
      Async.Spawn
  ): Future[Unit] = Future {
    val file = opening[io.ops.File](io)(into => io.ops.open(at, write, into))
    val buf = buffer(Block)
    val op = if write then io.ops.write(file, buf, 0L) else io.ops.read(file, buf, 0L)
    var i = 0
    while i < count do
      buf.clear()
      io.perform(op)
      i += 1
    io.perform(io.ops.close(file))
  }

  private def fileOpens(io: Io[RegularFiles], at: String, count: Int)(using Async.Spawn): Future[Unit] = Future {
    var i = 0
    while i < count do
      val file = opening[io.ops.File](io)(into => io.ops.open(at, false, into))
      io.perform(io.ops.close(file))
      i += 1
  }

  // ---- blocking, names, timers

  private def blockingCalls(io: Io[Blocking], count: Int)(using Async.Spawn): Future[Unit] = Future {
    val body: () => Unit = () => ()
    var i = 0
    while i < count do
      io.perform(io.ops.blocking(body))
      i += 1
  }

  private def resolving(io: Io[Resolving], count: Int)(using Async.Spawn): Future[Unit] = Future {
    val into = Slot[List[IpAddress]]()
    var i = 0
    while i < count do
      io.perform(io.ops.resolve("localhost", into))
      into.clear()
      i += 1
  }

  private def timers(io: Io[Interface], count: Int, millis: Long)(using Async.Spawn): Future[Unit] = Future {
    val timer = io.ops.timer(millis)
    var i = 0
    while i < count do
      io.perform(timer)
      i += 1
  }

  private object NoDrive extends Flow.Drive {
    def spawn(work: Async ?=> Unit): Unit = ()
    def later(run: Runnable): Unit = run.run()
    def hold(): Unit = ()
    def release(): Unit = ()
    def stop(): Unit = ()
  }

  private def timed(name: String, operations: Long)(body: => List[Future[Unit]])(using Async): Unit = {
    val start = System.nanoTime()
    body.foreach(_.await)
    val nanos = System.nanoTime() - start
    println(f"bench: $name%-20s $operations%9d ops ${nanos / 1e9}%8.3f s ${nanos / 1e3 / operations}%9.3f us/op")
  }

  /** Whether a backend with `ops` can do `scenario`, by the capability its name begins with. */
  private def supports(ops: Interface, scenario: String): Boolean = scenario.takeWhile(_ != '-') match
    case "pipe"     => ops.isInstanceOf[Pipes]
    case "tcp"      => ops.isInstanceOf[IpStreams]
    case "queue"    => ops.isInstanceOf[IpStreams] && ops.isInstanceOf[SubmissionQueues]
    case "flow"     => ops.isInstanceOf[IpStreams]
    case "unix"     => ops.isInstanceOf[UnixStreams]
    case "udp"      => ops.isInstanceOf[IpDatagrams]
    case "udg"      => ops.isInstanceOf[UnixDatagrams]
    case "fifo"     => ops.isInstanceOf[Fifos]
    case "file"     => ops.isInstanceOf[RegularFiles]
    case "blocking" => ops.isInstanceOf[Blocking]
    case "resolve"  => ops.isInstanceOf[Resolving]
    case _          => true

  /** Runs the scenarios `args` name, in order, `args.repeat` times over, skipping those over a capability the backend
    * lacks.
    */
  def run(io: Io[Interface], args: Args)(using Async.Spawn): Unit = {
    if args.serve > 0 then return serveCache(io.asInstanceOf[Io[IpStreams]], args.serve)
    val able = args.copy(scenarios = args.scenarios.filter(supports(io.ops, _)))
    // Every scenario left is over what the backend has.
    val all = io.asInstanceOf[Io[All]]
    for _ <- 1 to args.repeat do once(all, able)
  }

  private def once(io: Io[All], args: Args)(using Async.Spawn): Unit = {
    def count(n: Int): Int = math.max(1, (n * args.scale).toInt)
    val many = args.connections
    args.scenarios.foreach {
      case "pipe-pingpong" =>
        val rounds = count(200_000)
        timed("pipe-pingpong", rounds)(pipeEcho(io, rounds))
      case "pipe-concurrent" =>
        val rounds = count(10_000)
        timed("pipe-concurrent", rounds.toLong * many)(List.fill(many)(pipeEcho(io, rounds)).flatten)
      case "pipe-concurrent-a" => // closed by the main future, once all are done
        val rounds = count(10_000)
        val made = List.fill(many) {
          val (requestIn, requestOut) = pipe(io)
          val (replyIn, replyOut) = pipe(io)
          (requestIn, requestOut, replyIn, replyOut)
        }
        timed("pipe-concurrent-a", rounds.toLong * many)(
          made.flatMap((requestIn, requestOut, replyIn, replyOut) =>
            echoPair(io, rounds)(requestIn, replyOut, replyIn, requestOut)
          )
        )
        made.foreach((a, b, c, d) => closeAll(io, Seq(a, b, c, d)))
      case "pipe-concurrent-b" => // never closed
        val rounds = count(10_000)
        timed("pipe-concurrent-b", rounds.toLong * many)(
          List
            .fill(many) {
              val (requestIn, requestOut) = pipe(io)
              val (replyIn, replyOut) = pipe(io)
              echoPair(io, rounds)(requestIn, replyOut, replyIn, requestOut)
            }
            .flatten
        )
      case "pipe-stream" =>
        val chunks = count(20_000)
        val (in, out) = pipe(io)
        timed("pipe-stream", chunks)(streaming(io, chunks)(in, out)(io.perform(io.ops.close(out))))
        io.perform(io.ops.close(in))

      case "tcp-options" =>
        val listener =
          opening[io.ops.Listener](io)(into => io.ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), into))
        val address = io.ops.localAddress(listener).asInstanceOf[IpSocketAddress]
        val connecting = Future(opening[io.ops.Connection](io)(into => io.ops.connect(address, into)))
        val server = opening[io.ops.Connection](io)(into => io.ops.accept(listener, into))
        val client = connecting.await
        for (name, c) <- List(("client", client), ("server", server)) do
          println(s"options $name: nodelay ${io.ops.option(c, SocketOption.NoDelay)} snd ${io.ops
              .option(c, SocketOption.SendBuffer)} rcv ${io.ops.option(c, SocketOption.ReceiveBuffer)}")
        closeAll(io, Seq(client, server, listener))
      case "tcp-pingpong"    => socketEcho(tcp(io), 1, count(100_000), "tcp-pingpong")
      case "tcp-concurrent"  => socketEcho(tcp(io), many, count(5_000), "tcp-concurrent")
      case "tcp-stream"      => socketStream(tcp(io), count(20_000), "tcp-stream")
      case "tcp-connect"     => socketConnect(tcp(io), count(1_000), "tcp-connect")
      case "tcp-realistic"   => realistic(tcp(io), many, count(2_000), "tcp-realistic")
      case "work-flow" =>
        // A pipeline with no I/O in it: what an element costs to pass through three stages and a fold.
        val each = 1_000
        val times = count(20_000)
        timed("work-flow", each.toLong * times)(List(Future {
          var total = 0L
          var i = 0
          while i < times do {
            total += Flow.from(0 until each).map(_ + i).filter(_ % 3 != 0).map(_ * 2L).fold(0L)(_ + _)
            i += 1
          }
          if total == 42 then println(total)
        }))
      case scenario @ ("work-steps" | "work-vm") =>
        // What an element costs to be pushed through three stages that are stepped: a chain of steps, or a program.
        val each = 1_000
        val times = count(20_000)
        timed(scenario, each.toLong * times)(List(Future {
          var total = 0L
          val last = new Flow.Step[Long] {
            def push(element: Long): Unit = total += element
            def end(): Unit = ()
            def abandon(): Unit = ()
          }
          var bias = 0
          val step: Flow.Step[Int] =
            if scenario == "work-vm" then Pipe.from[Int].map(_ + bias).filter(_ % 3 != 0).map(_ * 2L).program.start(last)
            else {
              val port = new Flow.Port[Int]
              Flow.compile(port.map(_ + bias).filter(_ % 3 != 0).map(_ * 2L), port, last, NoDrive).nn.asInstanceOf[Flow.Step[Int]]
            }
          var i = 0
          while i < times do {
            bias = i
            var element = 0
            while element < each do {
              step.push(element)
              element += 1
            }
            i += 1
          }
          if total == 42 then println(total)
        }))
      case scenario @ ("work-setup-steps" | "work-setup-vm") =>
        // What a connection costs to begin: a pipeline with a stage of its own and sixteen keys, each element the
        // first of its key. As a flow it is made and compiled for each; as a program it is made once and started.
        val times = count(200_000)
        timed(scenario, times.toLong)(List(Future {
          var total = 0L
          val last = new Flow.Step[Int] {
            def push(element: Int): Unit = total += element
            def end(): Unit = ()
            def abandon(): Unit = ()
          }
          val program =
            Pipe.from[Int].process[Int](emit => element => emit(element)).groupThrough(_ & 15)(_.mapWith[Any, Int](_ => {
              var seen = 0
              element => { seen += 1; element + seen }
            })).program
          var i = 0
          while i < times do {
            val step: Flow.Step[Int] =
              if scenario == "work-setup-vm" then program.start(last)
              else {
                val port = new Flow.Port[Int]
                val flow = port.process[Int](emit => element => emit(element)).groupThrough(_ & 15) { (_, elements) =>
                  var seen = 0
                  elements.map { element => seen += 1; element + seen }
                }
                Flow.compile(flow, port, last, NoDrive).nn.asInstanceOf[Flow.Step[Int]]
              }
            var element = 0
            while element < 16 do {
              step.push(element)
              element += 1
            }
            step.end()
            i += 1
          }
          if total == 42 then println(total)
        }))
      case "work-flow-long" =>
        // A pipeline of a thousand stages, which is cut onto stacks of its own: what a hand-over costs an element.
        val stages = 1_000
        val elements = count(20_000)
        timed("work-flow-long", elements.toLong)(List(Future {
          var flow: Flow[Int] = Flow.from(0 until elements)
          var i = 0
          while i < stages do {
            flow = flow.map(_ + 1)
            i += 1
          }
          val total = flow.fold(0L)(_ + _)
          if total == 42 then println(total)
        }))
      case "work-iterator" =>
        // The same with the library's iterators, to read `work-flow` against.
        val each = 1_000
        val times = count(20_000)
        timed("work-iterator", each.toLong * times)(List(Future {
          var total = 0L
          var i = 0
          while i < times do {
            total += (0 until each).iterator.map(_ + i).filter(_ % 3 != 0).map(_ * 2L).foldLeft(0L)(_ + _)
            i += 1
          }
          if total == 42 then println(total)
        }))
      case "flow-mux" =>
        flowMux(io.asInstanceOf[Io[IpStreams & SubmissionQueues]], many, count(2_000), "flow-mux", Server.Strategy.Auto)
      case "flow-mux-await" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-await", Server.Strategy.Auto, None, true)
      case "flow-mux-machine-await" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-machine-await", Server.Strategy.Auto, Some(true), true)
      case "flow-mux-staged-await" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-staged-await", Server.Strategy.Auto, Some(false), true)
      case "flow-mux-machine" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-machine", Server.Strategy.Auto, Some(true))
      case "flow-mux-hand-loop" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-hand-loop", Server.Strategy.Auto, hand = "loop")
      case "flow-mux-hand-pinned" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-hand-pinned", Server.Strategy.Auto, hand = "pinned")
      case "flow-mux-hand-queue" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-hand-queue", Server.Strategy.Auto, hand = "queue")
      case "flow-mux-vm" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-vm", Server.Strategy.Auto, vm = true)
      case "flow-mux-staged" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-staged", Server.Strategy.Auto, Some(false))
      case "flow-mux-shard-futures" =>
        flowMux(io.asInstanceOf[Io[IpStreams]], many, count(2_000), "flow-mux-shard-futures", Server.Strategy.ShardFutures)
      case "flow-mux-pinned-futures" =>
        flowMux(
          io.asInstanceOf[Io[IpStreams & SubmissionQueues]],
          many,
          count(2_000),
          "flow-mux-pinned-futures",
          Server.Strategy.PinnedFutures
        )
      case "flow-mux-futures" =>
        flowMux(io.asInstanceOf[Io[IpStreams & SubmissionQueues]], many, count(2_000), "flow-mux-futures", Server.Strategy.Futures)
      case "queue-concurrent" =>
        queueEcho(io.asInstanceOf[Io[IpStreams & SubmissionQueues]], many, count(5_000), "queue-concurrent")
      case "queue-realistic" =>
        queueRealistic(io.asInstanceOf[Io[IpStreams & SubmissionQueues]], many, count(2_000), "queue-realistic", false)
      case "queue-realistic-bulk" =>
        queueRealistic(io.asInstanceOf[Io[IpStreams & SubmissionQueues]], many, count(2_000), "queue-realistic-bulk", true)
      case "tcp-realistic-bulk" =>
        realisticBulk(tcp(io), many, count(2_000), "tcp-realistic-bulk", gathering = false)
      case "tcp-realistic-gather" =>
        realisticBulk(tcp(io), many, count(2_000), "tcp-realistic-gather", gathering = true)
      case "work-realistic" =>
        val rounds = count(2_000)
        timed("work-realistic", rounds.toLong * many)(List(realisticWork(many, rounds)))
      case "unix-pingpong"   => socketEcho(unix(io), 1, count(100_000), "unix-pingpong")
      case "unix-concurrent" => socketEcho(unix(io), many, count(5_000), "unix-concurrent")
      case "unix-stream"     => socketStream(unix(io), count(20_000), "unix-stream")
      case "unix-connect"    => socketConnect(unix(io), count(5_000), "unix-connect")

      case "udp-pingpong" =>
        val rounds = count(100_000)
        timed("udp-pingpong", rounds)(udpEcho(io, rounds))
      case "udp-concurrent" =>
        val rounds = count(5_000)
        timed("udp-concurrent", rounds.toLong * many)(List.fill(many)(udpEcho(io, rounds)).flatten)
      case "udp-connected" =>
        val rounds = count(100_000)
        timed("udp-connected", rounds)(udpConnectedEcho(io, rounds))
      case "udg-pingpong" =>
        io.ops match
          case _: UnixDatagrams =>
            val rounds = count(100_000)
            timed("udg-pingpong", rounds)(udgEcho(io.asInstanceOf[Io[UnixDatagrams]], rounds))
          case _ => ()
      case "udg-concurrent" =>
        io.ops match
          case _: UnixDatagrams =>
            val rounds = count(5_000)
            val udg = io.asInstanceOf[Io[UnixDatagrams]]
            timed("udg-concurrent", rounds.toLong * many)(List.fill(many)(udgEcho(udg, rounds)).flatten)
          case _ => ()

      case "fifo-pingpong" =>
        io.ops match
          case _: Fifos =>
            val rounds = count(100_000)
            timed("fifo-pingpong", rounds)(fifoEcho(io.asInstanceOf[Io[Fifos]], rounds))
          case _ => ()

      case "file-read" =>
        val at = newFile(4)
        val blocks = count(50_000)
        timed("file-read", blocks)(List(fileBlocks(io, at, blocks, write = false)))
        remove(at)
      case "file-write" =>
        val at = newFile(4)
        val blocks = count(50_000)
        timed("file-write", blocks)(List(fileBlocks(io, at, blocks, write = true)))
        remove(at)
      case "file-concurrent" =>
        val at = newFile(4)
        val blocks = count(2_000)
        timed("file-concurrent", blocks.toLong * many)(List.fill(many)(fileBlocks(io, at, blocks, write = false)))
        remove(at)
      case "file-open" =>
        val at = newFile(1)
        val opens = count(20_000)
        timed("file-open", opens)(List(fileOpens(io, at, opens)))
        remove(at)

      case "blocking" =>
        val calls = count(50_000)
        timed("blocking", calls)(List(blockingCalls(io, calls)))
      case "blocking-concurrent" =>
        val calls = count(2_000)
        timed("blocking-concurrent", calls.toLong * many)(List.fill(many)(blockingCalls(io, calls)))
      case "resolve" =>
        val lookups = count(2_000)
        timed("resolve", lookups)(List(resolving(io, lookups)))

      case "timer-zero" =>
        val turns = count(200_000)
        timed("timer-zero", turns)(List(timers(io, turns, 0)))
      case "timers" =>
        // A thousand futures each sleeping a millisecond at a time: ideally a microsecond an op.
        val sleeps = count(200)
        timed("timers", sleeps.toLong * 1000)(List.fill(1000)(timers(io, sleeps, 1)))

      case other => throw new IllegalArgumentException(s"no scenario $other")
    }
    BenchFiles.remove(tmp)
  }
}
