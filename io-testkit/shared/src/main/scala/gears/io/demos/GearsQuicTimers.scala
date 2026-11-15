package gears.io.demos

import gears.async.Async
import gears.async.Future
import gears.async.UnboundedChannel
import gears.async.io.Interface
import gears.async.io.Io
import gears.async.io.IpAddress
import gears.async.io.IpDatagrams
import gears.async.io.IpSocketAddress
import gears.async.io.Slot
import gears.async.io.testkit.BufferArena
import gears.async.io.testkit.DeadlineTimers
import gears.io.testkit.SimpleIO

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.concurrent.duration.*

/** QUIC's timers, without QUIC: each connection's state machine keeps a deadline, the earliest of its timers, and
  * recomputes it after every packet, as quinn's `poll_timeout` and quiche's `timeout` give it. Here two of QUIC's
  * timers: the acknowledgement delay (RFC 9000 §13.2.1: acknowledge every `AckEvery` packets at once, or the rest
  * within `MaxAckDelay`) and the idle timeout (§10.1), which closes a connection that hears nothing.
  *
  * The server is shaped as a QUIC endpoint is: one bound socket, one task receiving on it and handing each packet to
  * its connection by the connection ID in the packet, as connections share the socket. Each connection is a driver
  * reading one inbox: its packets, and ticks from its timer task, which performs one deadline timer, repeatedly, that
  * the driver moves after each event (`moveTo`). Each driver sends its own acknowledgements on the shared socket, its
  * sends taking turns with the other drivers'. Nothing is cancelled while the connection lives: the timer's wait goes
  * on while packets come, and a receive never gives way to a timer, where a deadline on the receive, or a race of the
  * two, would cancel one of them each time the other came first.
  *
  * The clients send their packets, `interval` apart, and wait to hear every one acknowledged; the server closes each
  * connection once it has been idle `IdleTimeout`, and the run checks that every packet was acknowledged and every
  * connection closed.
  */
object GearsQuicTimers {
  val AckEvery = 4
  val Window = 16
  val MaxAckDelay: FiniteDuration = 25.millis
  val IdleTimeout: FiniteDuration = 200.millis

  // A packet: the connection ID (8 bytes), then the packet number (4 bytes), from a client; the largest number
  // acknowledged (4 bytes), from the server.
  private val PacketSize = 12
  private val AckSize = 12

  private enum Event {
    case Packet(number: Int)
    case Tick
  }

  /** What the server's connections did, all told. */
  final class Counts {
    var packets = 0L
    var acksAtOnce = 0L
    var acksOnTimer = 0L
    var moves = 0L
    var earlierMoves = 0L
    var laterMoves = 0L
    var fires = 0L
    var closedIdle = 0
  }

  final case class Result(counts: Counts, activeNanos: Long, totalNanos: Long)

  /** The demo's exact server timer state machine, with channels carrying packets and acknowledgements instead of UDP.
    * For backends which have timers but no datagrams. Active time ends once all packets have been acknowledged; total
    * time also includes waiting for every idle timeout. Client windowing and optional packet pacing are kept.
    */
  def runInMemory(connections: Int, packets: Int, interval: FiniteDuration, mode: DeadlineTimers.Mode)(using
      io: Io[Interface],
      async: Async.Spawn
  ): Result = {
    require(connections > 0 && packets > 0 && interval.length >= 0)
    Async.group {
      val counts = new Counts
      val closed = UnboundedChannel[Unit]()
      val inboxes = Array.fill(connections)(UnboundedChannel[Event]())
      val acknowledgements = Array.fill(connections)(UnboundedChannel[Int]())
      val start = System.nanoTime()
      for c <- 0 until connections do
        Future {
          drive(inboxes(c), counts, n => acknowledgements(c).sendImmediately(n), mode.timer(_))
          closed.sendImmediately(())
        }
      val clients = (0 until connections).map { c =>
        Future {
          val pause = io.ops.timer(interval)
          var acknowledged = -1
          for number <- 0 until packets do {
            while number - acknowledged > Window do
              acknowledged = math.max(acknowledged, acknowledgements(c).read().toOption.get)
            inboxes(c).sendImmediately(Event.Packet(number))
            if interval.length > 0 && number < packets - 1 then io.perform(pause)
          }
          while acknowledged < packets - 1 do
            acknowledged = math.max(acknowledged, acknowledgements(c).read().toOption.get)
        }
      }
      clients.foreach(_.await)
      val active = System.nanoTime() - start
      for _ <- 0 until connections do closed.read()
      check(counts.packets == connections.toLong * packets, s"the server heard ${counts.packets} packets")
      check(counts.closedIdle == connections, s"${counts.closedIdle} connections closed idle")
      Result(counts, active, System.nanoTime() - start)
    }
  }

  def run(connections: Int, packets: Int, interval: FiniteDuration)(using
      io: Io[IpDatagrams],
      async: Async.Spawn
  ): Unit = {
    require(connections > 0 && packets > 0, "connections and packets are positive")
    // Grouped inside the arena's scope, so that the futures still running, the server's, have stopped before the arena
    // and its buffers are closed.
    BufferArena.scoped { arena =>
      Async.group(run(connections, packets, interval, n => arena.synchronized(arena.allocate(n))))
    }
  }

  /** The run, with buffers from `buffer`, which every backend's ops can fill and drain. */
  private def run(connections: Int, packets: Int, interval: FiniteDuration, buffer: Int => ByteBuffer)(using
      io: Io[IpDatagrams],
      async: Async.Spawn
  ): Unit = {
    val socket = SimpleIO.bindDatagram(IpSocketAddress(IpAddress.IPv4Loopback, 0))
    val address = io.ops.localAddress(socket)
    val counts = new Counts
    val closed = UnboundedChannel[Long]()
    val start = System.nanoTime()
    try {
      val server = Future(serve(io, socket, counts, closed, buffer))
      val clients =
        (0 until connections).map(c => Future(client(io, address, c.toLong + 1, packets, interval, buffer)))
      clients.foreach(_.await)
      val sent = System.nanoTime()
      for _ <- 0 until connections do closed.read()
      val seconds = (System.nanoTime() - start) / 1e9
      server.cancel()
      println(
        f"quic-timers: $connections connections of $packets packets, ${interval.toMicros} µs apart: every packet " +
          f"acknowledged, every connection closed idle ${(System.nanoTime() - sent) / 1e6}%.0f ms after the last " +
          f"sent, in $seconds%.3f s"
      )
      println(
        s"quic-timers: server: ${counts.packets} packets, ${counts.acksAtOnce + counts.acksOnTimer} acknowledgements " +
          s"(${counts.acksOnTimer} on the delay timer), ${counts.moves} deadline moves, ${counts.fires} timer fires, " +
          s"${counts.closedIdle} connections closed idle"
      )
      check(counts.packets == connections.toLong * packets, s"the server heard ${counts.packets} packets")
      check(counts.closedIdle == connections, s"${counts.closedIdle} connections closed idle")
    } finally io.perform(io.ops.close(socket))
  }

  private def check(ok: Boolean, what: => String): Unit =
    if !ok then throw new IllegalStateException(s"quic-timers: $what")

  /** The endpoint: receives each packet and hands it to its connection's driver, starting one for a new connection.
    */
  private def serve(
      io: Io[IpDatagrams],
      socket: io.ops.BoundDatagram,
      counts: Counts,
      closed: UnboundedChannel[Long],
      buffer: Int => ByteBuffer
  )(using Async.Spawn): Unit = {
    val drivers = mutable.HashMap.empty[Long, UnboundedChannel[Event]]
    val packet = buffer(64)
    val from = Slot[IpSocketAddress]()
    val receive = io.ops.receive(socket, packet, from)
    while true do {
      packet.clear()
      io.perform(receive)
      val peer = from.clear()
      val id = packet.getLong()
      val number = packet.getInt()
      val inbox = drivers.synchronized(
        drivers.getOrElseUpdate(
          id, {
            val inbox = UnboundedChannel[Event]()
            Future {
              drive(io, socket, id, peer, inbox, counts, buffer)
              drivers.synchronized(drivers.remove(id))
              closed.sendImmediately(id)
            }
            inbox
          }
        )
      )
      inbox.sendImmediately(Event.Packet(number))
    }
  }

  /** A connection's driver: QUIC's state machine for these two timers, on its own inbox, moving its one deadline timer
    * after each event, until it has been idle `IdleTimeout`.
    */
  private def drive(
      io: Io[IpDatagrams],
      socket: io.ops.BoundDatagram,
      id: Long,
      peer: IpSocketAddress,
      inbox: UnboundedChannel[Event],
      counts: Counts,
      buffer: Int => ByteBuffer
  )(using Async.Spawn): Unit = {
    val ack = buffer(AckSize)
    drive(
      inbox,
      counts,
      largest => {
        ack.clear()
        ack.putLong(id).putInt(largest).flip()
        io.perform(io.ops.send(socket, ack, peer))
      },
      DeadlineTimers.Mode.Primitive.timer(_)(using io, summon[Async])
    )
  }

  private def drive(
      inbox: UnboundedChannel[Event],
      counts: Counts,
      sendAck: Int => Unit,
      makeTimer: Long => DeadlineTimers.Timer
  )(using Async.Spawn): Unit = {
    val Never = Long.MaxValue
    var idleAt = System.nanoTime() + IdleTimeout.toNanos
    var ackAt = Never
    var unacknowledged = 0
    var largest = -1
    val timer = makeTimer(idleAt)
    val rearmed = UnboundedChannel[Unit]()
    // Performs the timer, again and again, ticking the driver each time it fires, then waiting for it to be moved.
    val ticker = Future {
      while true do {
        timer.await()
        inbox.sendImmediately(Event.Tick)
        rearmed.read()
      }
    }
    def acknowledge(onTimer: Boolean): Unit = {
      sendAck(largest)
      counts.synchronized(if onTimer then counts.acksOnTimer += 1 else counts.acksAtOnce += 1)
      unacknowledged = 0
      ackAt = Never
    }
    def rearm(): Unit = {
      val next = math.min(ackAt, idleAt)
      val previous = timer.deadline
      timer.moveTo(next)
      counts.synchronized {
        counts.moves += 1
        if next - previous < 0 then counts.earlierMoves += 1
        else if next != previous then counts.laterMoves += 1
      }
    }
    try {
      var open = true
      while open do
        inbox.read().toOption.get match {
          case Event.Packet(number) =>
            val now = System.nanoTime()
            counts.synchronized(counts.packets += 1)
            largest = math.max(largest, number)
            unacknowledged += 1
            idleAt = now + IdleTimeout.toNanos
            if unacknowledged >= AckEvery then acknowledge(onTimer = false)
            else if ackAt == Never then ackAt = now + MaxAckDelay.toNanos
            rearm()
          case Event.Tick =>
            val now = System.nanoTime()
            counts.synchronized(counts.fires += 1)
            if now >= ackAt then acknowledge(onTimer = true)
            if now >= idleAt then {
              counts.synchronized(counts.closedIdle += 1)
              open = false
            } else {
              rearm()
              rearmed.sendImmediately(())
            }
        }
    } finally ticker.cancel()
  }

  /** A client: sends `packets` packets, `interval` apart and at most `Window` unacknowledged, as QUIC's congestion
    * window clocks sending by acknowledgements, and returns once it has heard every one acknowledged, failing if it
    * hears none for a second: its own deadline timer, moved later on every acknowledgement.
    */
  private def client(
      io: Io[IpDatagrams],
      server: IpSocketAddress,
      id: Long,
      packets: Int,
      interval: FiniteDuration,
      buffer: Int => ByteBuffer
  )(using Async.Spawn): Unit = {
    val socket = SimpleIO.connectDatagram(null, server)(using io, summon[Async])
    val silence = 1.second.toNanos
    val limit = io.deadlineTimer(System.nanoTime() + silence)
    try {
      val acks = UnboundedChannel[Int]()
      val exchange = Future {
        val receiving = Future {
          val ack = buffer(64)
          val receive = io.ops.receive(socket, ack)
          var largest = -1
          while largest < packets - 1 do {
            ack.clear()
            io.perform(receive)
            check(ack.getLong() == id, "an acknowledgement for another connection")
            largest = math.max(largest, ack.getInt())
            limit.moveTo(System.nanoTime() + silence)
            acks.sendImmediately(largest)
          }
        }
        val packet = buffer(PacketSize)
        val pause = io.ops.timer(interval)
        var acknowledged = -1
        for number <- 0 until packets do {
          while number - acknowledged > Window do acknowledged = math.max(acknowledged, acks.read().toOption.get)
          packet.clear()
          packet.putLong(id).putInt(number).flip()
          io.perform(io.ops.send(socket, packet))
          if number < packets - 1 then io.perform(pause)
        }
        receiving.await
      }
      val silent = Future(limit.await())
      Async.select(
        exchange.handle(_.get),
        silent.handle(_ => check(false, s"connection $id: no acknowledgement for a second"))
      )
      silent.cancel()
    } finally io.perform(io.ops.close(socket))
  }
}
