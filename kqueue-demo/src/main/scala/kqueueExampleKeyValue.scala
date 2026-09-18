package example

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Accept
import asyncio.reactor.Command
import asyncio.reactor.Completion
import asyncio.reactor.Interest
import asyncio.reactor.Reactor
import asyncio.reactor.ReadIntoBuffer
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

/** A pipelined key-value service over the length-framed stream protocol, in the style of a Redis client and server. A
  * connection stays open for many commands, and replies go back in order while further commands are still arriving, so
  * each socket has a read and a write request pending at the same time.
  */
object KQueueExampleKeyValue {
  private val BufferSize = 8192

  /** Unsent replies (server) or unread replies (client) beyond this many bytes pause reading until they drain. */
  private val Backlog = 64 * 1024

  /** Executes `SET key value`, `GET key`, and `INCR key` against an in-memory map. */
  private final class Store {
    private val values = mutable.Map.empty[String, String]

    def execute(command: String): String = command.split(" ", 3) match {
      case Array("SET", key, value) =>
        values(key) = value
        "OK"
      case Array("GET", key)  => values.getOrElse(key, "(nil)")
      case Array("INCR", key) =>
        values.get(key).map(_.toIntOption) match {
          case Some(None) => "ERR value is not an integer"
          case current    =>
            val next = current.flatten.getOrElse(0) + 1
            values(key) = next.toString
            next.toString
        }
      case _ => s"ERR unknown command `$command`"
    }
  }

  /** One client connection: parses commands out of whatever arrives, queues the replies, and writes them back while
    * reading continues. Owns a zone holding its two buffers, closed with the connection.
    */
  private final class Session(val fd: Int, store: Store, server: Server)
      extends AutoCloseable
      with Completion[Command] {
    private val zone: Zone = Zone.open()
    private val inbox: ByteBuffer = NativeBuffer.allocate(BufferSize)(using zone)
    private val outbox: ByteBuffer = NativeBuffer.allocate(BufferSize)(using zone)
    private val commands = new FrameParser
    private val replies = new ByteArrayOutputStream() // framed, waiting for the outbox
    private var writing = false
    private var reading = false
    private var clientClosed = false

    private inline val Failed = -2

    /** Reads a chunk of commands into the inbox. Failures become a result rather than an exception. */
    private object ReadCommands extends Command {
      def fd: Int = Session.this.fd
      def interest: Interest = Interest.Read
      def perform(): Int =
        try {
          inbox.clear()
          nioReadBytes(fd, inbox)
        } catch {
          case e: IOException =>
            println(s"Client socket $fd failed: ${e.getMessage}")
            Failed
        }
    }

    /** Writes the outbox of framed replies. */
    private object WriteReplies extends Command {
      def fd: Int = Session.this.fd
      def interest: Interest = Interest.Write
      def perform(): Int =
        try nioWriteBytes(fd, outbox)
        catch {
          case e: IOException =>
            println(s"Client socket $fd failed: ${e.getMessage}")
            Failed
        }
    }

    def onComplete(r: Reactor, command: Command, result: Int): Unit = command match {
      case ReadCommands =>
        reading = false
        if result == Failed then server.drop(this)
        else if result < 0 then {
          clientClosed = true // Answer what has been parsed, then close.
          if !writing then server.drop(this)
        } else {
          if result > 0 then {
            inbox.flip()
            commands.feed(inbox)
            var next = commands.next()
            while next != null do {
              StreamProtocol.appendTo(replies, store.execute(next))
              next = commands.next()
            }
            if !writing then sendReplies(r)
          }
          // Back-pressure: stop reading while the client is not taking its replies.
          if replies.size <= Backlog then submitRead(r)
        }
      case WriteReplies =>
        if result == Failed then server.drop(this)
        else if outbox.hasRemaining() then r.submit(WriteReplies, this)
        else {
          writing = false
          if replies.size > 0 then sendReplies(r)
          if !reading && !clientClosed && replies.size <= Backlog then submitRead(r)
          if clientClosed && !writing then server.drop(this)
        }
      case _ => ()
    }

    def start(r: Reactor): Unit = submitRead(r)

    private def submitRead(r: Reactor): Unit = {
      reading = true
      r.submit(ReadCommands, this)
    }

    /** Moves up to one buffer of queued replies into the outbox and starts writing it. */
    private def sendReplies(r: Reactor): Unit = {
      val queued = replies.toByteArray
      val chunk = math.min(queued.length, outbox.capacity())
      outbox.clear()
      outbox.put(queued, 0, chunk)
      outbox.flip()
      replies.reset()
      replies.write(queued, chunk, queued.length - chunk)
      writing = true
      r.submit(WriteReplies, this)
    }

    def close(): Unit = {
      zone.close()
      PosixSockets.close(fd)
      println(s"Closed client socket: $fd")
    }
  }

  private final class Server(serverFd: Int) extends Completion[Accept] {
    private val store = new Store
    private var sessions: Map[Int, Session] = Map.empty

    def start(r: Reactor): Unit = r.submit(Accept(serverFd), this)

    def onComplete(r: Reactor, command: Accept, clientFd: Int): Unit = {
      if clientFd >= 0 then {
        val session = new Session(clientFd, store, this)
        sessions += clientFd -> session
        session.start(r)
        println(s"Accepted new client connection on serverFd: $clientFd")
      }
      r.submit(command, this)
    }

    def drop(session: Session): Unit = {
      sessions -= session.fd
      session.close()
    }

    def close(): Unit = {
      val old = sessions
      sessions = Map.empty
      old.values.foreach(_.close())
    }
  }

  def serve(address: KQueueExampleAddress): Unit = {
    Reactor.scoped { r =>
      address.withBoundSocket(Transport.Stream) { serverFd =>
        PosixSockets.listen(serverFd, PosixSockets.maxConnections)
        println("Key-value server is now listening for connections.")
        val server = new Server(serverFd)
        server.start(r)
        try r.run(capacity = 255)
        finally server.close()
      }
    }
  }

  /** Pipelines a batch of commands without waiting for replies, and reads the replies back as they arrive. */
  private final class Pipeline(sock: ClientSock, batch: IndexedSeq[String])(using Zone) extends Completion[Command] {
    private val outbox = NativeBuffer.allocate(BufferSize)
    private val inbox = NativeBuffer.allocate(BufferSize)
    private val parser = new FrameParser
    private var sent = 0
    private var received = 0
    private var lastReply = ""
    outbox.flip() // Start empty in draining mode, so the first write packs commands rather than sending unset bytes.

    /** Packs as many pending commands as fit into the empty outbox. */
    private def pack(): Unit = {
      outbox.clear()
      while sent < batch.length && StreamProtocol.append(batch(sent), outbox) do sent += 1
      outbox.flip()
    }

    /** Packs the next commands whenever the outbox has drained, then writes it. */
    private object SendBatch extends Command {
      def fd: Int = sock.fd
      def interest: Interest = Interest.Write
      def perform(): Int = {
        sock.ensureConnected()
        if !outbox.hasRemaining() then pack()
        nioWriteBytes(sock.fd, outbox)
      }
    }

    def onComplete(r: Reactor, command: Command, result: Int): Unit = command match {
      case SendBatch =>
        if outbox.hasRemaining() || sent < batch.length then r.submit(SendBatch, this)
        else println(s"Pipelined ${batch.length} commands.")
      case read: ReadIntoBuffer =>
        if result < 0 then throw new IOException(s"Server closed the connection after $received replies")
        else {
          if result > 0 then {
            inbox.flip()
            parser.feed(inbox)
            var reply = parser.next()
            while reply != null do {
              received += 1
              lastReply = reply
              reply = parser.next()
            }
          }
          if received == batch.length then {
            println(s"Received $received replies; last reply: `$lastReply`")
            r.stop()
          } else {
            inbox.clear()
            r.submit(read, this)
          }
        }
      case _ => ()
    }

    def start(r: Reactor): Unit = {
      r.submit(SendBatch, this)
      inbox.clear()
      r.submit(ReadIntoBuffer(sock.fd, inbox), this)
    }
  }

  /** Sets a counter, increments it `count` times, then reads it back, all in one pipelined batch. */
  def run(address: KQueueExampleAddress, count: Int): Unit = {
    require(count > 0, "count must be positive")
    val batch = "SET counter 0" +: Vector.fill(count)("INCR counter") :+ "GET counter"
    Reactor.scoped { r =>
      address.withSocket(Transport.Stream) { clientFd =>
        println(s"opened file descriptor: $clientFd (socket-type)")
        address.connect(clientFd)
        println(s"connection in progress to: $address")
        Zone.acquire { implicit z =>
          new Pipeline(ClientSock(clientFd), batch).start(r)
          r.run(capacity = 255)
        }
      }
    }
  }
}
