package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.reactor.Command
import asyncio.reactor.Completion
import asyncio.reactor.Interest
import asyncio.reactor.Reactor
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileRead {

  /** Reads everything available whenever the FIFO is readable, gathering the contents until the writer closes it. */
  private final class Reader(fd: Int)(using Zone) extends Completion[Command] {
    private val chunk = NativeBuffer.allocate(1024)
    private val contents = new ByteArrayOutputStream()

    inline val ReadAgain = true
    inline val Stop = false

    /** Drains the FIFO; the result records whether EOF was reached. */
    final class Drain extends Command {
      def fd: Int = Reader.this.fd
      def interest: Interest = Interest.Read
      def perform(): Int = if drain() then 1 else 0
    }

    val drainCommand = new Drain

    /** Reads everything currently available; returns true once the writer has closed the FIFO. */
    def drain(): Boolean = {
      var isEOF = false
      while {
        chunk.clear()
        val read = nioReadBytes(fd, chunk)
        if read < 0 then {
          isEOF = true
          Stop
        } else if read == 0 then {
          println("No more data available to read (EAGAIN or EWOULDBLOCK).")
          Stop
        } else {
          println(s"actually read $read bytes")
          chunk.flip()
          drainTo(chunk, contents)
          ReadAgain
        }
      } do ()
      isEOF
    }

    def report(): Unit = {
      println(s"End of file reached. Contents are ${contents.size} long.")
      println(s"File contents: `${contents.toString(StandardCharsets.UTF_8)}`")
      contents.reset() // Start over for the next writer
    }

    def start(r: Reactor): Unit = r.submit(drainCommand, this)

    def onComplete(r: Reactor, command: Command, result: Int): Unit = {
      if result == 1 then report()
      r.submit(command, this)
    }
  }

  def run(fifo: String): Unit = {
    Reactor.scoped { r =>
      withFile(fifo, write = false) { fd =>
        Zone.acquire { implicit z =>
          val reader = new Reader(fd)
          reader.start(r)
          println("Event registered for file read.")
          println("starting to poll...")
          r.run(capacity = 1)
        }
      }
    }
  }
}
