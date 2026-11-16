package gears.async.io.node.host

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalajs.js
import scala.scalajs.js.typedarray.Int8Array
import scala.scalajs.js.typedarray.Uint8Array

/** A stream of Node's, a `net.Socket`, read and written without waiting.
  *
  * It is read as Node's paused streams are: Node takes bytes from the system up to its high-water mark and keeps them
  * until a read asks, so a peer that sends to a program that does not read is held back as the system would hold it. It
  * is written one chunk at a time, of at most `ChunkSize` bytes, the next not taken until Node has handed the last to
  * the system: Node itself takes whatever it is given, without bound, and a write it has taken cannot be taken back, so
  * a write here would block before Node has anything of it, and one that waited and was then cancelled has written
  * nothing. What Node holds, then, is one chunk at most, as a socket's send buffer a little larger.
  *
  * Its listeners are Node's for the life of the stream, four functions made once: an op that waits adds none.
  */
private[node] final class NodeStream(socket: js.Dynamic, readable: Boolean, writable: Boolean)
    extends ByteSource
    with ByteSink {
  private var endSeen = false
  private var error: IOException | Null = null
  private var closed = false
  private var unsent = 0
  private var shutDown = !writable

  private def tellReader(): Unit = {
    val tell = onReadable
    if tell != null then tell()
  }

  private def tellWriter(): Unit = {
    val tell = onWritable
    if tell != null then tell()
  }

  private val readableSeen: js.Function0[Unit] = () => {
    // With nothing to read, this is the end coming: a read that finds none has Node say so, with `end`.
    if socket.readableLength.asInstanceOf[Int] == 0 then socket.read(): Unit
    tellReader()
  }
  private val endOfStream: js.Function0[Unit] = () => {
    endSeen = true
    tellReader()
  }
  private val failed: js.Function1[js.Dynamic, Unit] = failure => {
    if error == null then error = NodeErr.io(failure)
    tellReader()
    tellWriter()
  }
  private val closedSeen: js.Function0[Unit] = () => {
    closed = true
    tellReader()
    tellWriter()
  }

  /** One function for every chunk written: Node calls it once for each, in order, so the sizes wait in a queue. */
  private val sizes = js.Array[Int]()
  private val sent: js.Function1[js.Dynamic, Unit] = failure => {
    unsent -= sizes.shift()
    if NodeErr.failed(failure) && error == null then error = NodeErr.io(failure)
    tellWriter()
  }

  socket.on("error", failed)
  socket.on("close", closedSeen)
  if readable then {
    socket.on("end", endOfStream)
    socket.on("readable", readableSeen)
  }

  def read(max: Int): Int8Array | Null = {
    val available = if readable && !closed then socket.readableLength.asInstanceOf[Int] else 0
    if available > 0 then {
      val chunk = socket.read(math.min(max, available))
      if chunk == null then throw new IOException("Node had no bytes of those it said were ready")
      // A read of exactly what was left does not have Node say that the end, if it has come already, is next: only a
      // read that finds nothing does, so one is made, which takes nothing.
      if socket.readableLength.asInstanceOf[Int] == 0 then socket.read(0): Unit
      NodeBuffer.signed(chunk.asInstanceOf[Uint8Array])
    } else if error != null then throw error.nn
    else if endSeen || closed then ByteSource.Ended
    else null
  }

  def write(bufs: Array[ByteBuffer]): Int = {
    if shutDown then throw new IOException("The stream is shut down for writing")
    if error != null then throw error.nn
    if closed then throw new IOException("Broken pipe: the stream has closed")
    if unsent > 0 then ByteSink.Blocked
    else {
      val chunk = NodeBuffer.drain(bufs, math.min(NodeStream.ChunkSize, NodeBuffer.room(bufs)))
      val size = chunk.length
      unsent += size
      sizes.push(size)
      try socket.write(new Uint8Array(chunk.buffer, chunk.byteOffset, size), sent): Unit
      catch {
        case js.JavaScriptException(failure) => // refused outright: its callback will not come
          unsent -= size
          sizes.pop()
          if error == null then error = NodeErr.io(failure)
          throw error.nn
      }
      size
    }
  }

  /** Ends the stream for writing: its end goes after what was written. */
  def shutdown(): Unit =
    if !shutDown then {
      shutDown = true
      if !closed then socket.end(): Unit
    }

  /** Closes the stream. What Node still holds of the bytes written is sent first, by Node, after this returns, as the
    * system sends what a closed socket's buffer holds.
    */
  def close(done: () => Unit): Boolean = {
    if !closed then
      if unsent > 0 && error == null then socket.destroySoon(): Unit
      else socket.destroy(): Unit
    true
  }
}

private[node] object NodeStream {

  /** The most a write hands Node at once. */
  inline val ChunkSize = 65536

  val ignore: js.Function1[js.Dynamic, Unit] = _ => ()
}
