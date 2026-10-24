package gears.async.io
package nio

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import NioCapabilities.Handle
import gears.async.io.Repeatable

/** The file ops (see `FileOps`), each a blocking task on the blocker pool, since `FileChannel` blocks for the disk:
  * opening, positional reads and writes, and `force`.
  */

/** Opens a regular file and publishes its handle (see `FileOps.open`). A cancel that arrives once the file is open
  * closes it again, since the handle never reaches the caller.
  */
final class OpenFile(path: String, write: Boolean, into: Slot[Handle]) extends NioBlocking {
  def block(): Unit = {
    val handle = NioHandles.openFile(path, write)
    if !unlessCancelled(into.set(handle)) then NioHandles.close(handle)
  }
}

/** A file op on the channel of a regular file: the handle is resolved when the task runs, so that one closed meanwhile
  * fails the op.
  */
sealed abstract class FileTask(val handle: Handle) extends NioBlocking with Repeatable {
  protected def channel: FileChannel = NioHandles.entry(handle) match {
    case file: RegularFile => file.channel
    case other             => throw new IOException(s"Handle $handle is not a regular file")
  }
}

/** A read at an offset (see `FileOps.read`): fills `buf` from the file at the op's offset, which each completion
  * advances by the bytes read, so that submitting the op again goes on from there.
  */
final class PositionalRead(handle: Handle, buf: ByteBuffer, private var offset: Long) extends FileTask(handle) {
  def block(): Unit = {
    val read = channel.read(buf, offset) // advances the buffer's position itself; -1 at the end
    if read > 0 then unlessCancelled(offset += read): Unit
  }
}

/** A write at an offset (see `FileOps.write`): drains `buf` into the file at the op's offset, which each completion
  * advances by the bytes written.
  */
final class PositionalWrite(handle: Handle, buf: ByteBuffer, private var offset: Long) extends FileTask(handle) {
  def block(): Unit = {
    val written = channel.write(buf, offset)
    if written > 0 then unlessCancelled(offset += written): Unit
  }
}

/** `force` (see `FileOps.sync`). */
final class FileSync(handle: Handle) extends FileTask(handle) {
  def block(): Unit = channel.force(false)
}
