package gears.async.io

import java.nio.ByteBuffer

/** How much of a readable handle's stream a program holds at once through the reads into buffers of the backend's
  * (`ByteStreams.read` into a slot): at most `buffers` buffers, of at most `bufferSize` bytes each. The buffers are the
  * backend's, as the memory behind them is, made and freed as it sees fit; what is the program's is how many it may
  * hold, each handle's own, so that what one stream's reader has not released holds back that stream and no other.
  * Every readable handle has a window from the moment it opens, of `ReadWindow.DefaultBuffers` buffers of
  * `ReadWindow.DefaultBufferSize` bytes, which costs nothing until a read uses it, and which
  * `ByteStreams.setReadWindow` changes.
  *
  * `bufferSize` is the most one such read receives: a backend that allocates the buffers fills them, where one whose
  * facility hands it chunks of its own, as Node's streams do, may hand such a chunk over as the `Received` instead, so
  * long as it is no larger.
  */
trait ReadWindow {
  def bufferSize: Int
  def buffers: Int

  /** The buffers not held by an unreleased `Received`: with none, a read waits for one to be released. */
  def available: Int
}

object ReadWindow {

  /** The size of a buffer of a handle's window, unless it is set. */
  inline val DefaultBufferSize = 65536

  /** How many buffers a handle's window has, unless it is set. */
  inline val DefaultBuffers = 4
}

/** Bytes a read into a buffer of the backend's received: `buffer` is one of its handle's window, positioned at 0 with
  * its limit at the bytes received, 0 at the end of the stream. It is the caller's until `release` gives it back to the
  * window, which must happen exactly once, from any thread, and which lets a read waiting for the window go on; the
  * `Received` is not to be used after. It outlives its handle: closing the handle does not take a buffer the caller
  * still holds, which is released as any other.
  */
trait Received {
  def buffer: ByteBuffer
  def release(): Unit
}
