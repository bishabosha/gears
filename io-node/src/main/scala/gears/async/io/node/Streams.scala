package gears.async.io
package node

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalajs.js.typedarray.Int8Array

/** Where a stream's bytes come from, as Node has them: what has arrived and is ready to take, whether the end has, and
  * the one read waiting for either.
  */
private[node] trait Source {

  /** The bytes that may be taken now. */
  def available: Int

  /** Takes at least one byte and at most `max`, of those available. */
  def take(max: Int): Int8Array

  /** Whether the end of the stream has come: nothing follows what is available. */
  def ended: Boolean

  /** What reads fail with once nothing is available, if the stream has failed. */
  def failure: IOException | Null

  private var waiter: (() => Unit) | Null = null

  /** Has `wake` called once, and forgotten, when bytes, the end or a failure comes; null for no call. */
  def onChange(wake: (() => Unit) | Null): Unit = waiter = wake

  /** Called once a read has taken the end of the stream, which a source that goes on after one acts on. */
  def endTaken(): Unit = ()

  /** Calls the read waiting, if there is one. */
  protected def wakeReader(): Unit = {
    val woken = waiter
    waiter = null
    if woken != null then woken()
  }
}

/** Where a stream's bytes go, as Node takes them: how many it takes now, and the one write waiting for room. */
private[node] trait Sink {

  /** The bytes that may be put now: none while what was put before has not gone. */
  def room: Int

  /** Puts `chunk`, no longer than `room`, which is the sink's from here on. */
  def put(chunk: Int8Array): Unit

  /** What writes of bytes fail with, if the stream takes no more: its reader gone, or shut down for writing. */
  def refusal: IOException | Null

  /** Called once, and forgotten, when there is room, or writes begin to fail. */
  var writer: (() => Unit) | Null = null

  /** Calls the waiting write, if there is one. */
  protected def wakeWriter(): Unit = {
    val woken = writer
    writer = null
    if woken != null then woken()
  }
}

/** A handle read as a stream. */
trait ReadEnd extends NodeHandle {

  /** Begins a read of at most `max` bytes for `run`, of `op`: once bytes have come, or the end has, calls `accept`,
    * which may refuse them with a failure, before any is taken, then `deliver` with them, null at the end, and finishes
    * `run`.
    */
  private[node] def read(run: Run, op: NodeOp, max: Int, accept: () => Unit, deliver: (Int8Array | Null) => Unit): Unit
}

/** A handle written as a stream. */
trait WriteEnd extends NodeHandle {

  /** Begins a write of `bufs`, which have bytes remaining, for `run`, of `op`: once at least one byte has been taken
    * from them, in order, finishes `run`.
    */
  private[node] def write(run: Run, op: NodeOp, bufs: Array[ByteBuffer]): Unit
}

/** A read end over a source of Node's: one read at a time, which takes what is available at once, and else waits. */
private[node] trait SourceEnd extends ReadEnd {
  private[node] def source: Source
  private var reading = false

  private[node] def read(
      run: Run,
      op: NodeOp,
      max: Int,
      accept: () => Unit,
      deliver: (Int8Array | Null) => Unit
  ): Unit = {
    if reading then
      throw new IllegalArgumentException(s"An op reading, as $op does, is already pending on its handle, $this")
    val from = source

    /** Finishes the read if it can be: true if it has. */
    def attempt(): Boolean =
      if from.available > 0 then {
        accept()
        deliver(from.take(max))
        run.succeed()
        true
      } else if from.failure != null then {
        run.fail(from.failure.nn)
        true
      } else if from.ended then {
        accept()
        deliver(null)
        from.endTaken()
        run.succeed()
        true
      } else false

    def waitForMore(): Unit = from.onChange { () =>
      try if !run.done && !attempt() then waitForMore()
      catch case failure: Throwable => run.fail(failure)
    }

    if !attempt() then {
      reading = true
      run.release = () => {
        reading = false
        from.onChange(null)
      }
      waitForMore()
    }
  }
}

/** A write end over a sink of Node's: one write at a time, which puts what there is room for at once, and else waits.
  */
private[node] trait SinkEnd extends WriteEnd {
  private[node] def sink: Sink
  private var writing = false

  private[node] def write(run: Run, op: NodeOp, bufs: Array[ByteBuffer]): Unit = {
    if writing then
      throw new IllegalArgumentException(s"An op writing, as $op does, is already pending on its handle, $this")
    val into = sink

    def attempt(): Boolean =
      if into.refusal != null then {
        run.fail(into.refusal.nn)
        true
      } else {
        val room = into.room
        if room > 0 then {
          into.put(Buffers.drain(bufs, math.min(room, Buffers.room(bufs))))
          run.succeed()
          true
        } else false
      }

    def waitForRoom(): Unit = into.writer = () =>
      try if !run.done && !attempt() then waitForRoom()
      catch case failure: Throwable => run.fail(failure)

    if !attempt() then {
      writing = true
      run.release = () => {
        writing = false
        into.writer = null
      }
      waitForRoom()
    }
  }
}

/** A read into the caller's buffers, one or several, filled in order. */
private[node] final class ReadOp(on: ReadEnd, bufs: Array[ByteBuffer]) extends NodeOp with Repeatable {
  override protected def handle: NodeHandle = on

  override protected def check(): Unit =
    if Buffers.remaining(bufs) == 0 then
      throw new IllegalArgumentException(
        "A read into a buffer with no room could only add nothing, which means the end of the stream"
      )

  protected def start(run: Run): Unit =
    on.read(run, this, Buffers.room(bufs), () => (), chunk => if chunk != null then Buffers.fill(bufs, chunk))
}

/** A read that hands over a chunk of Node's as a buffer of the pool's. */
private[node] final class PooledReadOp(on: ReadEnd, pool: NodePool, into: Slot[Received])
    extends NodeOp
    with Repeatable {
  override protected def slots: List[Slot[?]] = List(into)
  override protected def handle: NodeHandle = on

  protected def start(run: Run): Unit =
    on.read(
      run,
      this,
      pool.bufferSize,
      () => {
        pool.checkFree()
        if !into.isEmpty then throw new IllegalArgumentException("The read's slot was filled while it waited")
      },
      chunk => into.set(pool.received(chunk))
    )
}

/** A write from the caller's buffers, one or several, drained in order. */
private[node] final class WriteOp(on: WriteEnd, bufs: Array[ByteBuffer]) extends NodeOp with Repeatable {
  override protected def handle: NodeHandle = on

  protected def start(run: Run): Unit =
    if Buffers.remaining(bufs) == 0 then run.succeed() else on.write(run, this, bufs)
}

/** A close of a handle. */
private[node] final class CloseOp(on: NodeHandle) extends NodeOp {
  protected def start(run: Run): Unit = on.close(run)
}
