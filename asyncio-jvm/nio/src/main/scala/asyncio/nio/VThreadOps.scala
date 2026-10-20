package asyncio.nio

import gears.async.Async
import gears.async.Cancellable
import gears.async.cancellationScope

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedSelectorException
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

import asyncio.HandleSlot
import asyncio.Interest
import asyncio.IpAddress
import asyncio.ReadinessOps
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot

/** Everything `VThreadBackend` performs: its `Op` type. An op runs on the thread that performs it, a virtual thread,
  * over `NioHandles`' non-blocking channels: it tries its operation, and while that would block, yields and tries again
  * (`VThreadOp.Yields` times), then waits on the handle's own selector for that direction (`Selectable.enterWait`), in
  * the virtual thread itself, which the JDK parks until the selector is ready. A timer parks until its deadline.
  *
  * While it runs, the op is linked to the caller's group as a `Cancellable`: cancelling it sets a flag and wakes the
  * wait with `Selector.wakeup`, or unparks a timer, and the op gives up with `CancellationException`, leaving the
  * channel open. Nothing is ever interrupted.
  */
sealed abstract class VThreadOp extends Cancellable {

  /** Does the op on the calling thread, returning once it is done; throws `CancellationException` once cancelled. */
  protected def run(): Unit

  /** Whether the op may be performed again once it has returned. */
  protected def repeatable: Boolean = true

  // `Idle` until performed, `Running` while a thread performs it, then `Idle` again, or `Spent` if not repeatable.
  private val state = new AtomicInteger(VThreadOp.Idle)

  // Set by `cancel`, from any thread; checked by the performing thread whenever it would wait, and after.
  @volatile private var cancelled = false

  // The selector the performing thread waits on, if it does, which `cancel` wakes; otherwise, the thread to unpark.
  @volatile private var waitingOn: Selector | Null = null
  @volatile private var performer: Thread | Null = null

  /** Runs the op on the calling thread, which `VThreadBackend.perform` has checked. */
  private[nio] final def perform()(using Async): Unit = {
    if !state.compareAndSet(VThreadOp.Idle, VThreadOp.Running) then
      throw new IllegalStateException(
        if state.get() == VThreadOp.Spent then s"$this is not Repeatable and was already performed"
        else s"$this is already being performed"
      )
    cancelled = false
    performer = Thread.currentThread()
    try cancellationScope(this)(run())
    finally {
      performer = null
      state.set(if repeatable then VThreadOp.Idle else VThreadOp.Spent)
    }
  }

  /** Makes the op give up: a wait it is in, or its next, ends at once. Called by the caller's group when it is
    * cancelled. A wake-up that comes after the op has finished only makes the selector's next select return at once,
    * and its op try again.
    */
  def cancel(): Unit = {
    cancelled = true
    // `waitingOn` is set before the waiting thread checks `cancelled`, and `cancelled` before this reads it, so either
    // that thread sees the flag, or this sees the selector.
    val selector = waitingOn
    if selector != null then selector.wakeup(): Unit
    else {
      val thread = performer
      if thread != null then LockSupport.unpark(thread)
    }
  }

  protected final def checkCancelled(): Unit = if cancelled then throw new CancellationException()

  /** Tries `attempt` until it says the op is done: while it would block, yields and tries again, `yields` times, then
    * waits on `entry`'s selector for `ops` before each try. An attempt that succeeds after a cancel still counts. It
    * never spins on its carrier: a yield puts the thread behind every other one ready to run, which is what keeps
    * this fair.
    */
  protected final inline def until(entry: Selectable, write: Boolean, ops: Int, yields: Int = VThreadOp.Yields)(
      inline attempt: Boolean
  ): Unit = {
    var yielded = 0
    while !attempt do {
      checkCancelled()
      if yielded < yields then {
        yielded += 1
        Thread.`yield`()
      } else await(entry, write, ops)
    }
  }

  /** Waits until `entry`'s channel is ready for `ops`, or the op is cancelled, on the channel's own selector for
    * that direction. A closed handle's selector is closed too, which ends the wait; the next attempt finds the channel
    * closed.
    */
  protected final def await(entry: Selectable, write: Boolean, ops: Int): Unit = {
    val selector = entry.enterWait(write)
    try {
      val key = entry.channel.keyFor(selector)
      if key != null && key.interestOps() != ops then key.interestOps(ops): Unit
      waitingOn = selector
      if !cancelled then selector.select(VThreadOp.Ignore): Unit
    } catch {
      case _: ClosedSelectorException | _: java.nio.channels.CancelledKeyException => ()
    } finally {
      waitingOn = null
      entry.exitWait()
    }
    checkCancelled()
  }
}

private[nio] object VThreadOp {
  inline val Idle = 0
  inline val Running = 1
  inline val Spent = 2

  /** Tries an op makes, yielding between them, before waiting on a selector (`-Dasyncio.vthread.yields`): other
    * virtual threads run meanwhile, and the data often arrives, so most ops never wait, each wait costing two
    * `kevent`s on the selector and a registration with the JDK's poller. A yield puts the thread behind every other
    * one ready to run, so the others keep their turns; with none ready it returns at once. Chosen with the fairness
    * scenarios (see the NIO TODO, item 10).
    */
  val Yields: Int = Integer.getInteger("asyncio.vthread.yields", 8).intValue

  /** For `select`'s action: the op tries again itself, so the selected keys need nothing done. */
  val Ignore: java.util.function.Consumer[SelectionKey] = _ => ()
}

/** A wait on the calling thread for a handle to be ready for `ops`, on the handle's own selector for that direction
  * (see `Selectable.enterWait`): `ReactorBackend.waitHere` for NIO, as `VThreadOp.await` waits. Cancelled with
  * `Selector.wakeup`, so the handle stays open.
  */
private[nio] final class SelectorWait(entry: Selectable, write: Boolean, ops: Int) extends asyncio.WaitHere {
  @volatile private var cancelled = false
  @volatile private var waitingOn: Selector | Null = null

  /** Never finishes the op itself: the caller tries it once the selector reports the handle ready. */
  def await(): Boolean = {
    val selector = entry.enterWait(write)
    try {
      val key = entry.channel.keyFor(selector)
      if key != null && key.interestOps() != ops then key.interestOps(ops): Unit
      waitingOn = selector
      if !cancelled then selector.select(VThreadOp.Ignore): Unit
    } catch {
      case _: ClosedSelectorException | _: java.nio.channels.CancelledKeyException => ()
    } finally {
      waitingOn = null
      entry.exitWait()
    }
    false
  }

  def cancel(): Unit = {
    cancelled = true
    val selector = waitingOn
    if selector != null then selector.wakeup(): Unit
  }
}

/** A timer waited for on the calling thread, parked until its time has passed: `ReactorBackend.waitHere` for NIO's
  * timers, which contend for nothing, so a caller waits this way whatever else is waiting. Cancelled with an unpark.
  */
private[nio] final class TimerWait(milliseconds: Int) extends asyncio.WaitHere {
  private val deadline = System.nanoTime() + milliseconds * 1_000_000L
  @volatile private var cancelled = false
  @volatile private var parked: Thread | Null = null

  /** Parks until the deadline, or a cancel; true once the time has passed. */
  def await(): Boolean = {
    parked = Thread.currentThread()
    try {
      var remaining = deadline - System.nanoTime()
      while remaining > 0 && !cancelled do {
        LockSupport.parkNanos(this, remaining)
        remaining = deadline - System.nanoTime()
      }
      remaining <= 0
    } finally parked = null
  }

  def cancel(): Unit = {
    cancelled = true
    val thread = parked
    if thread != null then LockSupport.unpark(thread)
  }
}

/** Builds the ops `VThreadBackend` performs. */
object VThreadOps extends ReadinessOps[VThreadOp] {
  def read(handle: Int, buf: ByteBuffer): VThreadRead = new VThreadRead(handle, buf)
  def write(handle: Int, buf: ByteBuffer): VThreadWrite = new VThreadWrite(handle, buf)
  def accept(handle: Int, into: HandleSlot): VThreadAccept = new VThreadAccept(handle, into)
  def connect(handle: Int): VThreadOp = new VThreadConnect(handle)
  def receive(handle: Int, buf: ByteBuffer, from: Slot[IpAddress]): VThreadReceive =
    new VThreadReceive(handle, buf, from)
  def send(handle: Int, buf: ByteBuffer, to: IpAddress): VThreadSend = new VThreadSend(handle, buf, to)
  def send(handle: Int, buf: ByteBuffer): VThreadSend = new VThreadSend(handle, buf, null)
  def timer(milliseconds: Int): VThreadTimer = new VThreadTimer(milliseconds)
  def whenReady(handle: Int, interest: Interest)(perform: () => Boolean): VThreadWhenReady =
    new VThreadWhenReady(handle, interest, perform)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): VThreadOp = new VThreadResolve(host, into)
}

/** The selectable entry `handle` names, or null for a regular file, which is always ready. */
private def selectableOrFile(handle: Int, op: String): Selectable | Null = NioHandles.entry(handle) match {
  case selectable: Selectable => selectable
  case _: RegularFile         => null
  case other                  => throw new IOException(s"$op cannot use $other")
}

/** Reads into `buf` between position and limit: done once at least one byte has been read, or at end of stream. */
final class VThreadRead(handle: Int, buf: ByteBuffer) extends VThreadOp with Repeatable {
  protected def run(): Unit = {
    val entry = selectableOrFile(handle, "Reading")
    if entry == null then NioHandles.entry(handle).read(buf): Unit
    else until(entry, write = false, SelectionKey.OP_READ)(entry.read(buf) != 0)
  }
}

/** Writes `buf` between position and limit: done once at least one byte has been written. */
final class VThreadWrite(handle: Int, buf: ByteBuffer) extends VThreadOp with Repeatable {
  protected def run(): Unit = {
    val entry = selectableOrFile(handle, "Writing")
    if entry == null then NioHandles.entry(handle).write(buf): Unit
    else until(entry, write = true, SelectionKey.OP_WRITE)(entry.write(buf) >= 0 || !buf.hasRemaining)
  }
}

/** Accepts one connection on a listener, writing the new, non-blocking connection's handle into `into`. */
final class VThreadAccept(handle: Int, into: HandleSlot) extends VThreadOp with Repeatable {
  protected def run(): Unit = {
    val entry = NioHandles.entry(handle) match {
      case s: Selectable => s
      case other         => throw new IOException(s"Accepting needs a listener, not $other")
    }
    val server = entry.as(classOf[ServerSocketChannel], "Accepting")
    // A listener usually waits long for its next connection, so it does not yield before waiting: a yield wakes an
    // idle carrier.
    until(entry, write = false, SelectionKey.OP_ACCEPT, yields = 0) {
      val client = server.accept()
      if client != null then {
        try client.configureBlocking(false)
        catch {
          case t: Throwable =>
            client.close()
            throw t
        }
        into.set(NioHandles.add(Selectable(client, null)))
      }
      client != null
    }
  }
}

/** Finishes the non-blocking connect `NioHandles.connect` started. */
final class VThreadConnect(handle: Int) extends VThreadOp {
  override protected def repeatable: Boolean = false
  protected def run(): Unit = {
    val entry = NioHandles.entry(handle) match {
      case s: Selectable => s
      case other         => throw new IOException(s"Connecting needs a socket, not $other")
    }
    val channel = entry.as(classOf[SocketChannel], "Connecting")
    try until(entry, write = true, SelectionKey.OP_CONNECT, yields = 0)(channel.isConnected || channel.finishConnect())
    catch case e: java.net.ConnectException => throw NioHandles.connectFailed(e)
  }
}

/** Receives one packet into `buf`, left flipped for reading, and writes its sender into `from`. */
final class VThreadReceive(handle: Int, buf: ByteBuffer, from: Slot[IpAddress]) extends VThreadOp with Repeatable {
  protected def run(): Unit = {
    val entry = NioHandles.entry(handle) match {
      case s: Selectable => s
      case other         => throw new IOException(s"Receiving needs a datagram socket, not $other")
    }
    val channel = entry.as(classOf[DatagramChannel], "Receiving")
    until(entry, write = false, SelectionKey.OP_READ) {
      buf.clear()
      val sender = channel.receive(buf)
      if sender != null then {
        buf.flip()
        from.set(NioHandles.ipAddress(sender))
      }
      sender != null
    }
  }
}

/** Sends `buf` as one packet, to `to` or to the connected peer when `to` is null, consuming it. */
final class VThreadSend(handle: Int, buf: ByteBuffer, to: IpAddress | Null) extends VThreadOp with Repeatable {
  private val target = if to == null then null else NioHandles.socketAddress(to)
  protected def run(): Unit = {
    val entry = NioHandles.entry(handle) match {
      case s: Selectable => s
      case other         => throw new IOException(s"Sending needs a datagram socket, not $other")
    }
    val channel = entry.as(classOf[DatagramChannel], "Sending")
    val size = buf.remaining()
    // As NIO's: a connected channel sends nothing for an empty buffer, through `write` or `send` alike.
    if target == null && size == 0 then
      throw new IOException("The JDK cannot send an empty datagram on a connected socket")
    until(entry, write = true, SelectionKey.OP_WRITE) {
      val sent = if target != null then channel.send(buf, target) else channel.write(buf)
      sent > 0 || size == 0
    }
  }
}

/** Runs `body` once `handle` is ready for `interest`, until it returns true: false means it was not ready after all,
  * and the op waits again. It runs on the performing thread.
  */
final class VThreadWhenReady(handle: Int, interest: Interest, body: () => Boolean) extends VThreadOp with Repeatable {
  protected def run(): Unit = {
    val entry = NioHandles.entry(handle) match {
      case s: Selectable => s
      case _: RegularFile => null // always ready
      case other          => throw new IOException(s"Waiting needs a channel, not $other")
    }
    if entry == null then { while !body() do checkCancelled() }
    else {
      val write = interest == Interest.Write
      // Reading a listener is accepting.
      val ops =
        if write then SelectionKey.OP_WRITE
        else if entry.channel.isInstanceOf[ServerSocketChannel] then SelectionKey.OP_ACCEPT
        else SelectionKey.OP_READ
      var done = false
      while !done do {
        await(entry, write, ops)
        done = body()
      }
    }
  }
}

/** Parks until `milliseconds` have passed; a cancel unparks it. A timer of no time yields, so that a loop of them
  * lets every other thread ready to run have its turn, as it would on a reactor.
  */
final class VThreadTimer(val milliseconds: Int) extends VThreadOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")
  protected def run(): Unit = {
    if milliseconds == 0 then Thread.`yield`()
    val deadline = System.nanoTime() + milliseconds * 1_000_000L
    var remaining = deadline - System.nanoTime()
    while remaining > 0 do {
      checkCancelled()
      LockSupport.parkNanos(this, remaining)
      remaining = deadline - System.nanoTime()
    }
  }
}

/** Resolves a host name with `InetAddress`, writing its addresses into `into`. The lookup cannot be stopped, so a
  * cancel during it is seen once it returns, and leaves `into` empty, as on a reactor.
  */
final class VThreadResolve(host: String, into: Slot[List[ResolvedAddress]]) extends VThreadOp {
  override protected def repeatable: Boolean = false
  protected def run(): Unit = {
    val addresses = Resolve.addresses(host)
    checkCancelled()
    into.set(addresses)
  }
}
