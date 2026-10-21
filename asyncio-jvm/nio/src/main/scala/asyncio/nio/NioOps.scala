package asyncio.nio

import java.io.IOException
import java.net.Inet4Address
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

import asyncio.AddressFamily
import asyncio.BlockingOps
import asyncio.BufferPool
import asyncio.LifecycleOps
import asyncio.PooledReadOps
import asyncio.Received
import asyncio.Completion
import asyncio.HandleSlot
import asyncio.Interest
import asyncio.IpAddress
import asyncio.ReadinessOps
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot

/** Everything `NioReactor` can run: its `Op` member type. NIO-specific code may construct the classes below directly;
  * code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait NioOp {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[nio] var submitted = false

  /** While the op is pending in the reactor: its completion, so the op is its own record. Taken off just before the
    * completion runs, which may then submit the op again. Loop thread only.
    */
  private[nio] var completion: Completion[NioOp] | Null = null

  /** For a command that failed as soon as it was submitted: its failure, until its completion runs. */
  private[nio] var atOnceFailure: Throwable | Null = null

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[nio] var attachment: AnyRef | Null = null
}

/** Builds the ops `NioReactor` understands. */
object NioOps extends ReadinessOps[NioOp] with BlockingOps[NioOp] with PooledReadOps[NioOp] with LifecycleOps[NioOp] {
  def read(handle: Int, buf: ByteBuffer): Read = new Read(handle, buf)
  def write(handle: Int, buf: ByteBuffer): Write = new Write(handle, buf)
  def accept(handle: Int, into: HandleSlot): Accept = new Accept(handle, into)
  def connect(handle: Int): NioOp = new Connect(handle)
  def receive(handle: Int, buf: ByteBuffer, from: Slot[IpAddress]): Receive = new Receive(handle, buf, from)
  def send(handle: Int, buf: ByteBuffer, to: IpAddress): Send = new Send(handle, buf, to)
  def send(handle: Int, buf: ByteBuffer): Send = new Send(handle, buf, null)
  def timer(milliseconds: Int): NioTimer = new NioTimer(milliseconds)
  def whenReady(handle: Int, interest: Interest)(perform: () => Boolean): WhenReady =
    new WhenReady(handle, interest, perform)
  def blocking(body: () => Unit): NioBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NioBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Int, pool: BufferPool, into: Slot[Received]): PooledRead = new PooledRead(handle, NioOps.direct(pool), into)
  def listening(handle: Int): NioOp = new Listening(handle)
  def close(handle: Int): NioOp = new Close(handle)

  private def direct(pool: BufferPool): DirectPool = pool match {
    case direct: DirectPool => direct
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op the reactor performs as soon as it is submitted, with no handle to wait for: its completion runs on the
  * next turn, with the failure `perform` threw, if any. One-shot.
  */
sealed abstract class NioAtOnce extends NioOp {
  def perform(): Unit
}

/** Binds a listener from `NioHandles.listener` (see `LifecycleOps.listening`). */
final class Listening(val handle: Int) extends NioAtOnce {
  def perform(): Unit = NioHandles.bind(handle)
}

/** Closes a handle (see `LifecycleOps.close`): at once, as `NioHandles.close` does. */
final class Close(val handle: Int) extends NioAtOnce {
  def perform(): Unit = NioHandles.close(handle)
}

/** What to do with a handle once it is ready. `perform` runs a non-blocking operation on the reactor thread, on the
  * handle's entry, which the reactor looks up when the command is submitted; it returns whether the op is done, and an
  * exception it throws is the op's failure. Commands are `Repeatable`.
  */
abstract class Command extends NioOp with Repeatable {
  def handle: Int
  def interest: Interest
  def perform(): Boolean

  /** Whether the reactor may perform this command as soon as it is submitted, before waiting for readiness. */
  def tryFirst: Boolean = true

  /** The interest ops to wait for on `channel`: reading a listener is accepting. */
  def selectOps(channel: SelectableChannel): Int =
    if interest == Interest.Write then SelectionKey.OP_WRITE
    else if channel.isInstanceOf[ServerSocketChannel] then SelectionKey.OP_ACCEPT
    else SelectionKey.OP_READ

  /** The entry of the current submission, looked up by the reactor. */
  private[nio] var entry: Entry | Null = null

  /** Whether `NioBackend.tryNow` just tried this command and it would have blocked: its next submission skips trying. */
  private[nio] var triedNow = false

  /** The key of the selector this command last waited on, kept so that waiting there again needs no lookup. */
  private[nio] var key: SelectionKey | Null = null

  protected final def selectable[C <: SelectableChannel](kind: Class[C], op: String): C = entry match {
    case s: Selectable => s.as(kind, op)
    case other         => throw new IOException(s"$op needs a socket, not $other")
  }
}

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream. */
final class Read(val handle: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = entry.nn.read(buf) != 0
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `PooledReadOps.read`), as `Read` fills the caller's. */
final class PooledRead(val handle: Int, pool: DirectPool, into: Slot[Received]) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val slot = pool.take()
    if slot == null then throw new IOException("The pool has no buffer free")
    val buffer = slot.nn.buffer
    val read = entry.nn.read(buffer)
    if read == 0 then {
      slot.nn.release()
      false
    } else {
      buffer.flip()
      into.set(slot.nn)
      true
    }
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written. */
final class Write(val handle: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Write
  def perform(): Boolean = entry.nn.write(buf) >= 0 || !buf.hasRemaining()
}

/** Accepts one connection on a listener, writing the new connection's handle into `into`. */
final class Accept(val handle: Int, into: HandleSlot) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val client = selectable(classOf[ServerSocketChannel], "Accepting").accept()
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

/** Waits for a non-blocking connect to finish. Done once connected; throws if it failed. A connection that finished at
  * once, as a Unix socket's usually does, is done as soon as it is submitted.
  */
final class Connect(val handle: Int) extends Command {
  def interest: Interest = Interest.Write
  override def selectOps(channel: SelectableChannel): Int = SelectionKey.OP_CONNECT
  def perform(): Boolean = {
    val channel = selectable(classOf[SocketChannel], "Connecting")
    try channel.isConnected || channel.finishConnect()
    catch case e: java.net.ConnectException => throw NioHandles.connectFailed(e)
  }
}

/** Receives one packet into `buf`, left flipped for reading, and writes its sender into `from`. */
final class Receive(val handle: Int, buf: ByteBuffer, from: Slot[IpAddress]) extends Command {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    buf.clear()
    val sender = selectable(classOf[DatagramChannel], "Receiving").receive(buf)
    if sender != null then {
      buf.flip()
      from.set(NioHandles.ipAddress(sender))
    }
    sender != null
  }
}

/** Sends `buf` as one packet, to `to` or to the connected peer when `to` is null, consuming it. */
final class Send(val handle: Int, buf: ByteBuffer, to: IpAddress | Null) extends Command {
  def interest: Interest = Interest.Write
  private val target = if to == null then null else NioHandles.socketAddress(to)
  def perform(): Boolean = {
    val channel = selectable(classOf[DatagramChannel], "Sending")
    val size = buf.remaining()
    // A connected channel sends nothing for an empty buffer, through `write` or `send` alike, and says it sent it.
    if target == null && size == 0 then
      throw new java.io.IOException("The JDK cannot send an empty datagram on a connected socket")
    val sent = if target != null then channel.send(buf, target) else channel.write(buf)
    // A datagram goes whole or not at all; an empty one reports 0 bytes either way.
    sent > 0 || size == 0
  }
}

/** A command built from a function, for `ReadinessOps.whenReady`: it runs only once the handle is ready. */
final class WhenReady(val handle: Int, val interest: Interest, body: () => Boolean) extends Command {
  def perform(): Boolean = body()
  override def tryFirst: Boolean = false
}

/** A one-shot timer, kept in its reactor's heap while pending. */
final class NioTimer(val milliseconds: Int) extends NioOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  // The reactor's bookkeeping for a pending submission, on the loop thread: when it is due (`System.nanoTime`), its
  // place in submission order, and its index in the reactor's timer heap (-1 when not pending).
  private[nio] var deadline = 0L
  private[nio] var order = 0L
  private[nio] var heapIndex = -1
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `NioBlocking`, the only kind, completes itself from the blocker pool.
  */
sealed abstract class NioPromise extends NioOp {
  // `Unsettled` until the first completion or failure wins the compare-and-set; later ones are refused.
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](NioPromise.Unsettled)
  @volatile private[nio] var reactor: NioReactor | Null = null

  private[nio] def failure: Throwable | Null = outcome.get() match {
    case NioPromise.Failed(failure) => failure
    case _                          => null
  }

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = settle(NioPromise.Failed(failure))

  /** Tells the reactor this promise has finished, without a value. */
  private[nio] def signal(): Unit = settle(())

  /** Records the outcome, if this is the first, and tells the reactor this promise has finished. */
  protected final def settle(value: Any): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    if !outcome.compareAndSet(NioPromise.Unsettled, value) then
      throw new IllegalStateException("A promise can only be completed once")
    owner.resolved(this)
  }
}

object NioPromise {
  private[nio] object Unsettled
  private[nio] final case class Failed(failure: Throwable)
}

/** A promise the reactor completes by running `block` on the blocker pool (`NioBlocking.blockers`). An exception thrown by `block` is the op's
  * failure. Cancelling it skips `block` if it has not started, and interrupts its thread if it is running; either way
  * the completion's `onCancel` waits until `block` has returned. As with the kqueue backend's blocking ops, a subclass
  * writes its result into its slot through `publish`, which skips the write once cancelled.
  */
abstract class NioBlocking extends NioPromise {
  def block(): Unit

  // Guarded by `this`: the thread running `block`, whether the task was cancelled, and whether `block` has returned.
  private var runner: Thread | Null = null
  private var cancelled = false
  private var ended = false

  /** The cancelled submission whose `onCancel` waits for `block` to return. Loop thread only. */
  private[nio] var cancelling: Completion[NioOp] | Null = null

  /** Runs `block` on the blocker pool and completes the promise with the outcome, unless it is cancelled first. */
  private[nio] def start(): Unit =
    NioBlocking.blockers.execute { () =>
      if begin() then {
        var failure: Throwable | Null = null
        try block()
        catch case t: Throwable => failure = t
        finally end()
        // A cancelled task is detached, so neither outcome reaches its completion, but the reactor learns from it that
        // `block` has returned, which is when its `onCancel` runs.
        if failure != null then fail(failure) else signal()
      }
    }

  /** Runs this op on the calling thread instead of the blocker pool (see `ReactorBackend.blockingHere`). */
  private[nio] def here(): asyncio.BlockingHere = {
    if submitted then throw new IllegalStateException(s"$this is not Repeatable and was already submitted")
    submitted = true
    new asyncio.BlockingHere {
      def run(): Unit = {
        if !begin() then throw new java.util.concurrent.CancellationException()
        var failure: Throwable | Null = null
        try block()
        catch case t: Throwable => failure = t
        finally end()
        if synchronized(cancelled) then throw new java.util.concurrent.CancellationException()
        if failure != null then throw failure
      }
      def cancel(): Unit = interrupt(): Unit
    }
  }

  private def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Once this returns no cancel can interrupt the thread, so an interrupt that raced in is cleared, and cannot leak
    * into the thread's next task.
    */
  private def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted(): Unit
  }

  /** Marks the task cancelled and interrupts its thread if it is running. Returns true if it was running. */
  private[nio] def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  /** Writes `value` into `into` unless the task has been cancelled. */
  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then into.set(value)
  }

  /** Waits until a running `block` has returned. An interrupt of the waiting thread is kept for later. */
  private[nio] def awaitEnd(): Unit = {
    var interrupted = false
    synchronized {
      while !ended do
        try wait()
        catch case _: InterruptedException => interrupted = true
    }
    if interrupted then Thread.currentThread().interrupt()
  }
}

private[nio] object NioBlocking {

  /** The blocker pool: daemon platform threads, made as needed and kept a while. Not virtual threads, since one started
    * from a virtual thread inherits its scheduler: from a future carried by a reactor pool (`Carriers.Reactors`), a
    * blocking op would then take one of the pool's workers, and a reactor closing on that worker would wait for it
    * forever.
    */
  lazy val blockers: java.util.concurrent.ExecutorService =
    java.util.concurrent.Executors.newCachedThreadPool { (task: Runnable) =>
      val thread = new Thread(task, s"nio-blocker-${count.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }

  private val count = new java.util.concurrent.atomic.AtomicInteger
}

/** A blocking task built from a function, for `BlockingOps.blocking`. */
final class BlockingTask(body: () => Unit) extends NioBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `BlockingOps.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends NioBlocking {
  def block(): Unit = publish(into, body())
}

/** Resolves a host name with `InetAddress`, which blocks, on the blocker pool. */
final class Resolve(val host: String, into: Slot[List[ResolvedAddress]]) extends NioBlocking {
  def block(): Unit = publish(into, Resolve.addresses(host))
}

private[nio] object Resolve {

  /** `host`'s addresses, in numeric form, looked up with `InetAddress`, which blocks. */
  def addresses(host: String): List[ResolvedAddress] = {
    val addresses =
      try InetAddress.getAllByName(host).toList
      catch case e: IOException => throw new IOException(s"Failed to resolve $host: ${e.getMessage}", e)
    addresses.map {
      case v4: Inet4Address => ResolvedAddress(AddressFamily.IPv4, v4.getHostAddress)
      case v6               =>
        val text = v6.getHostAddress
        val scope = text.indexOf('%')
        ResolvedAddress(AddressFamily.IPv6, if scope < 0 then text else text.substring(0, scope))
    }
  }
}
