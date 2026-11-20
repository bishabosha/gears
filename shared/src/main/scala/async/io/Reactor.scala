package gears.async.io

import gears.async.Async
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Listener
import gears.async.SuspendSupport

import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/** EXPERIMENT.
  *
  * A submission queue with its use hidden: one future runs many flows, each ordinary direct-style code performing ops
  * on the reactor's `io`, as it would on any. There an op is tried at once, and one that has to wait is not waited for
  * by itself: it is in flight on the reactor's queue and its flow set aside, and the reactor, when no flow has more to do, performs the queue, with one wait for
  * all their ops, and resumes each flow whose op is done, on its own thread, one after another.
  *
  * So whatever is written against `Io.perform`, a protocol, a file's reader, goes through the queue with no change.
  * A flow is not a future: it is a continuation of the reactor's future, and nothing but the reactor resumes it. It has
  * an `Async` of its own, with which it awaits whatever a future may, a future it has spawned among them (in an
  * `Async.group`, as anywhere): the flow alone is set aside, and whoever completes what it awaits tells the reactor,
  * which resumes it between its ops. A future a flow spawns is a future as any other, on the scheduler, not a flow.
  */
private[io] final class Reactor[U <: Io[Interface] & Singleton] private (
    val under: U,
    val shard: Int,
    base: Async,
    queued: Boolean,
    /** Where a flow's op is performed when it is not put on the queue: `under`, or its view on the reactor's shard. */
    performing: Io[Interface]
) {

  val support: SuspendSupport = base.support

  private[io] final class Flow {
    var label: support.Label[Unit] | Null = null
    var suspension: support.Suspension[Unit, Unit] | Null = null
    var failure: Throwable | Null = null
  }

  /** A flow's `Async`: an await sets the flow aside, not the reactor's future. */
  private[io] final class FlowAsync(val flow: Flow, val group: CompletionGroup)
      extends Async(using base.support, base.scheduler) {
    def owner: Reactor[U] = Reactor.this
    def await[T](src: Async.Source[T]): T = awaitIn(flow, src, group)
    def withGroup(group: CompletionGroup): Async = new FlowAsync(flow, group)
  }

  /** What a flow waits with for a source: told by the source, or by the flow's group as it is cancelled, whichever is
    * first, on whatever thread; it has the reactor resume the flow.
    */
  private final class Awaiting[T](flow: Flow) extends Listener[T], Cancellable {
    private val decided = new AtomicBoolean(false)
    @volatile var item: T | Null = null
    @volatile var cancelled = false
    val lock: Listener.ListenerLock | Null = null

    def complete(data: T, source: Async.Source[T]): Unit =
      if decided.compareAndSet(false, true) then {
        item = data
        wake()
      }

    def cancel(): Unit =
      if decided.compareAndSet(false, true) then {
        cancelled = true
        wake()
      }

    private def wake(): Unit = {
      woken.add(flow): Unit
      signal()
    }
  }

  // ---- what flows of one reactor do among themselves, all on its thread, with no lock

  private val ready = new Ring[Flow]()
  private val dirty = new Ring[Outbox]()
  private var outFlying = 0

  private def flowOf(async: Async): Flow = async match {
    case mine: FlowAsync @unchecked if mine.owner eq Reactor.this => mine.flow
    case _ => throw new IllegalStateException("Only a flow of this reactor's")
  }

  private def setAside(flow: Flow): Unit =
    support.suspend[Unit, Unit](suspension => flow.suspension = suspension)(using flow.label.nn)

  /** Where flows of this reactor wait for one another: `park` sets the calling flow aside until another calls `wake`,
    * which has every flow parked here go on, in the order they parked, once the waker has no more to do.
    */
  final class Signal {
    private val parked = new Ring[Flow]()
    def park()(using async: Async): Unit = {
      val flow = flowOf(async)
      parked.add(flow)
      setAside(flow)
    }
    def wake(): Unit = {
      var flow = parked.poll()
      while flow != null do {
        ready.add(flow)
        flow = parked.poll()
      }
    }
  }

  /** What the flows of this reactor write to one handle, gathered: `put` copies a flow's bytes in and returns, and the
    * reactor, when no flow has more to do, writes all that was put since, with one op, however many flows put it and
    * however many times. Two buffers: one fills while the other is written. A flow that finds no room waits for a
    * write to end.
    */
  final class Outbox private[Reactor] (first: ByteBuffer, second: ByteBuffer, writeOf: ByteBuffer => under.Op) {
    private val bufs = Array(first, second)
    private val writes = Array[Any](writeOf(first), writeOf(second))
    private var filling = 0
    private var inFlight = false
    private var marked = false
    private var failure: Throwable | Null = null
    private val room = new Signal
    first.clear()
    second.clear()

    /** Adds what remains of `bytes`, whole and in the order of the calls. */
    def put(bytes: ByteBuffer)(using Async): Unit = {
      val length = bytes.remaining()
      require(length <= bufs(0).capacity(), s"$length bytes are more than the outbox holds")
      while failure == null && bufs(filling).remaining() < length do room.park()
      val failed = failure
      if failed != null then throw failed
      bufs(filling).put(bytes)
      if !marked then {
        marked = true
        dirty.add(this)
      }
    }

    /** Adds `length` bytes of `bytes`, whole and in the order of the calls: never mixed with another flow's. */
    def put(bytes: Array[Byte], from: Int, length: Int)(using Async): Unit = {
      require(length <= bufs(0).capacity(), s"$length bytes are more than the outbox holds")
      while failure == null && bufs(filling).remaining() < length do room.park()
      val failed = failure
      if failed != null then throw failed
      bufs(filling).put(bytes, from, length)
      if !marked then {
        marked = true
        dirty.add(this)
      }
    }

    /** Waits until all that was put has been written. */
    def drain()(using Async): Unit = {
      while failure == null && (inFlight || bufs(filling).position() > 0) do room.park()
      val failed = failure
      if failed != null then throw failed
    }

    /** No flow has more to do, or a write has ended: what has been put is written, if no write is under way. */
    private[Reactor] def flush(): Unit = {
      marked = false
      if !inFlight && failure == null && bufs(filling).position() > 0 then {
        bufs(filling).flip()
        filling = 1 - filling
        bufs(filling).clear()
        send()
      }
    }

    private def send(): Unit = {
      val which = 1 - filling
      if !queued then {
        // With no queue: a flow of its own performs the write.
        inFlight = true
        outFlying += 1
        fork(base.group) {
          while bufs(which).hasRemaining() do
            performing.performWithin(writes(which).asInstanceOf[performing.Op], Backend.NoTimeout)
        } { thrown =>
          inFlight = false
          outFlying -= 1
          if thrown != null then fail(thrown) else sent()
        }
      } else try {
        var done = true
        while done && bufs(which).hasRemaining() do
          done = under.startSubmitted(queue, writes(which).asInstanceOf[under.Op], this, Backend.NoTimeout)(using base)
        if done then sent()
        else {
          inFlight = true
          outFlying += 1
        }
      } catch case thrown: Throwable => fail(thrown)
    }

    /** The write in flight has ended. */
    private[Reactor] def written(thrown: Throwable | Null): Unit = {
      inFlight = false
      outFlying -= 1
      if thrown != null then fail(thrown) else send()
    }

    private def sent(): Unit = {
      room.wake()
      if bufs(filling).position() > 0 then flush()
    }

    private def fail(thrown: Throwable): Unit = {
      failure = thrown
      room.wake()
    }
  }

  /** An outbox for the handle `writeOf`'s ops write to, each the write of the buffer it is given. */
  def outbox(first: ByteBuffer, second: ByteBuffer)(writeOf: ByteBuffer => under.Op): Outbox =
    new Outbox(first, second, writeOf)

  /** Flows that were woken go on, and then what they put is written, until there is neither. */
  private def settle(): Unit =
    while !ready.isEmpty || !dirty.isEmpty do {
      var flow = ready.poll()
      while flow != null do {
        resume(flow)
        flow = ready.poll()
      }
      val outbox = dirty.poll()
      if outbox != null then outbox.flush()
    }

  // ---- what the reactor's future waits on when it has no queue: told from any thread that a flow can go on

  private val told = new AtomicBoolean(false)
  private val idle = new java.util.concurrent.atomic.AtomicReference[Listener[Unit] | Null](null)

  private object wakeups extends Async.Source[Unit] {
    def poll(k: Listener[Unit]): Boolean = told.get() && k.completeNow((), this)
    def onComplete(k: Listener[Unit]): Unit = {
      idle.set(k)
      if told.get() then rouse()
    }
    def dropListener(k: Listener[Unit]): Unit = idle.compareAndSet(k, null): Unit
  }

  private def rouse(): Unit = {
    val waiting = idle.getAndSet(null)
    if waiting != null then waiting.completeNow((), wakeups): Unit
  }

  private def signal(): Unit =
    if !queued then {
      told.set(true)
      rouse()
    } else queue.signal()

  private val woken = new ConcurrentLinkedQueue[Flow]()
  private var awaiting = 0

  private def awaitIn[T](flow: Flow, src: Async.Source[T], group: CompletionGroup): T =
    src.poll() match {
      case Some(item) => item
      case None       =>
        if group.isCancelled then throw new CancellationException()
        val waiter = new Awaiting[T](flow)
        waiter.link(group)
        src.onComplete(waiter)
        awaiting += 1
        support.suspend[Unit, Unit](suspension => flow.suspension = suspension)(using flow.label.nn)
        waiter.unlink()
        if waiter.cancelled then {
          src.dropListener(waiter)
          throw new CancellationException()
        }
        waiter.item.asInstanceOf[T]
    }

  private val queue = new SubmissionQueue[under.Op](shard)
  private val completed = new CompletionQueue[under.Op]
  private var live = 0
  private var failed: Throwable | Null = null

  /** The `Io` a flow performs its ops on: `under`'s ops and handles, performed through the reactor's queue. */
  val io: Io[under.ops.type] = new Io[under.ops.type] {
    val backend = under.backend.asInstanceOf[Backend { type Interface <: under.ops.type }]
    val ops: under.ops.type = under.ops

    private[io] def performWithin(op: Op, timeoutMillis: Long)(using async: Async): Unit =
      async match {
        case mine: FlowAsync @unchecked if mine.owner eq Reactor.this =>
          val flow = mine.flow
          // With no queue for its ops: performed as ever, with the flow's `Async`, so that the op's own wait sets the
          // flow aside, as any await of the flow's does, and whoever settles the op signals the reactor.
          if !queued then performing.performWithin(op.asInstanceOf[performing.Op], timeoutMillis)
          // Tried at once, which most often does a write: the flow goes on. One that has to wait is set aside.
          else if !under.startSubmitted(queue, op, flow, timeoutMillis) then {
            support.suspend[Unit, Unit](suspension => flow.suspension = suspension)(using flow.label.nn)
            val failure = flow.failure
            if failure != null then {
              flow.failure = null
              throw failure
            }
          }
        // Not a flow's, a future's that a flow spawned say: performed as ever.
        case _ => under.performWithin(op, timeoutMillis)
      }
  }

  /** Starts a flow: `body` runs at once, as far as its first op, and goes on as the reactor runs. */
  def spawn(body: Async ?=> Unit): Unit = {
    val flow = new Flow
    live += 1
    support.boundary[Unit] { label ?=>
      flow.label = label
      try body(using new FlowAsync(flow, base.group))
      catch case failure: Throwable => if failed == null then failed = failure
      live -= 1
    }
  }

  /** A flow begun by one of this reactor's flows, in `group`: `ended` is told how it ended, a failure or null, on the
    * reactor's thread. Its failure is its own, not the reactor's.
    */
  private[io] def fork(group: CompletionGroup)(body: Async ?=> Unit)(ended: Throwable | Null => Unit): Unit = {
    val flow = new Flow
    live += 1
    support.boundary[Unit] { label ?=>
      flow.label = label
      var failure: Throwable | Null = null
      try body(using new FlowAsync(flow, group))
      catch case thrown: Throwable => failure = thrown
      live -= 1
      ended(failure)
    }
  }

  // ---- from other threads: flows handed to this reactor, and its end

  private final class Handed(val begin: () => Unit)
  private val handed = new ConcurrentLinkedQueue[Handed]()
  @volatile private var open = false

  /** Keeps the reactor running though it has no flow, for flows to be handed to it, until `close`. Before `run`. */
  def keepOpen(): Unit = open = true

  /** Hands the reactor a flow to run, from any thread: it begins at the reactor's next turn. */
  def submit(body: Async ?=> Unit): Unit = {
    handed.add(new Handed(() => spawn(body))): Unit
    signal()
  }

  /** `fork`, begun not where the caller is but at the reactor's next turn, from its own loop: on a stack that is not
    * the caller's with more on top.
    */
  private[io] def forkLater(group: CompletionGroup)(body: Async ?=> Unit)(ended: Throwable | Null => Unit): Unit = {
    handed.add(new Handed(() => fork(group)(body)(ended))): Unit
    signal()
  }

  /** From any thread: the reactor ends once the flows it has have ended. */
  def close(): Unit = {
    open = false
    signal()
  }

  private def resume(flow: Flow): Unit = {
    val suspension = flow.suspension.nn
    flow.suspension = null
    suspension.resume(())
  }

  /** Runs the flows until every one has ended: performs what they have submitted, waits once for whatever of it has to
    * be waited for, and resumes those whose op is done. A flow's failure ends the reactor, thrown from here.
    */
  def run()(using Async): Unit = {
    settle()
    while (open || live > 0 || outFlying > 0 || !handed.isEmpty()) && failed == null do {
      // With flows that await what is not an op, or more flows to come, the queue waits though nothing of its own is
      // in flight.
      if !queued then {
        // No queue: every op is its flow's own, and the future waits to be told that one of them can go on.
        if !told.get() then summon[Async].await(wakeups)
        told.set(false)
      } else {
        queue.lingers = awaiting > 0 || open
        under.performSubmitted(queue, completed)
      }
      var i = 0
      while i < completed.count do {
        completed.tag(i) match {
          case outbox: Outbox @unchecked => outbox.written(completed.failure(i))
          case tag                       =>
            val flow = tag.asInstanceOf[Flow]
            flow.failure = completed.failure(i)
            resume(flow)
        }
        i += 1
      }
      completed.clear()
      var flow = woken.poll()
      while flow != null do {
        awaiting -= 1
        resume(flow)
        flow = woken.poll()
      }
      var arrival = handed.poll()
      while arrival != null do {
        arrival.begin()
        arrival = handed.poll()
      }
      settle()
    }
    val failure = failed
    if failure != null then throw failure
  }
}

object Reactor {

  /** A reactor of `io`'s, whose queue is of `shard` (see `SubmissionQueue`), for the caller's future to run. */
  inline val Queued = 0
  inline val Unqueued = 1
  inline val Pinned = 2

  /** A pinned reactor is one with no queue over the `Io`'s view on its shard (`Shards.onShard`). */
  def apply(io: Io[Interface], shard: Int, mode: Int = Queued)(using async: Async): Reactor[io.type] = {
    val performing: Io[Interface] =
      if mode == Pinned then Shards.onShard(io.asInstanceOf[Io[Shards]])(shard) else io
    new Reactor[io.type](io, shard, async, mode == Queued, performing)
  }

  /** A reactor with no submission queue at all, over the least an `Io` can give it: an op performed as ever but
    * listened for by a named shard's thread (`Shards.onShard`), so that what settles it, and so what resumes the
    * reactor's future, is the reactor's own shard. The future waits on a source of the reactor's own.
    */
  def pinned(io: Io[Interface], shard: Int)(using async: Async): Reactor[io.type] =
    apply(io, shard, Pinned)

  /** To compare: a reactor that does not put its flows' ops on its queue. Each is performed as `Io.perform` performs
    * it, its wait the flow's own; the queue is only what the reactor's future waits on to be told that a flow can go
    * on. What a reactor is over an `Io` that has no submission queues, given some way for its future to wait.
    */
  def unqueued(io: Io[Interface], shard: Int)(using async: Async): Reactor[io.type] =
    apply(io, shard, Unqueued)

  /** The reactor whose flow `async` is the `Async` of; null if it is not a flow's. */
  def of(async: Async): Reactor[?] | Null = async match {
    case flow: Reactor[?]#FlowAsync @unchecked => flow.owner
    case _                                     => null
  }
}
