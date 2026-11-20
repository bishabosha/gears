package gears.async.io.shareable

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/** The tasks given to a worker that it has not come to yet: put in from any thread, taken out in turn by the worker,
  * and, when it has said it has more than it will come to soon, half at a time by a worker with nothing to do.
  */
private[shareable] final class Inbox {
  private val tasks = new ConcurrentLinkedQueue[Runnable]

  /** How many tasks are in the inbox, or about to be: what tells one task waiting its turn from a backlog. */
  private val queued = new AtomicInteger(0)

  /** Whether the worker has tasks to spare: set as it calls for a thief, and until a turn leaves little behind. */
  @volatile private var surplus = false

  /** When the worker last came to have tasks to spare, as `System.nanoTime` has it: written before `surplus` is set. */
  @volatile private var surplusSince = 0L

  def isEmpty: Boolean = tasks.isEmpty()

  def size: Int = queued.get()

  /** Puts `task` last, from any thread: how many were there before it. */
  def add(task: Runnable): Int = {
    val before = queued.getAndIncrement()
    tasks.add(task)
    before
  }

  /** The next task, for the worker or a thief, or null. */
  def take(): Runnable | Null = {
    val task = tasks.poll()
    if task != null then queued.decrementAndGet()
    task
  }

  /** Whether `task` was still here, and has been taken back. */
  def takeBack(task: Runnable): Boolean = {
    val removed = tasks.remove(task)
    if removed then queued.decrementAndGet()
    removed
  }

  def declareSurplus(): Unit = if !surplus then declareSurplus(System.nanoTime())

  /** As `declareSurplus`, at `now`: the time is read only when there was nothing to spare before. */
  def declareSurplus(now: Long): Unit =
    if !surplus then
      surplusSince = now
      surplus = true

  /** The worker has tasks it has shown it will not come to soon: what a turn left behind, or futures it is handing on
    * with too many to resume. They are a thief's at once, with no moment for the worker to come to them first.
    */
  def declareBacklog(): Unit =
    surplusSince = System.nanoTime() - Inbox.StealDelayNanos
    surplus = true

  def clearSurplus(): Unit = if surplus then surplus = false

  /** How many tasks the worker has to spare: none unless it has said it has some. A thief may take half of them once
    * they have been to spare for `Inbox.StealDelayNanos` (see `stealableIn`).
    */
  def spare: Int = if surplus then queued.get() else 0

  /** How long from `now` until a thief may take what is to spare: none, zero or less, once the worker has had it to
    * spare for `Inbox.StealDelayNanos`. Until then the tasks are the worker's own to come to: futures just spawned
    * together most often work with each other, each running a moment and waiting for the next, and a worker that gets
    * to them at once keeps them together, where a thief that took one would have every message between them cross from
    * one thread to another. What the worker has not come to in that time, as when a task keeps it, is shared out.
    */
  def stealableIn(now: Long): Long = Inbox.StealDelayNanos - (now - surplusSince)

  /** For a worker with nothing to do, on its own thread, whose inbox `thief` is: half of the tasks here, the oldest, if
    * the worker has said it has some to spare. One is returned for the thief to run now, and the rest are put in
    * `thief`; null if there is nothing to take. Half at once, not one at a time: a thief that came back for each would
    * get few of them before the worker had run the rest, and how many would be down to how the two happened to
    * interleave. A worker that is merely awake does not take what another is about to run.
    */
  def takeHalfInto(thief: Inbox): Runnable | Null = takeHalfInto(thief, System.nanoTime())

  /** As `takeHalfInto`, at `now`. */
  def takeHalfInto(thief: Inbox, now: Long): Runnable | Null =
    if !surplus || stealableIn(now) > 0 then null
    else
      var more = (queued.get() + 1) / 2 - 1 // besides the first
      val first = take()
      if first != null then
        while more > 0 do
          val task = take()
          if task == null then more = 0
          else
            thief.add(task): Unit
            more -= 1
      first
}

private[shareable] object Inbox {

  /** How long a worker has had tasks to spare before a thief may take them (see `Inbox.stealableIn`): longer than a
    * worker takes to come to a few futures that each wait at once, and nothing beside a task worth another thread.
    */
  inline val StealDelayNanos = 50_000L
}
