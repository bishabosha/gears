package gears.async
import scala.collection.mutable
import scala.util.Success

import Future.Promise

/** A group of cancellable objects that are completed together. Cancelling the group means cancelling all its
  * uncompleted members.
  */
class CompletionGroup extends Cancellable.Tracking:
  private val members: mutable.Set[Cancellable] = mutable.Set()
  @volatile private var canceled: Boolean = false
  @volatile private var cancelWait: Promise[Unit] | Null = null

  /** The one awaiting member, if there is one (see `addAwaiter`). */
  private val awaiter = java.util.concurrent.atomic.AtomicReference[Cancellable | Null](null)

  /** Cancel all members */
  def cancel(): Unit =
    val toCancel = synchronized:
      if canceled then Seq.empty
      else
        canceled = true
        members.toSeq
    // After `canceled` is set: an awaiter that came too late to be read here reads that it is, and cancels itself.
    val waiting = awaiter.get()
    if waiting != null then waiting.cancel()
    toCancel.foreach(_.cancel())

  /** Wait for all members of the group to complete and unlink themselves. */
  private[async] def waitCompletion()(using Async): Unit =
    val wait = synchronized:
      // Set before the members are looked at: one that leaves meanwhile then either is seen gone, or sees this.
      if cancelWait == null then cancelWait = Promise()
      val waiting = cancelWait.nn
      if members.isEmpty && awaiter.get() == null then waiting.complete(Success(()))
      waiting
    wait.await
    unlink()

  /** Add given member to the members set. If the group has already been cancelled, cancels that member immediately. */
  def add(member: Cancellable): Unit =
    val alreadyCancelled = synchronized:
      members += member // Add this member no matter what since we'll wait for it still
      canceled
    if alreadyCancelled then member.cancel()

  /** Remove given member from the members set if it is an element */
  def drop(member: Cancellable): Unit = synchronized:
    members -= member
    if members.isEmpty && awaiter.get() == null && cancelWait != null then cancelWait.nn.complete(Success(()))

  /** As [[add]], for a member that is in the group only while it awaits, as the listener of an `await` is: the first
    * such member at a time costs no lock and no entry in the set, which is what a group whose body awaits one source
    * after another has.
    */
  private[async] def addAwaiter(member: Cancellable): Unit =
    if awaiter.compareAndSet(null, member) then { if canceled then member.cancel() }
    else add(member)

  /** As [[drop]], for a member added with [[addAwaiter]]. */
  private[async] def dropAwaiter(member: Cancellable): Unit =
    if awaiter.compareAndSet(member, null) then
      if cancelWait != null then
        synchronized:
          if members.isEmpty && awaiter.get() == null then cancelWait.nn.complete(Success(()))
    else drop(member)

  def isCancelled = canceled

object CompletionGroup:
  /** A sentinel group of cancellables that are in fact not linked to any real group. `cancel`, `add`, and `drop` do
    * nothing when called on this group.
    */
  object Unlinked extends CompletionGroup:
    override def cancel(): Unit = ()
    override def waitCompletion()(using Async): Unit = ()
    override def add(member: Cancellable): Unit = ()
    override def drop(member: Cancellable): Unit = ()
    override private[async] def addAwaiter(member: Cancellable): Unit = ()
    override private[async] def dropAwaiter(member: Cancellable): Unit = ()
  end Unlinked

end CompletionGroup
