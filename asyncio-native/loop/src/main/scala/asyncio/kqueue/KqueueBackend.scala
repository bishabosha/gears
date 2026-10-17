package asyncio.kqueue

import asyncio.Backend
import asyncio.Handles
import asyncio.Ops
import asyncio.Reactor

/** Kqueue's reactors. They all run the same ops, which keep no state of one reactor between submissions, so an op may
  * go to any of them in turn; what kqueue keeps about a descriptor stays with the reactor that registered it.
  */
object KqueueBackend extends Backend {

  type Op = KqueueOp

  def ops: Ops[Op] = KqueueOps

  /** Kqueue watches POSIX file descriptors, so a handle is the descriptor itself. */
  def handles: Handles = KqueueHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  /** Opens a reactor with `KqueueReactor.open`'s defaults. */
  def open(): Reactor.Of[Op] = KqueueReactor.open()
}
