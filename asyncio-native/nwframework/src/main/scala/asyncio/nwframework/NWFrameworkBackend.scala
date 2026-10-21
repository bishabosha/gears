package asyncio.nwframework

import asyncio.BlockingBackend
import asyncio.FifoBackend
import asyncio.Reactor
import asyncio.ReactorBackend

/** Reactors over Apple's Network.framework, for IP sockets, and Dispatch, for pipes and FIFOs (see `NWFrameworkReactor`).
  * Its ops keep no state of one reactor between submissions, so an op may go to any of its reactors in turn. It has
  * blocking ops, on the shared blocker pool, and FIFOs; it has no readiness, since Network.framework reports
  * completions and nothing can be tried on a connection without waiting, and no Unix domain sockets, which
  * Network.framework's public interface has no endpoint for.
  */
object NWFrameworkBackend extends ReactorBackend with BlockingBackend with FifoBackend {

  type Op = NWFrameworkOp

  def ops: NWFrameworkOps.type = NWFrameworkOps

  def handles: NWFrameworkHandles.type = NWFrameworkHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  def open(): Reactor.Of[Op] = NWFrameworkReactor.open()
}
