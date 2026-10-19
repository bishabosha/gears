package asyncio.nio

import asyncio.Reactor
import asyncio.UnixStreamBackend

/** NIO's reactors, each over a `Selector` of its own. They all run the same ops, which keep no state of one reactor
  * between submissions, so an op may go to any of them in turn, and handles name entries of one process-wide table.
  * Besides what every backend has, they have Unix stream sockets, but neither Unix datagram sockets nor FIFOs, which the
  * JDK cannot wait on.
  */
object NioBackend extends UnixStreamBackend {

  type Op = NioOp

  def ops: NioOps.type = NioOps

  def handles: NioHandles.type = NioHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  def open(): Reactor.Of[Op] = NioReactor.open()
}
