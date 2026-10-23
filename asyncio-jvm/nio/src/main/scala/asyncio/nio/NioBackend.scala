package asyncio.nio

import asyncio.BlockingBackend
import asyncio.BufferPool
import asyncio.FileBackend
import asyncio.PipeBackend
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.ResolveBackend
import asyncio.UnixStreamBackend

/** NIO's reactors, each over a `Selector` of its own. They all run the same ops, which keep no state of one reactor
  * between submissions, so an op may go to any of them in turn, and handles name entries of one process-wide table.
  * They have IP and Unix stream sockets, regular files and name resolution on the blocker pool, pipes and blocking
  * ops, but neither Unix datagram sockets nor FIFOs, which the JDK cannot wait on. A program on gears' own virtual
  * threads, which runs on no reactor, uses `NioOnCaller` instead: the same ops and handles, performed on the calling
  * thread.
  */
object NioBackend
    extends ReactorBackend
    with BlockingBackend
    with UnixStreamBackend
    with FileBackend
    with ResolveBackend
    with PipeBackend {

  type Op = NioOp

  def ops: NioOps.type = NioOps

  def handles: NioHandles.type = NioHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  def open(): Reactor.Of[Op] = NioReactor.open()

  def pool(bufferSize: Int, buffers: Int): BufferPool = new DirectPool(bufferSize, buffers)

}
