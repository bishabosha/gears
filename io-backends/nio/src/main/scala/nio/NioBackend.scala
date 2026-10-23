package asyncio.nio

import asyncio.Blocking
import asyncio.BufferPool
import asyncio.Files
import asyncio.Pipes
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.Resolving
import asyncio.UnixStreams

/** What NIO can do, for both its backends: IP and Unix stream sockets, regular files and name
  * resolution on the blocker pool, pipes and blocking ops, but neither Unix datagram sockets nor FIFOs, which the JDK
  * cannot wait on, nor readiness. Handles name entries of one process-wide table.
  */
trait NioCapabilities extends Blocking with UnixStreams with Files with Resolving with Pipes {
  type Op = NioOp
  def ops: NioOps.type = NioOps
  def handles: NioHandles.type = NioHandles
  def pool(bufferSize: Int, buffers: Int): BufferPool = new DirectPool(bufferSize, buffers)
}

/** NIO's reactors, each over a `Selector` of its own. They all run the same ops, which keep no state of one reactor
  * between submissions, so an op may go to any of them in turn. A program over it runs on a pool carrying its
  * futures, on the JDK's internals; one on gears' own virtual threads uses `NioOnCaller` instead, the same ops and
  * handles performed on the calling thread.
  */
object NioBackend extends ReactorBackend with NioCapabilities {
  def open(): Reactor.Of[Op] = NioReactor.open()
}
