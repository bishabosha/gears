package asyncio.nio

import asyncio.Blocking
import asyncio.BufferPool
import asyncio.Files
import asyncio.Pipes
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.Resolving
import asyncio.UnixStreams
import asyncio.Pool
import asyncio.unsafe.Pooled

/** What NIO can do, for both its backends and their `Io`s: IP and Unix stream sockets, regular files and name
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
  type Io = NioOnPool
  def open(): Reactor.Of[Op] = NioReactor.open()
  private[asyncio] def bind(pool: Pool.Of[Op]): NioOnPool = new NioOnPool(pool)
}

/** A program's `Io` over NIO's reactors: `NioBackend` bound to the pool the program runs on. */
final class NioOnPool private[nio] (protected val pool: Pool.Of[NioOp]) extends Pooled with NioCapabilities {
  type Io = NioOnPool
  val backend: NioBackend.type = NioBackend
}
