package asyncio.nwframework

import asyncio.Blocking
import asyncio.BufferPool
import asyncio.Fifos
import asyncio.Files
import asyncio.Pipes
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.Resolving
import asyncio.IpSockets
import asyncio.Pool
import asyncio.unsafe.Pooled

/** What Network.framework can do, as the backend and as a program's `Io` over it: IP sockets, regular files and name
  * resolution on the shared blocker pool, pipes, blocking ops, and FIFOs; no readiness, since Network.framework
  * reports completions and nothing can be tried on a connection without waiting, and no Unix domain sockets, which
  * Network.framework's public interface has no endpoint for.
  */
trait NWFrameworkCapabilities extends IpSockets with Blocking with Fifos with Files with Resolving with Pipes {
  type Op = NWFrameworkOp
  type Io = NWFrameworkOnPool
  def ops: NWFrameworkOps.type = NWFrameworkOps
  def handles: NWFrameworkHandles.type = NWFrameworkHandles
  def pool(bufferSize: Int, buffers: Int): BufferPool = new asyncio.unsafe.NativePool(bufferSize, buffers)
}

/** Reactors over Apple's Network.framework, for IP sockets, and Dispatch, for pipes and FIFOs (see `NWFrameworkReactor`).
  * Its ops keep no state of one reactor between submissions, so an op may go to any of its reactors in turn.
  */
object NWFrameworkBackend extends ReactorBackend with NWFrameworkCapabilities {
  def open(): Reactor.Of[Op] = NWFrameworkReactor.open()
  private[asyncio] def bind(pool: Pool.Of[Op]): NWFrameworkOnPool = new NWFrameworkOnPool(pool)
}

/** A program's `Io` over Network.framework: `NWFrameworkBackend` bound to the pool the program runs on. */
final class NWFrameworkOnPool private[nwframework] (protected val pool: Pool.Of[NWFrameworkOp])
    extends Pooled
    with NWFrameworkCapabilities {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
}
