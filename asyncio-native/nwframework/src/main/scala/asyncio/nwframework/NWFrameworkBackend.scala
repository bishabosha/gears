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

/** Reactors over Apple's Network.framework, for IP sockets, and Dispatch, for pipes and FIFOs (see
  * `NWFrameworkReactor`), with IP sockets, regular files and name resolution on the shared blocker pool, pipes,
  * blocking ops, and FIFOs; no readiness, since Network.framework reports completions and nothing can be tried on a
  * connection without waiting, and no Unix domain sockets, which Network.framework's public interface has no endpoint
  * for. Its ops keep no state of one reactor between submissions, so an op may go to any of its reactors in turn.
  */
object NWFrameworkBackend
    extends ReactorBackend
    with IpSockets
    with Blocking
    with Fifos
    with Files
    with Resolving
    with Pipes {
  type Op = NWFrameworkOp
  def ops: NWFrameworkOps.type = NWFrameworkOps
  def handles: NWFrameworkHandles.type = NWFrameworkHandles
  def pool(bufferSize: Int, buffers: Int): BufferPool = new asyncio.unsafe.NativePool(bufferSize, buffers)
  def open(): Reactor.Of[Op] = NWFrameworkReactor.open()
}
