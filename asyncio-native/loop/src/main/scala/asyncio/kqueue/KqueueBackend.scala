package asyncio.kqueue

import asyncio.Blocking
import asyncio.BufferPool
import asyncio.Fifos
import asyncio.Files
import asyncio.Pipes
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.Readiness
import asyncio.Resolving
import asyncio.UnixDatagrams
import asyncio.UnixStreams
import asyncio.Pool
import asyncio.unsafe.Pooled

/** What kqueue can do, as the backend and as a program's `Io` over it: every capability a backend may have: IP, Unix
  * stream and Unix datagram sockets, regular files and name resolution on the blocker pool, pipes, readiness, blocking
  * ops on blocker threads, and FIFOs. Kqueue watches POSIX file descriptors, so a handle is the descriptor itself.
  */
trait KqueueCapabilities
    extends Readiness
    with Blocking
    with UnixStreams
    with UnixDatagrams
    with Fifos
    with Files
    with Resolving
    with Pipes {
  type Op = KqueueOp
  type Io = KqueueOnPool
  def ops: KqueueOps.type = KqueueOps
  def handles: KqueueHandles.type = KqueueHandles
  def pool(bufferSize: Int, buffers: Int): BufferPool = new asyncio.unsafe.NativePool(bufferSize, buffers)
}

/** Kqueue's reactors. They all run the same ops, which keep no state of one reactor between submissions, so an op may
  * go to any of them in turn; what kqueue keeps about a descriptor stays with the reactor that registered it.
  */
object KqueueBackend extends ReactorBackend with KqueueCapabilities {
  /** Opens a reactor with `KqueueReactor.open`'s defaults. */
  def open(): Reactor.Of[Op] = KqueueReactor.open()

  private[asyncio] def bind(pool: Pool.Of[Op]): KqueueOnPool = new KqueueOnPool(pool)
}

/** A program's `Io` over kqueue: `KqueueBackend` bound to the pool the program runs on. */
final class KqueueOnPool private[kqueue] (protected val pool: Pool.Of[KqueueOp])
    extends Pooled
    with KqueueCapabilities {
  val backend: KqueueBackend.type = KqueueBackend
}
