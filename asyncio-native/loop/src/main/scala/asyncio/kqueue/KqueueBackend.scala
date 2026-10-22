package asyncio.kqueue

import asyncio.BlockingBackend
import asyncio.BufferPool
import asyncio.FifoBackend
import asyncio.FileBackend
import asyncio.PipeBackend
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.ReadinessBackend
import asyncio.ResolveBackend
import asyncio.UnixDatagramBackend
import asyncio.UnixStreamBackend

/** Kqueue's reactors. They all run the same ops, which keep no state of one reactor between submissions, so an op may
  * go to any of them in turn; what kqueue keeps about a descriptor stays with the reactor that registered it. They have
  * every capability a backend may: IP, Unix stream and Unix datagram sockets, regular files and name resolution on
  * the blocker pool, pipes, readiness, blocking ops on blocker threads, and FIFOs.
  */
object KqueueBackend
    extends ReactorBackend
    with ReadinessBackend
    with BlockingBackend
    with UnixStreamBackend
    with UnixDatagramBackend
    with FifoBackend
    with FileBackend
    with ResolveBackend
    with PipeBackend {

  type Op = KqueueOp

  def ops: KqueueOps.type = KqueueOps

  /** Kqueue watches POSIX file descriptors, so a handle is the descriptor itself. */
  def handles: KqueueHandles.type = KqueueHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  /** Opens a reactor with `KqueueReactor.open`'s defaults. */
  def open(): Reactor.Of[Op] = KqueueReactor.open()

  def pool(bufferSize: Int, buffers: Int): BufferPool = new asyncio.unsafe.NativePool(bufferSize, buffers)
}
