package gears.async.io
package kqueue

import gears.async.io.Blocking
import gears.async.io.BufferPool
import gears.async.io.Fifos
import gears.async.io.Files
import gears.async.io.Pipes
import gears.async.io.Reactor
import gears.async.io.ReactorBackend
import gears.async.io.Readiness
import gears.async.io.Resolving
import gears.async.io.UnixDatagrams
import gears.async.io.UnixStreams

/** Kqueue's reactors, with every capability a backend may have: IP, Unix stream and Unix datagram sockets, regular
  * files and name resolution on the blocker pool, pipes, readiness, blocking ops on blocker threads, and FIFOs. Kqueue
  * watches POSIX file descriptors, so a handle holds the descriptor. The reactors all run the same ops, which keep
  * no state of one reactor between submissions, so an op may go to any of them in turn; what kqueue keeps about a
  * descriptor stays with the reactor that registered it.
  */
object KqueueBackend
    extends ReactorBackend
    with Readiness
    with Blocking
    with UnixStreams
    with UnixDatagrams
    with Fifos
    with Files
    with Resolving
    with Pipes {
  type Op = KqueueOp

  /** An open descriptor, as this backend's ops name it: made only by the ops that open one (and `KqueueHandles`, for
    * kqueue's own code), so that a program cannot forge a handle from a number, and closed once, a second close
    * failing without touching the descriptor, whose number may by then be another's.
    */
  final class Handle private[kqueue] (private[kqueue] val fd: Int) {
    @volatile private var open = true

    /** Whether it has not been closed. */
    private[kqueue] def isOpen: Boolean = open

    /** Marks it closed; false if it already was. */
    private[kqueue] def markClosed(): Boolean = synchronized {
      val was = open
      open = false
      was
    }

    override def toString: String = s"Handle($fd)"
  }

  def ops: KqueueOps.type = KqueueOps
  def pool(bufferSize: Int, buffers: Int): BufferPool = new gears.async.io.internal.NativePool(bufferSize, buffers)

  /** Opens a reactor with `KqueueReactor.open`'s defaults. */
  def open(): Reactor.Of[Op, Handle] = KqueueReactor.open()
}
