package gears.async.io
package nwframework

import gears.async.io.Blocking
import gears.async.io.BufferPool
import gears.async.io.Fifos
import gears.async.io.Files
import gears.async.io.IpSockets
import gears.async.io.Pipes
import gears.async.io.Reactor
import gears.async.io.ReactorBackend
import gears.async.io.Resolving

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

  /** An open I/O object, as this backend's ops name it: made only by the ops that open one, so that a program cannot
    * forge a handle, and closed once, a second close failing.
    */
  final class Handle private[nwframework] (private[nwframework] val target: NWFrameworkHandle) {
    @volatile private var open = true

    /** Whether it has not been closed. */
    private[nwframework] def isOpen: Boolean = open

    /** Marks it closed; false if it already was. */
    private[nwframework] def markClosed(): Boolean = synchronized {
      val was = open
      open = false
      was
    }
  }
  def ops: NWFrameworkOps.type = NWFrameworkOps
  def pool(bufferSize: Int, buffers: Int): BufferPool = new gears.async.io.internal.NativePool(bufferSize, buffers)
  def open(): Reactor.Of[Op, Handle] = NWFrameworkReactor.open()
}
