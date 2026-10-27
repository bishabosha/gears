package gears.async.io
package nio

import gears.async.io.Blocking
import gears.async.io.BufferPool
import gears.async.io.Files
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.Reactor
import gears.async.io.ReactorBackend
import gears.async.io.Resolving
import gears.async.io.UnixStreams

/** What NIO can do, for both its backends: IP and Unix stream sockets, regular files and name resolution on the blocker
  * pool, pipes and blocking ops, but neither Unix datagram sockets nor FIFOs, which the JDK cannot wait on, nor
  * readiness. A handle holds the channel it names.
  */
trait NioCapabilities
    extends Blocking
    with IpStreams
    with IpDatagrams
    with UnixStreams
    with Files
    with Resolving
    with Pipes {
  type Op = NioOp
  type Handle = NioCapabilities.Handle
  type Readable = NioKinds.Readable
  type Writable = NioKinds.Writable
  type Connection = NioKinds.Connection
  type Listener = NioKinds.Listener
  type BoundDatagram = NioKinds.BoundDatagram
  type ConnectedDatagram = NioKinds.ConnectedDatagram
  type UnixBoundDatagram = NioKinds.UnixBoundDatagram
  type UnixConnectedDatagram = NioKinds.UnixConnectedDatagram
  type File = NioKinds.File
  def ops: NioOps.type = NioOps
  def pool(bufferSize: Int, buffers: Int): BufferPool = new DirectPool(bufferSize, buffers)
}

object NioCapabilities {

  /** An open channel, as NIO's ops name it, shared by its two backends, which run the same ops: made only by the ops
    * that open one, so that a program cannot forge a handle, and closed once, a second close failing.
    */
  final class Handle private[nio] (private[nio] val entry: Entry) {
    @volatile private var open = true

    // The ops pending on it, at most one reading and one writing, on any submitter: each claims its direction as it is
    // submitted, and lets it go as its completion is taken (see `Ops.alreadyPending`).
    private[nio] val reader = new java.util.concurrent.atomic.AtomicReference[AnyRef | Null](null)
    private[nio] val writer = new java.util.concurrent.atomic.AtomicReference[AnyRef | Null](null)

    /** Whether it has not been closed. */
    private[nio] def isOpen: Boolean = open

    /** Marks it closed; false if it already was. */
    private[nio] def markClosed(): Boolean = synchronized {
      val was = open
      open = false
      was
    }
  }
}

/** NIO's reactors, each over a `Selector` of its own. They all run the same ops, which keep no state of one reactor
  * between submissions, so an op may go to any of them in turn. A program over it runs on a pool carrying its futures,
  * on the JDK's internals; one on gears' own virtual threads uses `NioOnCaller` instead, the same ops and handles
  * performed on the calling thread.
  */
object NioBackend extends ReactorBackend with NioCapabilities {
  def open(): Reactor.Of[Op, Handle] = NioReactor.open()
}
