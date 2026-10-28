package gears.async.io
package nwframework

import gears.async.io.Blocking
import gears.async.io.BufferPool
import gears.async.io.Fifos
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.RegularFiles
import gears.async.io.Resolving
import gears.async.io.UnixDatagrams
import gears.async.io.UnixStreams
import gears.async.io.internal.Reactor
import gears.async.io.internal.ReactorBackend
import gears.async.io.kqueue.KqueueBackend
import gears.async.io.kqueue.KqueueOp
import gears.async.io.kqueue.KqueueReactor

/** Kqueue's reactors with Network.framework where it serves best, as Apple advises (TN3151): its TCP connections and
  * listeners, and UDP between two peers, a connected datagram socket; and kqueue's own code for the rest, which
  * Network.framework does not do or does no better: Unix domain sockets, bound datagram sockets, pipes, FIFOs, regular
  * files, name resolution and blocking ops, as `KqueueBackend` has them (see `NWFrameworkOps`). Each reactor is a
  * `KqueueReactor`, woken for Network.framework's callbacks, which come on Dispatch's threads, by a user event on its
  * kqueue (see `NWFrameworkExtension`). An op may go to any of its reactors in turn.
  */
sealed abstract class NWFrameworkBackends private[nwframework] (loopbackOnKqueue: Boolean)
    extends ReactorBackend
    with Blocking
    with IpStreams
    with IpDatagrams
    with UnixStreams
    with UnixDatagrams
    with Fifos
    with RegularFiles
    with Resolving
    with Pipes {
  type Op = KqueueOp

  /** Kqueue's handle: a descriptor, or a Network.framework object as its foreign one (`KqueueBackend.Handle`). */
  type Handle = KqueueBackend.Handle
  type Readable = NWFrameworkKinds.Readable
  type Writable = NWFrameworkKinds.Writable
  type Connection = NWFrameworkKinds.Connection
  type Listener = NWFrameworkKinds.Listener
  type BoundDatagram = NWFrameworkKinds.BoundDatagram
  type ConnectedDatagram = NWFrameworkKinds.ConnectedDatagram
  type UnixBoundDatagram = NWFrameworkKinds.UnixBoundDatagram
  type UnixConnectedDatagram = NWFrameworkKinds.UnixConnectedDatagram
  type File = NWFrameworkKinds.File

  val ops: NWFrameworkOps = new NWFrameworkOps(loopbackOnKqueue)
  def pool(bufferSize: Int, buffers: Int): BufferPool = new gears.async.io.internal.NativePool(bufferSize, buffers)
  def open(): Reactor.Of[Op, Handle] = KqueueReactor.openFor(ops)
}

/** Sockets of loopback addresses on kqueue, as Unix domain sockets are, since Network.framework carries loopback over
  * the kernel's own sockets at several times kqueue's cost.
  */
object NWFrameworkBackend extends NWFrameworkBackends(loopbackOnKqueue = true) {

  /** Every IP socket on Network.framework, loopback's too: for checking and measuring Network.framework itself, whose
    * checks run on loopback.
    */
  object Everywhere extends NWFrameworkBackends(loopbackOnKqueue = false)
}
