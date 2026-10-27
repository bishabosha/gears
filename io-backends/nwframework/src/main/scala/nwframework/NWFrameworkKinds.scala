package gears.async.io
package nwframework

import gears.async.io.kqueue.KqueueBackend.Handle

/** The backend's kinds of handle (see `Ops`): opaque types over kqueue's `Handle`, whatever is underneath, a
  * Network.framework object or a descriptor, so that a program holding them cannot pass one kind where another is asked
  * for, nor one to kqueue's ops. The openers make them from the handle they open, with the conversions here.
  */
object NWFrameworkKinds {
  opaque type Readable <: Handle = Handle
  opaque type Writable <: Handle = Handle
  opaque type Connection <: Readable & Writable = Handle
  opaque type Listener <: Handle = Handle
  opaque type BoundDatagram <: Handle = Handle
  opaque type ConnectedDatagram <: Handle = Handle
  opaque type UnixBoundDatagram <: Handle = Handle
  opaque type UnixConnectedDatagram <: Handle = Handle
  opaque type File <: Readable & Writable = Handle

  private[nwframework] def connection(handle: Handle): Connection = handle
  private[nwframework] def listener(handle: Handle): Listener = handle
  private[nwframework] def connectedDatagram(handle: Handle): ConnectedDatagram = handle
}
