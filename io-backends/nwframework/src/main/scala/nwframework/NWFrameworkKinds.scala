package gears.async.io
package nwframework

import NWFrameworkBackend.Handle

/** Network.framework's kinds of handle (see `Ops`): opaque types over its `Handle`, so that a program holding them
  * cannot pass one kind where another is asked for. The openers make them from the handle they open, with the
  * conversions here.
  */
object NWFrameworkKinds {
  opaque type Readable <: Handle = Handle
  opaque type Writable <: Handle = Handle
  opaque type Connection <: Readable & Writable = Handle
  opaque type Listener <: Handle = Handle
  opaque type Datagram <: Handle = Handle
  opaque type UnixDatagram <: Handle = Handle
  opaque type File <: Readable & Writable = Handle

  private[nwframework] def readable(handle: Handle): Readable = handle
  private[nwframework] def writable(handle: Handle): Writable = handle
  private[nwframework] def connection(handle: Handle): Connection = handle
  private[nwframework] def listener(handle: Handle): Listener = handle
  private[nwframework] def datagram(handle: Handle): Datagram = handle
  private[nwframework] def file(handle: Handle): File = handle
}
