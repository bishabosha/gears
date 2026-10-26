package gears.async.io
package kqueue

import KqueueBackend.Handle

/** Kqueue's kinds of handle (see `Ops`): opaque types over its `Handle`, so that a program holding them cannot pass one
  * kind where another is asked for. The backend's openers make them from the handle they open, with the conversions
  * here.
  */
object KqueueKinds {
  opaque type Readable <: Handle = Handle
  opaque type Writable <: Handle = Handle
  opaque type Connection <: Readable & Writable = Handle
  opaque type Listener <: Handle = Handle
  opaque type Datagram <: Handle = Handle
  opaque type UnixDatagram <: Handle = Handle
  opaque type File <: Readable & Writable = Handle

  private[kqueue] def readable(handle: Handle): Readable = handle
  private[kqueue] def writable(handle: Handle): Writable = handle
  private[kqueue] def connection(handle: Handle): Connection = handle
  private[kqueue] def listener(handle: Handle): Listener = handle
  private[kqueue] def datagram(handle: Handle): Datagram = handle
  private[kqueue] def unixDatagram(handle: Handle): UnixDatagram = handle
  private[kqueue] def file(handle: Handle): File = handle
}
