package gears.async.io
package nio

import NioCapabilities.Handle

/** NIO's kinds of handle (see `Ops`), shared by its two backends: opaque types over its `Handle`, so that a program
  * holding them cannot pass one kind where another is asked for. The openers make them from the handle they open, with
  * the conversions here.
  */
object NioKinds {
  opaque type Readable <: Handle = Handle
  opaque type Writable <: Handle = Handle
  opaque type Connection <: Readable & Writable = Handle
  opaque type Listener <: Handle = Handle
  opaque type Datagram <: Handle = Handle
  opaque type UnixDatagram <: Handle = Handle
  opaque type File <: Readable & Writable = Handle

  private[nio] def readable(handle: Handle): Readable = handle
  private[nio] def writable(handle: Handle): Writable = handle
  private[nio] def connection(handle: Handle): Connection = handle
  private[nio] def listener(handle: Handle): Listener = handle
  private[nio] def datagram(handle: Handle): Datagram = handle
  private[nio] def file(handle: Handle): File = handle
}
