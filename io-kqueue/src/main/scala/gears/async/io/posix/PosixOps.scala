package gears.async.io.posix

import gears.async.io.shareable.BaseReadableHandle
import gears.async.io.shareable.NonBlockingReadOps

import java.nio.ByteBuffer

trait PosixReadOps extends NonBlockingReadOps:
  type ReadableHandle <: PosixKernelHandle & BaseReadableHandle
  final def _readHandle(handle: ReadableHandle, buf: ByteBuffer): Int =
    NonBlocking.read(handle.fd, buf)
