package gears.async.io.posix

import gears.async.io.shareable.PipeOpener

import _root_.gears.async.io.shareable.AbstractBaseHandle
import _root_.gears.async.io.shareable.BaseReadableHandle
import _root_.gears.async.io.shareable.BaseWritableHandle

abstract class PosixKernelHandle(val fd: Int) extends AbstractBaseHandle:
  open() // construction is acquisition of the handle

  def cleanup(): Unit = Descriptors.closeHandle(fd)

trait PosixPipeOpener extends PipeOpener:
  type PipeReadable <: PosixKernelHandle & BaseReadableHandle
  type PipeWritable <: PosixKernelHandle & BaseWritableHandle

  def makeReadableHandle(fd: Int): PipeReadable
  def makeWritableHandle(fd: Int): PipeWritable

  def pipe(): (read: PipeReadable, write: PipeWritable) =
    val (readFd, writeFd) = Descriptors.pipe()
    (makeReadableHandle(readFd), makeWritableHandle(writeFd))
