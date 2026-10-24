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
    * kqueue's own code), so that a program cannot forge a handle from a number. Every use of the descriptor (a
    * command's system call and registration, a file op on a blocker thread, `readNow` and `writeNow`) takes the handle
    * for its length (`acquire`), which fails once the handle is closed, so that a stale handle fails its op rather
    * than reaching whatever reuses the descriptor's number; and closing marks it closed first, then waits for the
    * uses in progress to end before the descriptor is closed, so that its number is not reused under one of them.
    */
  final class Handle private[kqueue] (private[kqueue] val fd: Int) {
    // The sign bit: closed. The rest: how many uses of the descriptor are in progress.
    private val state = new java.util.concurrent.atomic.AtomicInteger(0)

    /** Whether it has not been closed. */
    private[kqueue] def isOpen: Boolean = state.get() >= 0

    /** Starts a use of the descriptor; false, starting none, if the handle is closed. */
    private[kqueue] def acquire(): Boolean = {
      var current = state.get()
      while current >= 0 && !state.compareAndSet(current, current + 1) do current = state.get()
      current >= 0
    }

    /** Ends a use `acquire` started. */
    private[kqueue] def release(): Unit = state.decrementAndGet(): Unit

    /** Marks it closed, so that no use starts after, and waits for the uses in progress to end: a system call or a
      * registration, short, unless a file op is reading or writing the disk. False if it was already closed.
      */
    private[kqueue] def closeUses(): Boolean = {
      var current = state.get()
      while current >= 0 && !state.compareAndSet(current, current | Int.MinValue) do current = state.get()
      if current < 0 then false
      else {
        var spins = 0
        while (state.get() & Int.MaxValue) != 0 do {
          if spins < 100 then Thread.onSpinWait() else Thread.`yield`()
          spins += 1
        }
        true
      }
    }

    override def toString: String = s"Handle($fd)"
  }

  def ops: KqueueOps.type = KqueueOps
  def pool(bufferSize: Int, buffers: Int): BufferPool = new gears.async.io.internal.NativePool(bufferSize, buffers)

  /** Opens a reactor with `KqueueReactor.open`'s defaults. */
  def open(): Reactor.Of[Op, Handle] = KqueueReactor.open()
}
