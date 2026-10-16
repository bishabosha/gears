package asyncio.unsafe

import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.time
import scala.scalanative.unsafe.*

/** The system calls the loop makes where they cannot block, bound without posixlib's `@blocking`: reads, writes,
  * accepts and datagram calls on non-blocking descriptors, and `kevent` calls that only register changes or poll
  * without waiting.
  *
  * A `@blocking` call tells the GC the thread is leaving managed code, so that a collection need not wait for it, and
  * to that end captures the thread's registers. On arm64 Scala Native captures them with `setjmp`, and macOS's `setjmp`
  * saves the signal mask and alternate signal stack, each a system call of its own: posixlib's binding costs three
  * system calls where one is needed. A call that returns at once never keeps a collection waiting, so it needs none of
  * that. Calls that may wait, a `kevent` that blocks for events, keep posixlib's `@blocking` binding.
  *
  * The signatures match posixlib's declarations of the same C functions.
  */
@extern
private[asyncio] object Unblocked {
  def read(fildes: CInt, buf: CVoidPtr, nbyte: CSize): CInt = extern

  def write(fildes: CInt, buf: CVoidPtr, nbyte: CSize): CInt = extern

  def accept(fd: CInt, address: Ptr[socket.sockaddr], addressLength: Ptr[socket.socklen_t]): CInt = extern

  def recvmsg(fd: CInt, message: Ptr[socket.msghdr], flags: CInt): CSSize = extern

  def sendto(
      fd: CInt,
      buffer: CVoidPtr,
      length: CSize,
      flags: CInt,
      destination: Ptr[socket.sockaddr],
      destinationLength: socket.socklen_t
  ): CSSize = extern

  def kevent(
      kq: CInt,
      changelist: CVoidPtr,
      nchanges: CInt,
      eventlist: CVoidPtr,
      nevents: CInt,
      timeout: Ptr[time.timespec]
  ): CInt = extern
}
