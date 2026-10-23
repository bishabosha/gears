package asyncio

import java.nio.ByteBuffer

/** Opening a backend's handles; `Ops.close` closes them. Every operation is synchronous and never blocks: a handle from
  * here is a name for an I/O object the backend has created, and whatever touches the network, the disk, or another
  * thread to make it usable is an op on it, such as `SocketOps.listening`; what no environment can create
  * synchronously, a regular file, is opened by an op alone (`FileOps.open`). Failures throw `java.io.IOException`.
  *
  * A `Handle` names an open file, socket, or other I/O object; what its number means belongs to the backend (see
  * `Handle`). Handles belong to the caller, who closes them with `Ops.close`; the backend never closes them. What every
  * backend opens is nothing: each kind of handle comes with the capability that has it (`SocketHandles`, `PipeHandles`,
  * and the rest).
  */
trait Handles

/** IP sockets, for a backend that has them (see `SocketBackend`). */
trait SocketHandles extends Handles {

  /** A stream socket to connect to `address`; perform `ops.connect` to wait until it is connected. */
  def connect(address: IpAddress): Handle

  /** A stream socket that will listen on `address` once `ops.listening` is performed on it: binding and listening
    * happen there, so an address in use fails that op, not this. With `reuseAddr`, the address can be bound while
    * connections from an earlier listener on it are still in `TIME_WAIT`, so a restarted server need not wait for them
    * to expire.
    */
  def listener(address: IpAddress, reuseAddr: Boolean = true): Handle

  /** A datagram socket, bound to `local` and connected to `remote` where given, which must be of the same family. A
    * backend whose facility binds asynchronously binds it when `ops.listening` is performed on it; one that binds here,
    * as the backends over POSIX sockets do, completes that op at once, so code that performs it is correct on both, and
    * an address in use fails whichever of the two the backend binds in.
    */
  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Handle
}

/** Pipes, for a backend that has them (see `PipeBackend`). */
trait PipeHandles extends Handles {

  /** A pipe, as its read end and its write end. */
  def pipe(): (Handle, Handle)
}

/** Using handles without waiting, for a backend whose handles are non-blocking (see `ReadinessBackend`). */
trait ReadinessHandles extends Handles {

  /** Reads what is available now into `buf`: the bytes read, 0 if nothing is, or -1 at end of stream. */
  def readNow(handle: Handle, buf: ByteBuffer): Int

  /** Writes what fits now from `buf`: the bytes written, or -1 if nothing fits. */
  def writeNow(handle: Handle, buf: ByteBuffer): Int
}

/** Unix domain stream sockets, which the ops for IP stream sockets (`accept`, `connect`, `read`, `write`) also run on.
  */
trait UnixStreamHandles extends SocketHandles {

  /** A stream socket to connect to `address`; perform `ops.connect` to wait until it is connected. */
  def connect(address: UnixAddress): Handle

  /** A stream socket that will listen on `address` once `ops.listening` is performed on it, replacing a socket file
    * already there then.
    */
  def listener(address: UnixAddress): Handle
}

/** Unix domain datagram sockets, whose packets `UnixDatagramOps` sends and receives. */
trait UnixDatagramHandles extends SocketHandles {

  /** A datagram socket, bound to `local` and connected to `remote` where given. A client needs a `local` path so that
    * replies can reach it.
    */
  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null): Handle
}

/** FIFOs, waited on as pipes are. */
trait FifoHandles extends Handles {

  /** Opens a FIFO for reading or writing without blocking. Opening one for writing fails while it has no reader. */
  def openFifo(path: String, write: Boolean): Handle
}
