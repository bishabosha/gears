package asyncio.darwin

import java.io.IOException
import java.util.concurrent.atomic.AtomicReferenceArray
import scala.collection.mutable
import scala.scalanative.libc.stdlib
import scala.scalanative.libc.string
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.FifoHandles
import asyncio.IpAddress
import asyncio.unsafe.Descriptors
import DarwinEvent.*
import DarwinHandle.{Connect, Read, Write}

/** A datagram taken in, with its sender. */
private[darwin] final class Datagram(val bytes: Array[Byte], val from: IpAddress)

/** An open I/O object of the Darwin backend and everything known about it: a Network.framework connection, listener
  * or datagram socket, or a descriptor, each with its C side (`DarwinIo`); a regular file has only its descriptor,
  * since its reads and writes never wait. The handle number is its index in `DarwinHandles`' table.
  *
  * The C side only queues what the framework delivers; `drain` takes those events and applies them here: bytes
  * received go to the stash, datagrams to `packets`, accepted connections to `accepted`, the connection's state and
  * errors to their fields, and the op waiting for any of it is told. Everything is guarded by the handle's own lock,
  * since the ops on it may be on any reactor, and the reactor that drains its events is whichever is watching it:
  * the reactor of an op waiting on it, told to the C side with `dn_handle_watch`, so that it is woken for them.
  */
private[darwin] final class DarwinHandle(
    val kind: Int,
    val ptr: DarwinIo.Handle,
    val fd: Int,
    val remote: IpAddress | Null
) {
  def isFile: Boolean = kind == DarwinHandle.File
  def isDescriptor: Boolean = kind == DarwinHandle.Descriptor

  // Guarded by `this`, from here on.
  private var closed = false

  /** The connection's or listener's state: 0 not ready yet, 1 ready, 2 failed, 3 cancelled; and its errors. */
  private var state = 0
  private var error = 0
  private var readError = 0
  private var writeError = 0

  // A stream's received bytes not yet taken, in C memory, and whether a receive is outstanding.
  private var stash: Ptr[Byte] = null
  private var stashLength = 0
  private var stashCapacity = 0
  private var receiving = false
  private var eof = false

  // Sends outstanding, in bytes, for the window (`DarwinHandle.SendWindow`).
  private var sentBytes = 0L

  private val accepted = new java.util.ArrayDeque[DarwinIo.Handle]()
  private val packets = new java.util.ArrayDeque[Datagram]()

  // A bound datagram socket's flows, by the C handle's address and by peer.
  private val flows = mutable.LongMap.empty[Flow]
  private val flowsByPeer = mutable.HashMap.empty[IpAddress, Flow]
  private final class Flow(val ptr: DarwinIo.Handle, var peer: IpAddress | Null)

  // The ops waiting here, one per kind: the reactor to tell and the token of the submission there; no reactor, none.
  private var readReactor: DarwinReactor | Null = null
  private var readToken = 0L
  private var writeReactor: DarwinReactor | Null = null
  private var writeToken = 0L
  private var connectReactor: DarwinReactor | Null = null
  private var connectToken = 0L
  private var watcher: DarwinReactor | Null = null

  private def closedFailure = new IOException("The handle was closed")
  private def failure(what: String, errno: Int) =
    new IOException(s"$what failed: ${DarwinHandles.errorText(if errno != 0 then errno else 5)}")

  /** Takes the C side's queued events and applies them, each under the lock, telling the waiters they finish. From
    * the reactor attempting an op here, or draining this handle as its watcher. Nothing is taken once closed.
    */
  def drain(event: Ptr[DarwinIo.Event]): Unit = synchronized {
    while !closed && DarwinIo.dn_handle_next(ptr, event) != 0 do
      try apply(event)
      finally DarwinIo.dn_event_done(event)
  }

  /** Applies one event, telling the waiters it finishes. Under the lock. */
  private def apply(e: Ptr[DarwinIo.Event]): Unit = {
    val flow = e.flow
    if e.kind == Flow then {
      flows.put(flow.toLong, new Flow(flow, null))
      DarwinIo.dn_receive_message(flow) // a flow keeps one receive outstanding, so that its datagrams come in
    } else if flow != null then applyToFlow(flow, e)
    else
      e.kind match {
        case Received =>
          receiving = false
          if e.len > 0 then stashAppend(e.bytes, e.len)
          if e.complete && e.isFinal then eof = true
          if e.error != 0 then readError = e.error
          take(Read)
        case Sent =>
          sentBytes -= e.len
          if e.error != 0 then writeError = e.error
          take(Write)
        case Ready =>
          state = 1
          take(Connect)
        case Failed =>
          state = 2
          error = e.error
          if readError == 0 then readError = e.error
          if writeError == 0 then writeError = e.error
          take(Connect)
          take(Read)
          take(Write)
        case Cancelled =>
          state = 3
        case Accepted =>
          accepted.add(e.accepted)
          take(Read)
        case Packet =>
          receiving = false
          if e.error != 0 then {
            readError = e.error
            take(Read)
          } else {
            val sender: IpAddress | Null = if e.fromLength.toInt > 0 then DarwinHandles.decode(e.from) else remote
            if sender != null then {
              packets.add(new Datagram(copy(e.bytes, e.len), sender))
              take(Read)
            }
          }
        case Readable => take(Read)
        case Writable => take(Write)
        case _        => ()
      }
  }

  /** Applies an event of one of a bound datagram socket's flows. Under the lock. */
  private def applyToFlow(flowPtr: DarwinIo.Handle, e: Ptr[DarwinIo.Event]): Unit = {
    val flow = flows.getOrNull(flowPtr.toLong)
    e.kind match {
      case Packet if flow != null =>
        if e.error == 0 then {
          if flow.peer == null then flow.peer = DarwinHandles.flowPeer(flowPtr)
          val peer = flow.peer
          if peer != null then {
            flowsByPeer.put(peer.nn, flow)
            packets.add(new Datagram(copy(e.bytes, e.len), peer.nn))
          }
          DarwinIo.dn_receive_message(flowPtr)
          take(Read)
        }
      case Sent =>
        sentBytes -= e.len
        if e.error != 0 then writeError = e.error
        take(Write)
      case Failed | Cancelled if flow != null =>
        flows.remove(flowPtr.toLong)
        if flow.peer != null then flowsByPeer.remove(flow.peer.nn)
        DarwinIo.dn_handle_release(flowPtr)
      case _ => ()
    }
  }

  private def copy(bytes: Ptr[Byte], len: Int): Array[Byte] = {
    val out = new Array[Byte](len)
    if len > 0 then string.memcpy(out.at(0), bytes, len.toCSize)
    out
  }

  private def stashAppend(bytes: Ptr[Byte], len: Int): Unit = {
    if stashLength + len > stashCapacity then {
      var capacity = if stashCapacity == 0 then 4096 else stashCapacity
      while capacity < stashLength + len do capacity *= 2
      stash = stdlib.realloc(stash, capacity.toCSize)
      stashCapacity = capacity
    }
    string.memcpy(stash + stashLength, bytes, len.toCSize)
    stashLength += len
  }

  /** Tells the op waiting for `kind`, if any, taking it off, and sets the watcher accordingly. Under the lock: a
    * reactor told from another thread queues the token and wakes, which takes no lock of the handle's.
    */
  private def take(kind: Int): Unit = {
    var reactor: DarwinReactor | Null = null
    var token = 0L
    kind match {
      case Read =>
        reactor = readReactor
        token = readToken
        readReactor = null
      case Write =>
        reactor = writeReactor
        token = writeToken
        writeReactor = null
      case Connect =>
        reactor = connectReactor
        token = connectToken
        connectReactor = null
    }
    updateWatcher()
    if reactor != null then reactor.nn.notified(token)
  }

  /** Registers the op of `token` on `reactor` as waiting for `kind`; refused if one already is. Under the lock. */
  private def await(kind: Int, reactor: DarwinReactor, token: Long): Unit = {
    val slotTaken = kind match {
      case Read    => readReactor != null
      case Write   => writeReactor != null
      case Connect => connectReactor != null
    }
    if slotTaken then throw new IllegalStateException(s"A ${DarwinHandle.name(kind)} is already pending on the handle")
    kind match {
      case Read =>
        readReactor = reactor
        readToken = token
      case Write =>
        writeReactor = reactor
        writeToken = token
      case Connect =>
        connectReactor = reactor
        connectToken = token
    }
    updateWatcher()
  }

  /** Withdraws the waiter for `kind` if it is still `token`'s (a cancelled op). */
  def unwait(kind: Int, token: Long): Unit = synchronized {
    kind match {
      case Read    => if readReactor != null && readToken == token then readReactor = null
      case Write   => if writeReactor != null && writeToken == token then writeReactor = null
      case Connect => if connectReactor != null && connectToken == token then connectReactor = null
    }
    updateWatcher()
  }

  private var watchMask = 0

  /** The reactor the C side wakes for events, and for which kinds: that of an op waiting here, for what it waits for,
    * so that a send's completion, say, wakes nobody while no write waits; or none. Under the lock. (Keeping the
    * reactor that last read as the watcher, so that a read registers no change, measured no gain: see the TODO.)
    */
  private def updateWatcher(): Unit = {
    val wanted: DarwinReactor | Null =
      if readReactor != null then readReactor
      else if writeReactor != null then writeReactor
      else if connectReactor != null then connectReactor
      else null
    val mask =
      (if readReactor != null then DarwinEvent.ForRead else 0) |
        (if writeReactor != null then DarwinEvent.ForWrite else 0) |
        (if connectReactor != null then DarwinEvent.ForConnect else 0)
    if !(wanted eq watcher) || mask != watchMask then {
      watcher = wanted
      watchMask = mask
      if !closed && ptr != null then DarwinIo.dn_handle_watch(ptr, if wanted == null then null else wanted.nn.ptr, mask)
    }
  }

  // ---- What the ops do, each under the lock, after `drain`: true when done, false with a waiter registered.

  private def checkOpen(): Unit = if closed then throw closedFailure

  /** A stream read: takes up to `cap` stashed bytes into `into`, returning how many; 0 at the end of the stream;
    * or false after registering the read waiter, with a receive outstanding. After taking bytes it keeps a receive
    * outstanding, so that the next read finds its bytes waiting (`DarwinHandle.Prefetch`).
    */
  def read(into: Ptr[Byte], cap: Int, reactor: DarwinReactor, token: Long): Int = synchronized {
    checkOpen()
    if stashLength > 0 then {
      val n = math.min(stashLength, cap)
      string.memcpy(into, stash, n.toCSize)
      stashLength -= n
      if stashLength > 0 then string.memmove(stash, stash + n, stashLength.toCSize)
      if DarwinHandle.Prefetch && stashLength == 0 && !eof && readError == 0 && !receiving && state == 1 then
        receive(cap)
      n
    } else if eof then 0
    else if readError != 0 then throw failure("read", readError)
    else {
      await(Read, reactor, token)
      if !receiving then receive(cap)
      -1
    }
  }

  private def receive(cap: Int): Unit = {
    receiving = true
    DarwinIo.dn_receive(ptr, cap.toUInt)
  }

  /** A write or datagram send of `len` bytes, copied and sent now: true if done at once, the sends outstanding being
    * within the window; false with the write waiter registered, told once a send has been processed.
    */
  def send(bytes: Ptr[Byte], len: Int, reactor: DarwinReactor, token: Long): Boolean = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    sendOn(ptr, bytes, len, reactor, token)
  }

  private def sendOn(on: DarwinIo.Handle, bytes: Ptr[Byte], len: Int, reactor: DarwinReactor, token: Long): Boolean = {
    val fits = sentBytes < DarwinHandle.SendWindow
    if !fits then await(Write, reactor, token)
    sentBytes += len
    DarwinIo.dn_send(on, bytes, len)
    fits
  }

  /** After the write waiter was told: true if a send went, or a failure thrown. */
  def sent(): Boolean = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    true
  }

  /** The connect op: true once ready, a failure thrown, or false with the connect waiter registered. */
  def connected(reactor: DarwinReactor, token: Long): Boolean = synchronized {
    checkOpen()
    if state == 1 then true
    else if state == 2 then throw failure("connect", error)
    else {
      await(Connect, reactor, token)
      false
    }
  }

  /** The accept op: the next accepted connection's C handle, or null with the read waiter registered. */
  def accept(reactor: DarwinReactor, token: Long): DarwinIo.Handle = synchronized {
    checkOpen()
    if !accepted.isEmpty then accepted.poll()
    else if state == 2 then throw failure("accept", error)
    else {
      await(Read, reactor, token)
      null
    }
  }

  /** The receive op: the next datagram, or null with the read waiter registered; a connected socket keeps one receive
    * outstanding.
    */
  def receivePacket(reactor: DarwinReactor, token: Long): Datagram | Null = synchronized {
    checkOpen()
    if !packets.isEmpty then {
      val packet = packets.poll()
      if kind == DarwinHandle.Datagram && !receiving && readError == 0 then receiveMessage()
      packet
    } else if readError != 0 then throw failure("receive", readError)
    else {
      await(Read, reactor, token)
      if kind == DarwinHandle.Datagram && !receiving then receiveMessage()
      null
    }
  }

  private def receiveMessage(): Unit = {
    receiving = true
    DarwinIo.dn_receive_message(ptr)
  }

  /** A datagram to `to` from a bound socket: on the flow of that peer if it has sent to us, or a flow opened to it. */
  def sendTo(to: IpAddress, bytes: Ptr[Byte], len: Int, reactor: DarwinReactor, token: Long): Boolean = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("send", writeError)
    val flow = flowsByPeer.getOrElse(
      to, {
        val (host, port) = DarwinHandles.hostAndPort(to)
        val opened = Zone.acquire { implicit z => DarwinIo.dn_flow_open(ptr, toCString(host), port) }
        val f = new Flow(opened, to)
        flows.put(opened.toLong, f)
        flowsByPeer.put(to, f)
        f
      }
    )
    sendOn(flow.ptr, bytes, len, reactor, token)
  }

  /** A descriptor's wait for readiness, `read` or write: false, with the waiter registered and the source armed. */
  def armed(read: Boolean, reactor: DarwinReactor, token: Long): Boolean = synchronized {
    checkOpen()
    await(if read then Read else Write, reactor, token)
    DarwinIo.dn_fd_arm(ptr, if read then 1 else 0)
    false
  }

  /** Closes the handle, from any thread: the ops waiting on it are told, and fail on their reactors; the connection
    * is cancelled (once its sends have gone, on the C side), a listener's at once, and the C side released. Accepted
    * connections not taken and flows go with it.
    */
  def close(): Unit =
    synchronized {
      if closed then return
      closed = true
      take(Connect)
      take(Read)
      take(Write)
      watcher = null
      if ptr != null then {
        DarwinIo.dn_handle_watch(ptr, null, 0)
        while !accepted.isEmpty do {
          val a = accepted.poll()
          DarwinIo.dn_cancel(a)
          DarwinIo.dn_handle_release(a)
        }
        for flow <- flows.values do {
          DarwinIo.dn_cancel(flow.ptr)
          DarwinIo.dn_handle_release(flow.ptr)
        }
        flows.clear()
        flowsByPeer.clear()
        DarwinIo.dn_cancel(ptr)
        DarwinIo.dn_handle_release(ptr)
      } else unistd.close(fd)
      if stash != null then {
        stdlib.free(stash)
        stash = null
      }
    }
}

private[darwin] object DarwinHandle {
  inline val Stream = 1
  inline val Listener = 2
  inline val Datagram = 3
  inline val BoundDatagram = 4
  inline val Descriptor = 5 // a pipe or FIFO, waited on through a Dispatch source
  inline val File = 6

  /** What an op waits for. */
  inline val Read = 1
  inline val Write = 2
  inline val Connect = 3

  private def name(kind: Int): String = kind match {
    case Read    => "read"
    case Write   => "write"
    case Connect => "connect"
  }

  /** How many bytes of sends a handle may have outstanding before a write waits for a completion: the pacing
    * Network.framework asks for, as a kernel socket's send buffer paces writes. `ASYNCIO_DARWIN_SEND_WINDOW` in the
    * environment overrides it, in bytes; 0 makes every write wait for its send, for measuring.
    */
  val SendWindow: Long = sys.env.get("ASYNCIO_DARWIN_SEND_WINDOW").map(_.toLong).getOrElse(256L * 1024)

  /** Whether a read that took bytes issues the next receive at once, so that the next read finds its bytes waiting:
    * `ASYNCIO_DARWIN_PREFETCH=0` turns it off, for measuring.
    */
  val Prefetch: Boolean = !sys.env.get("ASYNCIO_DARWIN_PREFETCH").contains("0")
}

/** The Darwin backend's handles: indexes into a table of its I/O objects, since Network.framework's are not
  * descriptors. IP sockets are Network.framework's; pipes, FIFOs and regular files are descriptors, which
  * Network.framework has nothing for. Unix domain sockets it has no public endpoint for, so there are none.
  */
object DarwinHandles extends FifoHandles {

  // The table: a handle number is an index, reused once closed. Grown under the lock, and published through the
  // volatile field so that any thread reads the current table.
  @volatile private var table = new AtomicReferenceArray[DarwinHandle | Null](256)
  private val free = new java.util.ArrayDeque[Integer]()
  private var next = 0

  private def add(handle: DarwinHandle): Int = synchronized {
    val index =
      if !free.isEmpty then free.poll().intValue
      else {
        if next == table.length then {
          val grown = new AtomicReferenceArray[DarwinHandle | Null](table.length * 2)
          for i <- 0 until table.length do grown.set(i, table.get(i))
          table = grown
        }
        next += 1
        next - 1
      }
    table.set(index, handle)
    // The C side's first field is the tag: the number, so that a reactor draining the handle finds it without a
    // lookup or a lock (`at`).
    if handle.ptr != null then !handle.ptr.asInstanceOf[Ptr[Long]] = index.toLong
    index
  }

  /** The handle's object, or an `IOException` if it is not open. */
  private[darwin] def get(handle: Int): DarwinHandle = {
    val current = table
    val found = if handle >= 0 && handle < current.length then current.get(handle) else null
    if found == null then throw new IOException(s"Handle $handle is not open")
    found
  }

  /** The handle whose C side is `ptr`, if still open: found by the number the C side carries, and checked against
    * it, since the number may have been reused since the handle closed.
    */
  private[darwin] def at(ptr: DarwinIo.Handle): DarwinHandle | Null = {
    val index = (!ptr.asInstanceOf[Ptr[Long]]).toInt
    val current = table
    val found = if index >= 0 && index < current.length then current.get(index) else null
    if found != null && found.ptr == ptr then found else null
  }

  /** Registers an accepted connection, as its own handle. */
  private[darwin] def accepted(ptr: DarwinIo.Handle): Int = add(new DarwinHandle(DarwinHandle.Stream, ptr, -1, null))

  def openFile(path: String, write: Boolean): Int =
    add(new DarwinHandle(DarwinHandle.File, null, Descriptors.openFile(path, write), null))

  def openFifo(path: String, write: Boolean): Int = descriptor(Descriptors.openFifo(path, write))

  def pipe(): (Int, Int) = {
    val (in, out) = Descriptors.pipe()
    (descriptor(in), descriptor(out))
  }

  private def descriptor(fd: Int): Int =
    add(new DarwinHandle(DarwinHandle.Descriptor, DarwinIo.dn_fd_open(fd), fd, null))

  private[darwin] def hostAndPort(address: IpAddress): (String, Int) = address match {
    case IpAddress.IPv4(host, port) => (host, port)
    case IpAddress.IPv6(host, port) => (host, port)
  }

  def connect(address: IpAddress): Int = {
    val (host, port) = hostAndPort(address)
    val ptr = Zone.acquire { implicit z => DarwinIo.dn_connect(toCString(host), port) }
    add(new DarwinHandle(DarwinHandle.Stream, ptr, -1, address))
  }

  def listen(address: IpAddress, reuseAddr: Boolean = true): Int = {
    val (host, port) = hostAndPort(address)
    val error = stackalloc[CInt]()
    val ptr = Zone.acquire { implicit z => DarwinIo.dn_listen(toCString(host), port, if reuseAddr then 1 else 0, error) }
    if ptr == null then throw new IOException(s"Failed to listen on $host:$port: ${errorText(!error)}")
    add(new DarwinHandle(DarwinHandle.Listener, ptr, -1, null))
  }

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Int = {
    require(
      local == null || remote == null || local.getClass == remote.getClass,
      "Local and remote addresses must be of the same family"
    )
    require(local != null || remote != null, "A datagram socket needs a local or a remote address")
    if remote != null then {
      val (host, port) = hostAndPort(remote)
      val ptr = Zone.acquire { implicit z =>
        if local != null then {
          val (localHost, localPort) = hostAndPort(local)
          DarwinIo.dn_datagram_connect(toCString(host), port, toCString(localHost), localPort)
        } else DarwinIo.dn_datagram_connect(toCString(host), port, null, 0)
      }
      add(new DarwinHandle(DarwinHandle.Datagram, ptr, -1, remote))
    } else {
      val (host, port) = hostAndPort(local.nn)
      val error = stackalloc[CInt]()
      val ptr = Zone.acquire { implicit z => DarwinIo.dn_datagram_bind(toCString(host), port, error) }
      if ptr == null then throw new IOException(s"Failed to bind $host:$port: ${errorText(!error)}")
      add(new DarwinHandle(DarwinHandle.BoundDatagram, ptr, -1, null))
    }
  }

  /** Closes a handle, from any thread: the ops pending on it, on whichever reactor, fail there (see `Handles.close`
    * and `DarwinHandle.close`). The number is free for reuse at once.
    */
  def close(handle: Int): Unit = {
    val found = synchronized {
      val current = table
      val found = if handle >= 0 && handle < current.length then current.get(handle) else null
      if found != null then {
        current.set(handle, null)
        free.add(handle)
      }
      found
    }
    if found == null then throw new IOException(s"Handle $handle is not open")
    found.close()
  }

  private[darwin] def errorText(errno: Int): String = fromCString(scala.scalanative.posix.string.strerror(errno))

  /** A flow's peer, asked of the C side, or null if not known yet. */
  private[darwin] def flowPeer(flow: DarwinIo.Handle): IpAddress | Null = {
    val storage = stackalloc[socket.sockaddr_storage]()
    if DarwinIo.dn_flow_remote(flow, storage) > 0 then decode(storage) else null
  }

  /** Decodes an IP socket address filled in by the C side. */
  private[darwin] def decode(addr: Ptr[socket.sockaddr_storage]): IpAddress = {
    val family = addr.asInstanceOf[Ptr[socket.sockaddr]].sa_family.toInt
    val text = stackalloc[Byte](64)
    if family == socket.AF_INET then {
      val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
      inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
      IpAddress.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
    } else if family == socket.AF_INET6 then {
      val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
      inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
      IpAddress.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt)
    } else throw new IOException(s"Unsupported address family $family")
  }
}
