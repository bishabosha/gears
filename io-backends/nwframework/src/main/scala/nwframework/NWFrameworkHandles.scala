package gears.async.io
package nwframework

import gears.async.io.IpAddress
import gears.async.io.Received
import gears.async.io.internal.NativePool
import gears.async.io.kqueue.KqueueBackend.Handle
import gears.async.io.kqueue.KqueueHandles
import gears.async.io.kqueue.KqueueReactor

import java.io.IOException
import scala.scalanative.libc.stdlib
import scala.scalanative.libc.string
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import NWFrameworkEvent.*
import NWFrameworkHandle.{Close, Connect, Read, Write}

/** An open Network.framework connection, listener or connected datagram socket, and everything known about it, with its
  * C side (`NWFrameworkIo`). Its number is its index in `NWFrameworkHandles`' table, which the C side carries, and the
  * program's handle, a kqueue handle with this as its foreign object (`KqueueBackend.Handle.foreign`), holds it.
  *
  * The C side only queues what the framework delivers; `drain` takes those events and applies them here: bytes received
  * go to the stash, datagrams to `packets`, accepted connections to `accepted`, the connection's state and errors to
  * their fields, and the op waiting for any of it is told. Everything is guarded by the handle's own lock, since the
  * ops on it may be on any reactor, and the reactor that drains its events is whichever is watching it: the reactor of
  * an op waiting on it, told to the C side with `nwf_handle_watch`, so that it is woken for them.
  */
private[nwframework] final class NWFrameworkHandle(
    val kind: Int,
    val ptr: NWFrameworkIo.Handle,
    val remote: IpAddress | Null
) {

  /** Its index in `NWFrameworkHandles`' table, set as it is added. */
  private[nwframework] var number = -1

  /** The options it opened with, or for an accepted connection its listener's, which `option` reports, since
    * Network.framework neither reports nor changes them after.
    */
  private[nwframework] var opened: SocketOptions = SocketOptions.default

  /** A listener's address as it was asked for, whose port, if 0, `address` replaces with the one it got. */
  private[nwframework] var listenedOn: IpAddress | Null = null

  /** The local address, or with `peer` the peer's (see `StreamSocketOps.localAddress`). */
  def address(peer: Boolean): IpAddress = synchronized {
    checkOpen()
    if kind == NWFrameworkHandle.Listener then {
      if peer then throw new IOException(s"A listener has no peer: $this")
      val port = NWFrameworkIo.nwf_listener_port(ptr)
      listenedOn.nn match {
        case IpAddress.IPv4(host, _) => IpAddress.IPv4(host, port)
        case IpAddress.IPv6(host, _) => IpAddress.IPv6(host, port)
      }
    } else {
      val storage = stackalloc[socket.sockaddr_storage]()
      if NWFrameworkIo.nwf_connection_address(ptr, if peer then 1 else 0, storage) > 0 then
        NWFrameworkHandles.decode(storage)
      else if peer && remote != null then remote.nn
      else throw new IOException(s"The ${if peer then "peer" else "address"} of $this is not known")
    }
  }

  /** What the handle shows, `Handle(nw connection 3)`: its kind and its number in the table. */
  override def toString: String = {
    val what = kind match {
      case NWFrameworkHandle.Stream   => "connection"
      case NWFrameworkHandle.Listener => "listener"
      case _                          => "datagram"
    }
    s"nw $what $number"
  }

  // Guarded by `this`, from here on.
  private var closed = false // no more ops: closing, or closed
  private var released = false // the C side is let go: no more calls to it

  /** The connection's or listener's state: 0 not ready yet, 1 ready, 2 failed, 3 cancelled; and its errors. */
  private var state = 0
  private var error = 0
  private var readError = 0
  private var writeError = 0
  private var outputShut = false // the sending side was ended: writes fail

  // A stream's received bytes not yet taken, in C memory, and whether a receive is outstanding.
  private var stash: Ptr[Byte] = null
  private var stashLength = 0
  private var stashCapacity = 0
  private var receiving = false
  private var eof = false

  // The pool of the pooled read waiting, whose receive's content (`nwf_receive_held`) is copied into a buffer taken
  // from it as the content comes, if that read still waits: a read cancelled meanwhile holds no buffer, the content
  // going to the stash. And a buffer so filled, waiting to be taken by the read, or copied by a plain one (its
  // position is what a plain read took).
  private var pooledFor: NativePool | Null = null
  private var pooledReady: NativePool#NativeSlot | Null = null

  // The close op waiting for the listener's cancelled state, if any.
  private var closeReactor: KqueueReactor | Null = null
  private var closeToken = 0L

  // Sends outstanding, in bytes, for the window (`NWFrameworkHandle.SendWindow`).
  private var sentBytes = 0L

  private val accepted = new java.util.ArrayDeque[NWFrameworkIo.Handle]()
  private val packets = new java.util.ArrayDeque[Array[Byte]]() // from the peer, a connected socket's only sender
  private var packetBytes = 0L // in `packets`, at most `NWFrameworkHandle.ReceiveBuffer`, as a kernel socket's buffer

  // The ops waiting here, one per kind: the reactor to tell and the token of the submission there; no reactor, none.
  private var readReactor: KqueueReactor | Null = null
  private var readToken = 0L
  private var writeReactor: KqueueReactor | Null = null
  private var writeToken = 0L
  private var connectReactor: KqueueReactor | Null = null
  private var connectToken = 0L
  private var watcher: KqueueReactor | Null = null

  private def closedFailure = new IOException("The handle was closed")
  private def failure(what: String, errno: Int) =
    new IOException(s"$what failed: ${NWFrameworkHandles.errorText(if errno != 0 then errno else 5)}")

  /** Takes the C side's queued events and applies them, each under the lock, telling the waiters they finish. From the
    * reactor attempting an op here, or draining this handle as its watcher. Nothing is taken once the C side is
    * released.
    */
  def drain(event: Ptr[NWFrameworkIo.Event]): Unit = synchronized {
    while !released && NWFrameworkIo.nwf_handle_next(ptr, event) != 0 do
      try apply(event)
      finally NWFrameworkIo.nwf_event_done(event)
  }

  /** Applies one event, telling the waiters it finishes. Under the lock. */
  private def apply(e: Ptr[NWFrameworkIo.Event]): Unit =
    e.kind match {
      case Received =>
        receiving = false
        if e.held != null then { // a pooled read's: into a pool buffer if that read still waits, else the stash
          val pool = pooledFor
          pooledFor = null
          val slot = if pool != null && readReactor != null && !closed then pool.take() else null
          if slot != null then {
            NWFrameworkIo.nwf_data_copy(e.held, slot.address)
            slot.buffer.limit(e.len)
            pooledReady = slot
          } else if !closed then { // the pool empty: the read, tried again, finds the bytes and fails as kqueue's
            stashReserve(e.len)
            NWFrameworkIo.nwf_data_copy(e.held, stash + stashLength)
            stashLength += e.len
          }
        } else if e.len > 0 then stashAppend(e.bytes, e.len)
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
      case Failed if state == 1 && kind == NWFrameworkHandle.Stream =>
        // A connection fails once both sides have ended their streams and the peer closed (ENETDOWN, seen after a
        // shutdownOutput), while the framework still holds what the peer sent and its end, for receives to come:
        // writes fail from here, and reads go on until a receive brings the end or fails itself.
        error = e.error
        if writeError == 0 then writeError = e.error
        take(Write)
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
        if closeReactor != null then finishClose() // the close op waiting for it
      case Accepted =>
        accepted.add(e.accepted)
        take(Read)
      case Packet =>
        receiving = false
        if e.error != 0 then {
          readError = e.error
          take(Read)
        } else if queued(e) then {
          packets.add(copy(e.bytes, e.len))
          take(Read)
        }
      case _ => ()
    }

  /** Whether a datagram of `e.len` bytes has room among those queued, as a kernel socket's buffer would; if so, counted
    * in. One that has none is dropped, as a kernel socket drops what its buffer cannot hold.
    */
  private def queued(e: Ptr[NWFrameworkIo.Event]): Boolean =
    if packetBytes + e.len > NWFrameworkHandle.ReceiveBuffer then false
    else {
      packetBytes += e.len
      true
    }

  private def copy(bytes: Ptr[Byte], len: Int): Array[Byte] = {
    val out = new Array[Byte](len)
    if len > 0 then string.memcpy(out.at(0), bytes, len.toCSize)
    out
  }

  private def stashAppend(bytes: Ptr[Byte], len: Int): Unit = {
    stashReserve(len)
    string.memcpy(stash + stashLength, bytes, len.toCSize)
    stashLength += len
  }

  /** Room for `len` more bytes in the stash: grown by `realloc`, which a zone cannot do, the stash outliving any one op
    * and growing as bytes arrive.
    */
  private def stashReserve(len: Int): Unit =
    if stashLength + len > stashCapacity then {
      var capacity = if stashCapacity == 0 then 4096 else stashCapacity
      while capacity < stashLength + len do capacity *= 2
      stash = stdlib.realloc(stash, capacity.toCSize)
      stashCapacity = capacity
    }

  /** Tells the op waiting for `kind`, if any, taking it off, and sets the watcher accordingly. Under the lock: a
    * reactor told from another thread queues the token and wakes, which takes no lock of the handle's.
    */
  private def take(kind: Int): Unit = {
    var reactor: KqueueReactor | Null = null
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
  private def registerWaiting(kind: Int, reactor: KqueueReactor, token: Long): Unit = {
    val slotTaken = kind match {
      case Read    => readReactor != null
      case Write   => writeReactor != null
      case Connect => connectReactor != null
    }
    if slotTaken then
      throw new IllegalStateException(s"A ${NWFrameworkHandle.name(kind)} is already pending on the handle")
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
      case Read =>
        if readReactor != null && readToken == token then {
          readReactor = null
          pooledFor = null // a pooled read's content to come goes to the stash, holding no buffer
        }
      case Write   => if writeReactor != null && writeToken == token then writeReactor = null
      case Connect => if connectReactor != null && connectToken == token then connectReactor = null
      case Close   => if closeReactor != null && closeToken == token then closeReactor = null
    }
    updateWatcher()
  }

  private var watchMask = 0

  /** The reactor the C side wakes for events, and for which kinds: that of an op waiting here, for what it waits for,
    * so that a send's completion, say, wakes nobody while no write waits; or none. Under the lock. (Keeping the reactor
    * that last read as the watcher, so that a read registers no change, measured no gain: see docs/io/nwframework.md.)
    */
  private def updateWatcher(): Unit = {
    val wanted: KqueueReactor | Null =
      if readReactor != null then readReactor
      else if writeReactor != null then writeReactor
      else if connectReactor != null then connectReactor
      else if closeReactor != null then closeReactor
      else null
    val mask =
      (if readReactor != null then NWFrameworkEvent.ForRead else 0) |
        (if writeReactor != null then NWFrameworkEvent.ForWrite else 0) |
        (if connectReactor != null then NWFrameworkEvent.ForConnect else 0) |
        (if closeReactor != null then NWFrameworkEvent.Ending | NWFrameworkEvent.ForRead else 0) // a late receive too
    if !(wanted eq watcher) || mask != watchMask then {
      watcher = wanted
      watchMask = mask
      if !released && ptr != null then
        val on = if wanted == null then null else NWFrameworkExtension.existing(wanted.nn).ptr
        NWFrameworkIo.nwf_handle_watch(ptr, on, mask)
    }
  }

  // ---- What the ops do, each under the lock, after `drain`: true when done, false with a waiter registered.

  private def checkOpen(): Unit = if closed then throw closedFailure

  /** A stream read: takes up to `cap` stashed bytes into `into`, returning how many; 0 at the end of the stream; or -1
    * after registering the read waiter, with a receive outstanding. After taking bytes it keeps a receive outstanding,
    * so that the next read finds its bytes waiting (`NWFrameworkHandle.Prefetch`).
    */
  def read(into: Ptr[Byte], cap: Int, reactor: KqueueReactor, token: Long): Int = synchronized {
    checkOpen()
    if pooledReady != null then {
      // What a pooled read's receive brought, taken by a plain read: copied, and the buffer released once empty.
      val slot = pooledReady.nn
      val buffer = slot.buffer
      val n = math.min(buffer.remaining(), cap)
      string.memcpy(into, slot.address + buffer.position(), n.toCSize)
      buffer.position(buffer.position() + n)
      if !buffer.hasRemaining() then {
        pooledReady = null
        slot.release()
        if NWFrameworkHandle.Prefetch && !eof && readError == 0 && !receiving && state == 1 then receive(cap)
      }
      n
    } else if stashLength > 0 then {
      val n = math.min(stashLength, cap)
      string.memcpy(into, stash, n.toCSize)
      stashLength -= n
      if stashLength > 0 then string.memmove(stash, stash + n, stashLength.toCSize)
      if NWFrameworkHandle.Prefetch && stashLength == 0 && !eof && readError == 0 && !receiving && state == 1 then
        receive(cap)
      n
    } else if eof then 0
    else if readError != 0 then throw failure("read", readError)
    else {
      registerWaiting(Read, reactor, token)
      pooledFor = null // a plain read takes what comes from the stash
      if !receiving then receive(cap)
      -1
    }
  }

  /** A pooled read: a buffer of `pool`'s with what was received, its limit at the bytes, or null with the read waiter
    * registered and a receive outstanding whose content is copied into a buffer of the pool's as it comes, taken then.
    * Bytes already received, into the stash or a pool buffer, are handed over first. Fails if the pool has no buffer
    * free when one is needed.
    */
  def readPooled(pool: NativePool, reactor: KqueueReactor, token: Long): Received | Null = synchronized {
    checkOpen()
    if pooledReady != null then {
      val slot = pooledReady.nn
      pooledReady = null
      val buffer = slot.buffer
      if buffer.position() > 0 then { // partly taken by a plain read: what is left, from the start
        val left = buffer.remaining()
        string.memmove(slot.address, slot.address + buffer.position(), left.toCSize)
        buffer.position(0).limit(left)
      }
      prefetchPooled(pool)
      slot
    } else if stashLength > 0 then {
      val slot = takeFrom(pool)
      val n = math.min(stashLength, pool.bufferSize)
      string.memcpy(slot.address, stash, n.toCSize)
      stashLength -= n
      if stashLength > 0 then string.memmove(stash, stash + n, stashLength.toCSize)
      slot.buffer.limit(n)
      if stashLength == 0 then prefetchPooled(pool)
      slot
    } else if eof then {
      val slot = takeFrom(pool)
      slot.buffer.limit(0)
      slot
    } else if readError != 0 then throw failure("read", readError)
    else {
      registerWaiting(Read, reactor, token)
      if !receiving then {
        pooledFor = pool
        receiving = true
        NWFrameworkIo.nwf_receive_held(ptr, pool.bufferSize.toUInt)
      }
      null
    }
  }

  private def takeFrom(pool: NativePool): NativePool#NativeSlot = {
    val slot = pool.take()
    if slot == null then throw new IOException("The pool has no buffer free")
    slot.nn
  }

  /** Keeps a receive outstanding into the stash, so that the next read finds its bytes waiting: not into a buffer of
    * `pool`'s, which would hold one for a read not yet made, leaving a pool shared by many connections with none for
    * the reads that are; the next pooled read copies them.
    */
  private def prefetchPooled(pool: NativePool): Unit =
    if NWFrameworkHandle.Prefetch && !eof && readError == 0 && !receiving && state == 1 then receive(pool.bufferSize)

  private def receive(cap: Int): Unit = {
    receiving = true
    NWFrameworkIo.nwf_receive(ptr, cap.toUInt)
  }

  /** A write or datagram send of `len` bytes, copied and sent now: true if done at once, the sends outstanding being
    * within the window; false with the write waiter registered, told once a send has been processed.
    */
  def send(bytes: Ptr[Byte], len: Int, reactor: KqueueReactor, token: Long): Boolean = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    if outputShut then throw new IOException("The connection was shut down for writing")
    sendOn(bytes, len, reactor, token)
  }

  /** A stream write of up to `len` bytes: as many as the window has room for, copied and sent now, returned, so that a
    * write hands over no more than a kernel socket's send buffer would take; 0, sending nothing, with the write waiter
    * registered, told once a send has been processed, if it has none. A write that waited has sent nothing, so its
    * cancel leaves the buffer as it was, as on a readiness backend. With no window (`SendWindow` 0, for measuring),
    * everything is sent and the waiter registered, returned negated.
    */
  def write(bytes: Ptr[Byte], len: Int, reactor: KqueueReactor, token: Long): Int = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    if outputShut then throw new IOException("The connection was shut down for writing")
    if NWFrameworkHandle.SendWindow == 0 then {
      sendOn(bytes, len, reactor, token): Unit
      -len
    } else {
      val room = NWFrameworkHandle.SendWindow - sentBytes
      if room <= 0 then {
        registerWaiting(Write, reactor, token)
        0
      } else {
        val n = math.min(len.toLong, room).toInt
        sentBytes += n
        NWFrameworkIo.nwf_send(ptr, bytes, n)
        n
      }
    }
  }

  /** A scattering read (see `Ops.read`): waits as `read` does for the first buffer with room, then fills those after it
    * from what has come already, stopping at the first left short: the bytes taken, 0 at the end of the stream, or -1
    * waiting.
    */
  def readMany(bufs: Array[java.nio.ByteBuffer], reactor: KqueueReactor, token: Long): Int = synchronized {
    var total = 0
    var result = 0
    var i = 0
    var more = true
    while more && i < bufs.length do {
      val buf = bufs(i)
      if buf.hasRemaining() then {
        val n =
          if total == 0 then read(gears.async.io.internal.NativeBuffer.atPosition(buf), buf.remaining(), reactor, token)
          else if pooledReady != null || stashLength > 0 then
            read(gears.async.io.internal.NativeBuffer.atPosition(buf), buf.remaining(), reactor, token)
          else 0
        if n > 0 then {
          gears.async.io.internal.NativeBuffer.advance(buf, n)
          total += n
        } else if total == 0 then result = n // the end, or waiting
        more = n > 0 && !buf.hasRemaining()
      }
      i += 1
    }
    if total > 0 then total else result
  }

  /** A gathering write (see `Ops.write`), as `write` writes one buffer: the bytes of `bufs`, in order, as many as the
    * send window has room for, copied once into one buffer the send frees, and returned; 0, sending nothing, with the
    * window full; negated with no window.
    */
  def writeMany(bufs: Array[java.nio.ByteBuffer], reactor: KqueueReactor, token: Long): Int = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    if outputShut then throw new IOException("The connection was shut down for writing")
    val total = gears.async.io.internal.Checks.remaining(bufs)
    val room = if NWFrameworkHandle.SendWindow == 0 then total else NWFrameworkHandle.SendWindow - sentBytes
    if room <= 0 then {
      registerWaiting(Write, reactor, token)
      0
    } else {
      val n = math.min(total, room).toInt
      val region = gathered(bufs, n)
      if NWFrameworkHandle.SendWindow == 0 then {
        registerWaiting(Write, reactor, token)
        sentBytes += n
        NWFrameworkIo.nwf_send_owned(ptr, region, n)
        -n
      } else {
        sentBytes += n
        NWFrameworkIo.nwf_send_owned(ptr, region, n)
        n
      }
    }
  }

  /** A connected datagram socket's send of `bufs`, in order, as one datagram, as `send` sends one buffer. */
  def sendMany(bufs: Array[java.nio.ByteBuffer], reactor: KqueueReactor, token: Long): Boolean = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    val n = gears.async.io.internal.Checks.remaining(bufs).toInt
    val fits = sentBytes < NWFrameworkHandle.SendWindow
    if !fits then registerWaiting(Write, reactor, token)
    sentBytes += n
    NWFrameworkIo.nwf_send_owned(ptr, gathered(bufs, n), n)
    fits
  }

  /** The first `n` bytes remaining in `bufs`, in order, copied into a buffer from `malloc`, the buffers advanced past
    * them. `malloc`, not a zone: the C side hands it to `dispatch_data_create` with `DISPATCH_DATA_DESTRUCTOR_FREE`,
    * which owns it from then on and frees it with `free` once Network.framework has sent it.
    */
  private def gathered(bufs: Array[java.nio.ByteBuffer], n: Int): Ptr[Byte] = {
    val region = stdlib.malloc(math.max(n, 1).toCSize)
    var at = 0
    var i = 0
    while at < n && i < bufs.length do {
      val buf = bufs(i)
      val take = math.min(buf.remaining(), n - at)
      if take > 0 then {
        // A bulk copy, so that a buffer with no native memory behind it is taken too.
        val part = buf.duplicate()
        part.limit(part.position() + take)
        scala.scalanative.memory.PointerBuffer.wrap(region + at, take).put(part)
        gears.async.io.internal.NativeBuffer.advance(buf, take)
        at += take
      }
      i += 1
    }
    region
  }

  /** Ends a connected stream's sending side (see `StreamSocketOps.shutdownOutput`), after the sends before it. */
  def shutdownOutput(): Unit = synchronized {
    checkOpen()
    if kind != NWFrameworkHandle.Stream || state != 1 then throw new IOException("Not a connected stream socket")
    if writeError != 0 then throw failure("write", writeError)
    if !outputShut then {
      outputShut = true
      NWFrameworkIo.nwf_send_final(ptr)
    }
  }

  private def sendOn(bytes: Ptr[Byte], len: Int, reactor: KqueueReactor, token: Long): Boolean = {
    val fits = sentBytes < NWFrameworkHandle.SendWindow
    if !fits then registerWaiting(Write, reactor, token)
    sentBytes += len
    NWFrameworkIo.nwf_send(ptr, bytes, len)
    fits
  }

  /** After the write waiter was told: true if a send went, or a failure thrown. */
  def sent(): Boolean = synchronized {
    checkOpen()
    if writeError != 0 then throw failure("write", writeError)
    true
  }

  /** The connect op: true once ready, a failure thrown, or false with the connect waiter registered. */
  def connected(reactor: KqueueReactor, token: Long): Boolean = synchronized {
    checkOpen()
    if state == 1 then true
    else if state == 2 then throw failure("connect", error)
    else {
      registerWaiting(Connect, reactor, token)
      false
    }
  }

  /** The accept op: the next accepted connection's C handle, or null with the read waiter registered. */
  def accept(reactor: KqueueReactor, token: Long): NWFrameworkIo.Handle = synchronized {
    checkOpen()
    if !accepted.isEmpty then accepted.poll()
    else if state == 2 then throw failure("accept", error)
    else {
      registerWaiting(Read, reactor, token)
      null
    }
  }

  /** The accept-all op: the accepted connections waiting, at most `max`, or null with the read waiter registered. */
  def acceptAll(max: Int, reactor: KqueueReactor, token: Long): Array[NWFrameworkIo.Handle] | Null = synchronized {
    checkOpen()
    if !accepted.isEmpty then {
      val taken = new Array[NWFrameworkIo.Handle](math.min(max, accepted.size))
      var i = 0
      while i < taken.length do {
        taken(i) = accepted.poll()
        i += 1
      }
      taken
    } else if state == 2 then throw failure("accept", error)
    else {
      registerWaiting(Read, reactor, token)
      null
    }
  }

  /** The receive op: the next datagram, or null with the read waiter registered; a connected socket keeps one receive
    * outstanding.
    */
  def receivePacket(reactor: KqueueReactor, token: Long): Array[Byte] | Null = synchronized {
    checkOpen()
    if !packets.isEmpty then {
      val packet = packets.poll()
      packetBytes -= packet.length
      if kind == NWFrameworkHandle.Datagram && !receiving && readError == 0 then receiveMessage()
      packet
    } else if readError != 0 then throw failure("receive", readError)
    else {
      registerWaiting(Read, reactor, token)
      if kind == NWFrameworkHandle.Datagram && !receiving then receiveMessage()
      null
    }
  }

  private def receiveMessage(): Unit = {
    receiving = true
    NWFrameworkIo.nwf_receive_message(ptr)
  }

  /** The close op: begins closing, as `close` does. A connection is closed at once, true: the C side, which holds its
    * own reference until the cancelled state, sends what was written and then cancels it, as a kernel socket's close
    * leaves unsent bytes to the kernel, so a close never waits on a peer that stopped reading. A listener is false,
    * with the close waiter registered, told once the cancelled state has come, when `closeDone` is true: once the
    * framework has let its port go, so that a port whose listener's close op has completed can be listened on again at
    * once, which completing the op before the cancelled state did not guarantee.
    */
  def closeAsync(reactor: KqueueReactor, token: Long): Boolean = synchronized {
    if closed then throw closedFailure
    closed = true
    take(Connect)
    take(Read)
    take(Write)
    if kind == NWFrameworkHandle.Stream || kind == NWFrameworkHandle.Datagram then {
      NWFrameworkIo.nwf_cancel_async(ptr)
      finishClose() // its events to come are freed with the C side, unread
      true
    } else {
      cancelChildren()
      closeReactor = reactor
      closeToken = token
      updateWatcher()
      NWFrameworkIo.nwf_cancel_async(ptr)
      false
    }
  }

  /** Whether the close op is done: the connection's cancelled state came (see `closeAsync`). */
  def closeDone: Boolean = synchronized(released)

  /** Lets the C side go, once nothing more is to come from it, telling the close op waiting for that. Under the lock.
    */
  private def finishClose(): Unit = {
    if !released then {
      released = true
      if stash != null then {
        stdlib.free(stash)
        stash = null
      }
      if pooledReady != null then {
        pooledReady.nn.release()
        pooledReady = null
      }
      NWFrameworkIo.nwf_handle_release(ptr)
    }
    val reactor = closeReactor
    closeReactor = null
    if reactor != null then reactor.nn.notified(closeToken)
  }

  /** Cancels and releases a listener's accepted connections not taken. Under the lock. */
  private def cancelChildren(): Unit =
    while !accepted.isEmpty do {
      val a = accepted.poll()
      NWFrameworkIo.nwf_cancel(a)
      NWFrameworkIo.nwf_handle_release(a)
    }

  /** Closes the handle, from any thread: the ops waiting on it are told, and fail on their reactors; the connection is
    * cancelled (once its sends have gone, on the C side), a listener's at once, and the C side released. Accepted
    * connections not taken go with it.
    */
  def close(): Unit =
    synchronized {
      if closed then return
      closed = true
      take(Connect)
      take(Read)
      take(Write)
      watcher = null
      NWFrameworkIo.nwf_handle_watch(ptr, null, 0)
      cancelChildren()
      NWFrameworkIo.nwf_cancel(ptr)
      finishClose()
    }
}

private[nwframework] object NWFrameworkHandle {
  inline val Stream = 1
  inline val Listener = 2
  inline val Datagram = 3 // connected

  /** What an op waits for. */
  inline val Read = 1
  inline val Write = 2
  inline val Connect = 3
  inline val Close = 4

  private def name(kind: Int): String = kind match {
    case Read    => "read"
    case Write   => "write"
    case Connect => "connect"
    case Close   => "close"
  }

  /** The most bytes of datagrams a socket queues before it drops what comes: a kernel UDP socket's receive buffer. */
  lazy val ReceiveBuffer: Long = NWFrameworkIo.nwf_udp_receive_buffer().toLong

  /** How many bytes of sends a handle may have outstanding before a write waits for a completion, and the most one
    * write hands over: the pacing Network.framework asks for, as a kernel socket's send buffer paces writes.
    * `GEARS_IO_NWFRAMEWORK_SEND_WINDOW` in the environment overrides it, in bytes; 0 makes every write wait for its
    * send, for measuring.
    */
  val SendWindow: Long = sys.env.get("GEARS_IO_NWFRAMEWORK_SEND_WINDOW").map(_.toLong).getOrElse(256L * 1024)

  /** Whether a read that took bytes issues the next receive at once, so that the next read finds its bytes waiting:
    * `GEARS_IO_NWFRAMEWORK_PREFETCH=0` turns it off, for measuring.
    */
  val Prefetch: Boolean = !sys.env.get("GEARS_IO_NWFRAMEWORK_PREFETCH").contains("0")
}

/** The backend's Network.framework objects, each numbered in a table whose number its C side carries, so that a reactor
  * draining one finds it without a lookup by pointer. A program's handle of one is a kqueue handle whose foreign object
  * it is (see `KqueueBackend.Handle.foreign`).
  */
private[nwframework] object NWFrameworkHandles {

  // The table: a handle number is an index, reused once closed.
  private val table = new gears.async.io.internal.HandleTable[NWFrameworkHandle](256)

  private def add(handle: NWFrameworkHandle): Handle = {
    val number = table.add(handle)
    handle.number = number
    // The C side's first field is the tag: the number, so that a reactor draining the handle finds it without a
    // lookup or a lock (`at`).
    !handle.ptr.asInstanceOf[Ptr[Long]] = number.toLong
    KqueueHandles.foreign(handle)
  }

  /** Whether `handle` is Network.framework's. */
  def owns(handle: Handle): Boolean = handle.foreign.isInstanceOf[NWFrameworkHandle]

  /** The handle's object, or an `IOException` if it is not open. */
  def get(handle: Handle): NWFrameworkHandle = {
    if !handle.isOpen then throw new IOException(s"Not an open handle: $handle")
    handle.foreign.asInstanceOf[NWFrameworkHandle]
  }

  /** The handle whose C side is `ptr`, if still open: found by the number the C side carries, and checked against it,
    * since the number may have been reused since the handle closed.
    */
  def at(ptr: NWFrameworkIo.Handle): NWFrameworkHandle | Null = {
    val found = table.find((!ptr.asInstanceOf[Ptr[Long]]).toInt)
    if found != null && found.ptr == ptr then found else null
  }

  /** Registers a connection `listener` accepted, as its own handle, with the listener's options. */
  def accepted(listener: NWFrameworkHandle, ptr: NWFrameworkIo.Handle): Handle = {
    val connection = new NWFrameworkHandle(NWFrameworkHandle.Stream, ptr, null)
    connection.opened = listener.opened
    add(connection)
  }

  /** `options` as the C side takes them (see `NWFrameworkIo.TcpSettings`). */
  private def tcpSettings(options: SocketOptions)(using Zone): Ptr[NWFrameworkIo.TcpSettings] = {
    val settings = alloc[NWFrameworkIo.TcpSettings]()
    settings._1 = if gears.async.io.internal.Checks.noDelay(options) then 1 else 0
    options.get(SocketOption.KeepAlive) match {
      case null                                             => settings._2 = 0
      case SocketOption.KeepAlive.Off                       => settings._2 = 1
      case SocketOption.KeepAlive.On(idle, interval, count) =>
        settings._2 = 2
        settings._3 = idle
        settings._4 = interval
        settings._5 = count
    }
    settings
  }

  private def hostAndPort(address: IpAddress): (String, Int) = address match {
    case IpAddress.IPv4(host, port) => (host, port)
    case IpAddress.IPv6(host, port) => (host, port)
  }

  def connect(address: IpAddress, options: SocketOptions): Handle = {
    val (host, port) = hostAndPort(address)
    val ptr = Zone.acquire { implicit z => NWFrameworkIo.nwf_connect(toCString(host), port, tcpSettings(options)) }
    val connection = new NWFrameworkHandle(NWFrameworkHandle.Stream, ptr, address)
    connection.opened = options
    add(connection)
  }

  /** A listener that will be listening once Network.framework has it ready, which `NWFrameworkListen` waits for. */
  def listener(address: IpAddress, options: SocketOptions): Handle = {
    val (host, port) = hostAndPort(address)
    val ptr = Zone.acquire { implicit z => NWFrameworkIo.nwf_listen(toCString(host), port, tcpSettings(options)) }
    if ptr == null then throw new IOException(s"Failed to create a listener for $host:$port")
    val listener = new NWFrameworkHandle(NWFrameworkHandle.Listener, ptr, null)
    listener.listenedOn = address
    listener.opened = options
    add(listener)
  }

  /** A datagram socket connected to `remote`, bound to `local` if given. */
  def datagram(local: IpAddress | Null, remote: IpAddress, options: SocketOptions): Handle = {
    val dontFragment = if options.get(SocketOption.DontFragment) == true then 1 else 0
    require(local == null || local.getClass == remote.getClass, "Local and remote addresses must be of the same family")
    val (host, port) = hostAndPort(remote)
    val ptr = Zone.acquire { implicit z =>
      if local != null then {
        val (localHost, localPort) = hostAndPort(local)
        NWFrameworkIo.nwf_datagram_connect(toCString(host), port, toCString(localHost), localPort, dontFragment)
      } else NWFrameworkIo.nwf_datagram_connect(toCString(host), port, null, 0, dontFragment)
    }
    val datagram = new NWFrameworkHandle(NWFrameworkHandle.Datagram, ptr, remote)
    datagram.opened = options
    add(datagram)
  }

  /** Closes a handle, from any thread: the ops pending on it, on whichever reactor, fail there (see `Ops.close` and
    * `NWFrameworkHandle.close`). The number is free for reuse at once.
    */
  def close(handle: Handle): Unit = remove(handle).close()

  /** Marks `handle` closed and takes it out of the table, its number free for reuse at once; throws if it is not open.
    */
  def remove(handle: Handle): NWFrameworkHandle = {
    if handle.close(() => ()) == gears.async.io.kqueue.KqueueBackend.Handle.AlreadyClosed then
      throw new IOException(s"Not an open handle: $handle")
    val target = handle.foreign.asInstanceOf[NWFrameworkHandle]
    table.remove(target.number)
  }

  def errorText(errno: Int): String = fromCString(scala.scalanative.posix.string.strerror(errno))

  /** Decodes an IP socket address filled in by the C side, an IPv4-mapped one as the IPv4 address it maps. */
  def decode(addr: Ptr[socket.sockaddr_storage]): IpAddress = {
    val family = addr.asInstanceOf[Ptr[socket.sockaddr]].sa_family.toInt
    val text = stackalloc[Byte](64)
    if family == socket.AF_INET then {
      val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
      inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
      IpAddress.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
    } else if family == socket.AF_INET6 then {
      val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
      inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
      gears.async.io.internal.IpText.unmapped(IpAddress.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt))
    } else throw new IOException(s"Unsupported address family $family")
  }
}
