package asyncio.nio

import asyncio.BlockingBackend
import asyncio.BufferPool
import asyncio.FileBackend
import asyncio.PipeBackend
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.ReadinessBackend
import asyncio.ResolveBackend
import asyncio.UnixStreamBackend

/** NIO's reactors, each over a `Selector` of its own. They all run the same ops, which keep no state of one reactor
  * between submissions, so an op may go to any of them in turn, and handles name entries of one process-wide table.
  * They have IP and Unix stream sockets, regular files and name resolution on the blocker pool, pipes, readiness and
  * blocking ops, but neither Unix datagram sockets nor FIFOs, which the JDK cannot wait on.
  */
object NioBackend
    extends ReactorBackend
    with ReadinessBackend
    with BlockingBackend
    with UnixStreamBackend
    with FileBackend
    with ResolveBackend
    with PipeBackend {

  type Op = NioOp

  def ops: NioOps.type = NioOps

  def handles: NioHandles.type = NioHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  def open(): Reactor.Of[Op] = NioReactor.open()

  def pool(bufferSize: Int, buffers: Int): BufferPool = new DirectPool(bufferSize, buffers)

  /** A command tried first is tried here: a non-blocking channel may be read and written from any thread, and trying
    * touches no reactor. A `whenReady`, a timer or a blocking op is not.
    */
  override def tryNow(op: Op): Boolean = op match {
    case command: Command if command.tryFirst =>
      require(command.completion == null, s"$op is already pending")
      command.entry = NioHandles.entry(command.handle)
      val done = command.perform()
      // Just tried, so the reactor it goes to next waits for readiness at once, which a select reports if it came since.
      command.triedNow = !done
      done
    // A timer of no time is due now. It yields first, so that, as on a reactor, where its completion runs a turn
    // later, the other threads ready to run get their turn: a loop of them stays fair, and costs no hand-off.
    case timer: NioTimer if timer.milliseconds == 0 =>
      require(timer.completion == null, s"$op is already pending")
      Thread.`yield`()
      true
    case _ => false
  }

  /** A command tried first can be tried on the caller; a `whenReady`, a timer or a blocking op cannot. */
  override def triesNow(op: Op): Boolean = op match {
    case command: Command => command.tryFirst
    case _                => false
  }

  /** A command tried first waits on its handle's own selector, in the caller's thread (`SelectorWait`); a regular
    * file is always ready, so `tryNow` finishes it. A timer parks the caller until its time (`TimerWait`): no reactor
    * thread to fire it and resume the caller, which the OS may keep from running while the cores are busy.
    */
  override def waitHere(op: Op): asyncio.WaitHere | Null = op match {
    case command: Command if command.tryFirst =>
      NioHandles.entry(command.handle) match {
        case s: Selectable =>
          new SelectorWait(s, command.interest == asyncio.Interest.Write, command.selectOps(s.channel))
        case _ => null
      }
    case timer: NioTimer =>
      require(timer.completion == null, s"$op is already pending")
      new TimerWait(timer.milliseconds)
    case _ => null
  }

  /** A blocking op runs on the caller under the same rules as on the blocker pool (see `NioBlocking`). */
  override def blockingHere(op: Op): asyncio.BlockingHere | Null = op match {
    case blocking: NioBlocking => blocking.here()
    case _                     => null
  }
}
