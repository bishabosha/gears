package asyncio.nio

import asyncio.Reactor
import asyncio.UnixStreamBackend

/** NIO's reactors, each over a `Selector` of its own. They all run the same ops, which keep no state of one reactor
  * between submissions, so an op may go to any of them in turn, and handles name entries of one process-wide table.
  * Besides what every backend has, they have Unix stream sockets, but neither Unix datagram sockets nor FIFOs, which the
  * JDK cannot wait on.
  */
object NioBackend extends UnixStreamBackend {

  type Op = NioOp

  def ops: NioOps.type = NioOps

  def handles: NioHandles.type = NioHandles

  def attachment(op: Op): AnyRef | Null = op.attachment

  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  def open(): Reactor.Of[Op] = NioReactor.open()

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
    case _ => false
  }

  /** A blocking op runs on the caller under the same rules as on the blocker pool (see `NioBlocking`). */
  override def blockingHere(op: Op): asyncio.BlockingHere | Null = op match {
    case blocking: NioBlocking => blocking.here()
    case _                     => null
  }
}
