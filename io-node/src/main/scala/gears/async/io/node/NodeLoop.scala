package gears.async.io
package node

import gears.async.Async
import gears.async.io.node.base.TimerHeap
import gears.async.io.node.host.NodeErr
import gears.async.io.node.internal.NodeGlobals

import java.util.concurrent.CancellationException
import scala.scalajs.js
import scala.scalajs.js.wasm.JSPI.allowOrphanJSAwait

/** Where the Node.js backend's ops run: Node's event loop, as a program's `Io`.
  *
  * It performs an op for the future performing it, each kind in its own way (`performOp`), and keeps the two things the
  * backend asks of the event loop itself: one timer, for the earliest of every deadline there is (`timers`), and a turn
  * now and then for the callbacks that wait while ops complete without waiting (`spendTurn`).
  */
private[node] object NodeLoop extends Io[NodeOps.type] {
  val backend: NodeBackend.type = NodeBackend
  val ops: NodeOps.type = NodeOps

  private[io] def performWithin(op: NodeOp, timeoutMillis: Long)(using async: Async): Unit = {
    if async.group.isCancelled then throw new CancellationException()
    op.requestEntry()
    try {
      op.link()
      try {
        op.setTimeout(timeoutMillis, System.nanoTime())
        try performOp(op)
        catch case js.JavaScriptException(error) => throw NodeErr.io(error)
        finally op.clearTimeout()
      } finally op.unlink()
    } finally {
      val waited = op.waited
      op.requestExit()
      // An op that ended without waiting, completed or refused, has left the event loop no turn: it is counted.
      if !waited then spendTurn()
    }
  }

  /** Performs `op`, which has been entered: when this returns it is done, and if it throws, that is its failure. */
  private def performOp(op: NodeOp): Unit = op match {
    case op: NodeMakeTimer =>
      if !op.dueAtOnce then {
        op.holdTimerOn(timers)
        op.parkUntilSettled()
      }
    case op: NodeMakeDeadline =>
      if !op.dueAtOnce then {
        op.holdTimerOn(timers)
        op.parkUntilSettled()
      }
    case op: NodeRead               => op.performWhenReady(timers): Unit
    case op: NodeWrite              => op.performWhenReady(timers): Unit
    case op: NodeAccept             => op.performWhenReady(timers): Unit
    case op: NodeReceive            => op.performWhenReady(timers): Unit
    case op: NodeCloseHandle        => if !op.pollClose() then op.parkUntilSettled()
    case op: NodeShutdownOutput     => op.perform()
    case op: NodeFileRead           => op.performCall(timers): Unit
    case op: NodeFileWrite          => op.performCall(timers): Unit
    case op: NodeSync               => op.performCall(timers): Unit
    case op: NodeSend               => op.performCall(timers): Unit
    case op: NodeConnect            => op.performCall(timers): Unit
    case op: NodeListen             => op.performCall(timers): Unit
    case op: NodeOpenDatagram[?]    => op.performCall(timers): Unit
    case op: NodeOpenFile           => op.performCall(timers): Unit
    case op: NodeOpenFifoForReading => op.performCall(timers): Unit
    case op: NodeOpenFifoForWriting => op.performCall(timers): Unit
    case op: NodeResolve            => op.performCall(timers): Unit
  }

  // ---- The one timer

  /** Every deadline of the backend: an op's own, its time limit, a FIFO's next look. */
  val timers: TimerHeap = new TimerHeap(() => rearm())

  /** The longest Node's timers wait, in milliseconds: beyond 2^31 - 1 they fire at once. */
  private inline val LongestWait = 1L << 30

  /** How many timers are fired before the event loop is given a turn. */
  private inline val TimerBudget = 32

  private var timeout: js.UndefOr[js.timers.SetTimeoutHandle] = js.undefined
  private var immediate: js.UndefOr[js.Dynamic] = js.undefined

  /** The deadline Node's timer was set for, if one is set. */
  private var armedFor = 0L

  private def armed: Boolean = timeout.isDefined || immediate.isDefined

  private def disarm(): Unit = {
    timeout.foreach(js.timers.clearTimeout(_))
    immediate.foreach(NodeGlobals.clearImmediate(_))
    timeout = js.undefined
    immediate = js.undefined
  }

  private val fire: js.Function0[Unit] = () => {
    timeout = js.undefined
    immediate = js.undefined
    val now = System.nanoTime()
    var budget = TimerBudget
    while budget > 0 do {
      val timer = timers.nextReadyTimer(now)
      if timer == null then budget = 0
      else {
        timer.timerWakeup(now)
        budget -= 1
      }
    }
    // What a timer's waiter did as it fired may have armed the timer already.
    if !armed && !timers.isEmpty then arm(timers.peekNextDeadline())
  }

  /** Node counts whole milliseconds, on a clock of its own, and may fire a little early by this one: a timer that fires
    * before its time finds nothing due, and is set again. A deadline that has come is fired on the event loop's next
    * turn, never from within the call that found it so. With no deadline in sight, a timer is set all the same, for as
    * long as Node allows: a program that waits for ever is still running, and without a timer of Node's, Node would end
    * the process.
    */
  private def arm(deadline: Long): Unit = {
    armedFor = deadline
    val left = if deadline == TimerHeap.Never then TimerHeap.Never else deadline - System.nanoTime()
    if left <= 0 then immediate = NodeGlobals.setImmediate(fire)
    else timeout = js.timers.setTimeout(math.min((left + 999_999L) / 1_000_000L, LongestWait).toDouble)(fire())
  }

  /** The heap's earliest deadline has changed, or the heap has emptied. A timer of Node's set for an earlier time than
    * is now needed is left: it finds nothing due, at worst. One is cleared only when nothing waits at all, so that it
    * does not keep the process running.
    */
  private def rearm(): Unit =
    if timers.isEmpty then disarm()
    else {
      val next = timers.peekNextDeadline()
      if !armed then arm(next)
      else if next < armedFor then {
        disarm()
        arm(next)
      }
    }

  // ---- Turns

  private inline val TurnBudget = 32
  private var spent = 0

  /** Lets the event loop turn among ops that end without waiting: a future whose every op is ready at once, a read at
    * the end of its stream say, or refused at once, one on a closed handle, would otherwise never leave the loop a
    * turn, and no timer would fire nor any socket be heard. Every `TurnBudget` such ops, whichever futures perform
    * them, the one performing waits for the loop's next turn.
    */
  private def spendTurn(): Unit = {
    spent += 1
    if spent >= TurnBudget then {
      spent = 0
      js.await(new js.Promise[Unit]((resolve, _) => NodeGlobals.setImmediate(() => resolve(()): Unit): Unit))
    }
  }
}
