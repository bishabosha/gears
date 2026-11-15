package gears.async.io
package node

/** A timer of a duration, begun as it is performed. */
private[node] final class TimerOp(nanos: Long) extends NodeOp with Repeatable {
  private var due = Clock.Never

  protected def start(run: Run): Unit = {
    due = Clock.after(run.start, nanos)
    if nanos == 0 then run.succeed()
    else {
      val alarm = Clock.at(due)(() => run.succeed())
      run.release = () => alarm.cancel()
    }
  }

  override private[node] def settle(run: Run): Unit = if Clock.passed(due) then run.succeed()
}

/** A timer of a deadline, which may be moved while it waits. */
private[node] final class DeadlineOp(private var time: Long) extends NodeOp with Repeatable with Deadline {
  private var waiting: Run | Null = null
  private var alarm: Clock.Alarm | Null = null

  def deadline: Long = time

  def moveTo(nanoTime: Long): Unit = {
    time = nanoTime
    val run = waiting
    if run != null then arm(run)
  }

  private def arm(run: Run): Unit = {
    val old = alarm
    if old != null then old.cancel()
    alarm = Clock.at(time)(() => run.succeed())
  }

  protected def start(run: Run): Unit =
    if time - System.nanoTime() <= 0 then run.succeed()
    else {
      waiting = run
      arm(run)
      run.release = () => {
        waiting = null
        val armed = alarm
        if armed != null then armed.cancel()
        alarm = null
      }
    }

  override private[node] def settle(run: Run): Unit = if time - System.nanoTime() <= 0 then run.succeed()
}
