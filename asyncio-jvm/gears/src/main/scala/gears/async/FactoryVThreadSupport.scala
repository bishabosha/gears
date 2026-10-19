package gears.async

import java.util.concurrent.ThreadFactory
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.unchecked.uncheckedVariance

/** `VThreadSupport`, with the virtual threads made by `factory` instead of gears' own, and `S` as its scheduler: so
  * that the threads can run on a scheduler of the caller's, such as a reactor pool, which `factory`'s threads are
  * carried by. In gears' package, since the hooks it overrides are gears' own.
  */
final class FactoryVThreadSupport[S <: Scheduler](factory: ThreadFactory) extends AsyncSupport:

  type Scheduler = S

  private def start(body: Runnable): Unit = factory.newThread(body).start()

  private[async] final class FactoryLabel[R]():
    private var result: Option[R] = None
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    def clearResult() =
      lock.lock()
      result = None
      lock.unlock()

    def setResult(data: R) =
      lock.lock()
      try
        result = Some(data)
        cond.signalAll()
      finally lock.unlock()

    def waitResult(): R =
      lock.lock()
      try
        while result.isEmpty do cond.await()
        result.get
      finally lock.unlock()

  override type Label[R] = FactoryLabel[R]

  private[async] final class FactorySuspension[-T, +R](using val l: Label[R] @uncheckedVariance)
      extends gears.async.Suspension[T, R]:
    private var nextInput: Option[T] = None
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    def setInput(data: T) =
      lock.lock()
      try
        nextInput = Some(data)
        cond.signalAll()
      finally lock.unlock()

    def waitInput(): T @uncheckedVariance =
      lock.lock()
      try
        while nextInput.isEmpty do cond.await()
        nextInput.get
      finally lock.unlock()

    override def resume(arg: T): R =
      l.clearResult()
      setInput(arg)
      l.waitResult()

  override type Suspension[-T, +R] = FactorySuspension[T, R]

  override def boundary[R](body: (Label[R]) ?=> R): R =
    val label = FactoryLabel[R]()
    start: () =>
      val result = body(using label)
      label.setResult(result)
    label.waitResult()

  override private[async] def resumeAsync[T, R](suspension: Suspension[T, R])(arg: T)(using Scheduler): Unit =
    suspension.l.clearResult()
    suspension.setInput(arg)

  override def scheduleBoundary(body: (Label[Unit]) ?=> Unit)(using Scheduler): Unit =
    start: () =>
      val label = FactoryLabel[Unit]()
      body(using label)

  override def suspend[T, R](body: Suspension[T, R] => R)(using l: Label[R]): T =
    val sus = new FactorySuspension[T, R]()
    val res = body(sus)
    l.setResult(res)
    sus.waitInput()
