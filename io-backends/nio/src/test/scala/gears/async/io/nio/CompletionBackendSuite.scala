package gears.async.io.internal

import gears.async.io.*
import gears.async.{Async, Future}

import java.util.concurrent.{CancellationException, CountDownLatch, Semaphore}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** A completion-only driver: submission never attempts readiness, and cancellation needs a later completion from the
  * producer. This exercises the backend contract without a selector, file descriptor or readiness command.
  */
class CompletionBackendSuite extends munit.FunSuite {
  private class Operation extends ReactorOperation {
    val submitted = new CountDownLatch(1)
    @volatile var result: Try[Unit] | Null = null
    @volatile var cancelRequested = false
    @volatile var reactor: CompletionReactor | Null = null
    def finish(result: Try[Unit]): Unit = {
      this.result = result
      reactor.nn.wakeup()
    }
  }

  /** The ops of this backend: none it builds, since its one kind of op is made by the test. */
  private object CompletionOps extends Handles {
    type Op = Operation
    type Handle = AnyRef
    def timer(duration: scala.concurrent.duration.FiniteDuration): Op & Repeatable =
      throw new UnsupportedOperationException
    def deadlineTimer(nanoTime: Long): Op & Repeatable & Deadline = throw new UnsupportedOperationException
    def close(handle: Handle): Op = throw new UnsupportedOperationException
  }

  private class CompletionReactor extends Reactor {
    type Op = Operation
    type Handle = AnyRef
    private val pending = mutable.LinkedHashMap.empty[Operation, Completion[Operation]]
    private val wake = new Semaphore(0)
    private var stopped = false
    val owner = Thread.currentThread()
    def ops: CompletionOps.type = CompletionOps
    def attachment(op: Op): AnyRef | Null = null
    def attach(op: Op, value: AnyRef | Null): Unit = ()
    def submit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Long): Unit = {
      assert(VirtualCarriers.currentCarrier() eq owner)
      pending(op) = completion.asInstanceOf[Completion[Operation]]
      op.reactor = this
      op.submitted.countDown()
    }
    def trySubmit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Long): Boolean = {
      submit(op, completion, timeoutMillis)
      false
    }
    def cancel(op: Op): Boolean = {
      assert(VirtualCarriers.currentCarrier() eq owner)
      if !pending.contains(op) then false
      else {
        op.cancelRequested = true
        true // The producer still owns its resources, so there is no callback yet.
      }
    }
    def runOnce(block: Boolean): Unit = {
      if block && !pending.keysIterator.exists(_.result != null) then wake.acquire()
      wake.drainPermits()
      val completed = pending.iterator.filter(_._1.result != null).toList
      completed.foreach { (op, callback) =>
        pending.remove(op)
        op.result.nn match {
          case Success(_)                        => deliverComplete(callback, op)
          case Failure(_: CancellationException) => deliverCancel(callback, op)
          case Failure(error)                    => deliverFailure(callback, op, error)
        }
      }
    }
    def wakeup(): Unit = wake.release()
    def stop(): Unit = { stopped = true; wakeup() }
    def run(): Unit = while !stopped do runOnce(true)
    def close(): Unit = {
      val remaining = pending.toList
      pending.clear()
      remaining.foreach((op, callback) => deliverCancel(callback, op))
    }
  }

  private object Backend extends ReactorBackend {
    type Interface = CompletionOps.type
    val ops: CompletionOps.type = CompletionOps
    def pool(bufferSize: Int, buffers: Int): BufferPool = throw new UnsupportedOperationException
    def open(): Reactor.Of[ops.Op, ops.Handle] = new CompletionReactor
  }

  test("a foreign completion resumes on the poller's worker without readiness") {
    val op = new Operation
    val producer = new Thread(() => { op.submitted.await(); op.finish(Success(())) })
    producer.start()
    try
      ReactorRuns.run(1)(using Backend) { io ?=>
        val scheduler = summon[Async].scheduler.asInstanceOf[ReactorScheduler]
        val owner = scheduler.currentWorker
        io.perform(op)
        assert(scheduler.currentWorker eq owner)
      }
    finally { producer.interrupt(); producer.join() }
  }

  test("cancellation keeps the consumer suspended until a foreign completion acknowledges it") {
    val op = new Operation
    val release = new CountDownLatch(1)
    val producer = new Thread(() => {
      op.submitted.await()
      release.await()
      op.finish(Failure(new CancellationException()))
    })
    producer.start()
    try
      ReactorRuns.run(1)(using Backend) { io ?=>
        val consumer = Future(io.perform(op))
        try {
          while op.submitted.getCount > 0 do io.`yield`()
          consumer.cancel()
          while !op.cancelRequested do io.`yield`()
          assert(consumer.poll().isEmpty)
        } finally release.countDown()
        assert(consumer.awaitResult.failed.get.isInstanceOf[CancellationException])
      }
    finally {
      release.countDown()
      producer.interrupt()
      producer.join()
    }
  }
}
