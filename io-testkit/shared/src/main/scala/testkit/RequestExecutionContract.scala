package gears.async.io.testkit

import gears.async.io.internal.*
import gears.async.io.{Pipes, Repeatable, Slot}
import gears.async.{Async, Cancellable, CompletionGroup, Future}

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Callable, CancellationException, CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration.*

/** Experimental descriptions exercise the real backend records, including their migration between poller threads. */
private[io] trait RequestExecutionContract extends munit.FunSuite {
  val backend: ReactorBackend & Pipes
  protected def prepareRead(request: ReadRequest[backend.Readable]): backend.Op & Repeatable
  protected def prepareTimer(request: TimerRequest): backend.Op & Repeatable
  private type R = Reactor.Of[backend.Op, backend.Handle]

  private class Outcome extends Completion[backend.Op] {
    var completed = 0
    var cancelled = 0
    var failure: Throwable | Null = null
    def done: Boolean = completed > 0 || cancelled > 0 || failure != null
    def onComplete(op: backend.Op): Unit = completed += 1
    override def onCancel(op: backend.Op): Unit = cancelled += 1
    override def onFailure(op: backend.Op, error: Throwable): Unit = failure = error
  }

  private def until(reactor: R)(done: => Boolean): Unit = {
    val limit = System.nanoTime() + 5_000_000_000L
    while !done do {
      if System.nanoTime() >= limit then fail("the execution did not complete")
      reactor.runOnce(false)
    }
  }

  private def perform(reactor: R, op: backend.Op): Unit = {
    val outcome = new Outcome
    reactor.submit(op, outcome)
    until(reactor)(outcome.done)
    if outcome.failure != null then throw outcome.failure.nn
    assertEquals(outcome.completed, 1)
  }

  private def withPipe(body: (R, backend.Readable, backend.Writable) => Unit): Unit = {
    val reactor = backend.open()
    val read = Slot[backend.Readable]()
    val write = Slot[backend.Writable]()
    try {
      perform(reactor, backend.ops.pipe(read, write))
      val in = read.clear()
      val out = write.clear()
      try body(reactor, in, out)
      finally {
        perform(reactor, backend.ops.close(in))
        perform(reactor, backend.ops.close(out))
      }
    } finally reactor.close()
  }

  test("equal data descriptions prepare distinct executions without consumer or cancellation state") {
    withPipe { (_, in, _) =>
      val first = ReadRequest(in, ByteBuffer.allocate(1))
      val second = ReadRequest(in, ByteBuffer.allocate(1))
      assertEquals(first, second) // ByteBuffer equality compares the remaining bytes.
      assertEquals(first.productArity, 2)
      val value: AnyRef = first
      assert(!value.isInstanceOf[Cancellable])
      assert(!value.isInstanceOf[Async.Source[?]])
      val a = prepareRead(first)
      val b = prepareRead(second)
      assert(!(a eq b))
      assert(a.isInstanceOf[ReactorOperation])
    }
  }

  test("a prepared read follows buffer progress and rechecks refusal on every reuse") {
    withPipe { (reactor, in, out) =>
      val buffer = ByteBuffer.allocate(1)
      val request = ReadRequest(in, buffer)
      val execution = prepareRead(request)
      for n <- 1 to 8 do {
        buffer.clear()
        perform(reactor, backend.ops.write(out, ByteBuffer.wrap(Array(n.toByte))))
        perform(reactor, execution)
        assertEquals(buffer.position(), 1)
        assertEquals(buffer.get(0), n.toByte)
        assert(request.buffer eq buffer)
      }
      intercept[IllegalArgumentException](perform(reactor, execution))
      assertEquals(buffer.position(), 1)
    }
  }

  test("relative timer descriptions create independent executions, each reusable") {
    val reactor = backend.open()
    try {
      val request = TimerRequest(Duration.Zero)
      val first = prepareTimer(request)
      val second = prepareTimer(request)
      assert(!(first eq second))
      val a = new Outcome
      val b = new Outcome
      reactor.submit(first, a)
      reactor.submit(second, b)
      until(reactor)(a.done && b.done)
      assertEquals((a.completed, b.completed), (1, 1))
      perform(reactor, first)
      perform(reactor, second)
    } finally reactor.close()
  }

  test("a prepared read on a closed handle is rejected without touching its buffer") {
    val reactor = backend.open()
    val read = Slot[backend.Readable]()
    val write = Slot[backend.Writable]()
    try {
      perform(reactor, backend.ops.pipe(read, write))
      val in = read.clear()
      val out = write.clear()
      try {
        val buffer = ByteBuffer.allocate(1)
        val execution = prepareRead(ReadRequest(in, buffer))
        perform(reactor, backend.ops.close(in))
        intercept[IOException](perform(reactor, execution))
        assertEquals(buffer.position(), 0)
      } finally perform(reactor, backend.ops.close(out))
    } finally reactor.close()
  }

  private class Owner extends ReactorOperation.Executor {
    private val executor = Executors.newSingleThreadExecutor()
    private val thread = on(Thread.currentThread())
    val reactor: R = on(backend.open())
    def isCurrent: Boolean = Thread.currentThread() eq thread
    def execute(task: Runnable): Unit = executor.execute(task)
    def on[A](body: => A): A = executor.submit(new Callable[A] { def call(): A = body }).get(5, TimeUnit.SECONDS)
    def close(): Unit = {
      try on(reactor.close())
      finally executor.shutdownNow(): Unit
    }
  }

  for sameOwner <- List(true, false) do
    test(s"an old cancellation cannot reach a prepared read reused on a poller (same worker: $sameOwner)") {
      withPipe { (_, in, _) =>
        val request = ReadRequest(in, ByteBuffer.allocate(1))
        val execution = prepareRead(request)
        val first = new Owner
        val second = if sameOwner then first else new Owner
        val oldGroup = new CompletionGroup
        val newGroup = new CompletionGroup
        val entered = new CountDownLatch(1)
        val proceed = new CountDownLatch(1)
        val error = new AtomicReference[Throwable]()
        val blocker = new Cancellable {
          def cancel(): Unit = {
            entered.countDown()
            val released = proceed.await(5, TimeUnit.SECONDS)
            assert(released)
          }
        }
        blocker.link(oldGroup)
        val canceller = new Thread(() => {
          try oldGroup.cancel()
          catch case failure: Throwable => error.set(failure)
        })
        var active: Owner | Null = null
        try {
          first.on {
            execution.beginAwait(first)
            first.reactor.submit(execution, execution)
            execution.link(oldGroup)
          }
          active = first
          canceller.start()
          val captured = entered.await(5, TimeUnit.SECONDS)
          assert(captured)
          val cancelled = first.on {
            val result = first.reactor.cancel(execution)
            execution.unlink()
            execution.endAwait()
            result
          }
          active = null
          assert(cancelled)
          second.on {
            execution.beginAwait(second)
            second.reactor.submit(execution, execution)
            execution.link(newGroup)
          }
          active = second
          proceed.countDown()
          canceller.join(5000)
          assert(!canceller.isAlive)
          assertEquals(error.get(), null)
          first.on(())
          val pending = second.on(execution.poll())
          assert(pending.isEmpty)
          newGroup.cancel()
          val result = second.on(execution.poll())
          assert(result.get.failed.get.isInstanceOf[CancellationException])
          assertEquals(request.buffer.position(), 0)
          second.on { execution.unlink(); execution.endAwait() }
          active = null
        } finally {
          proceed.countDown()
          canceller.join(5000)
          if active != null then
            active.nn.on {
              active.nn.reactor.cancel(execution)
              execution.unlink()
              execution.endAwait()
            }
          try if !sameOwner then second.close()
          finally first.close()
        }
      }
    }

  for workers <- List(1, 4); fresh <- List(false, true) do
    test(s"immediate reads remain fair to a sibling timer (workers: $workers, fresh execution: $fresh)") {
      ReactorRuns.run(workers)(using backend) { io ?=>
        val read = Slot[backend.Readable]()
        val write = Slot[backend.Writable]()
        io.perform(backend.ops.pipe(read, write))
        val in = read.clear()
        io.perform(backend.ops.close(write.clear()))
        try {
          val request = ReadRequest(in, ByteBuffer.allocate(1))
          val execution = prepareRead(request)
          val stopped = new AtomicBoolean()
          val started = new CountDownLatch(1)
          val busy = Future {
            started.countDown()
            var count = 0
            while !stopped.get() && count < 500000 do {
              io.perform(if fresh then prepareRead(request) else execution)
              count += 1
            }
            assert(stopped.get(), "immediate readiness monopolized the worker")
            count
          }
          val ticker = Future {
            // Do not let worker startup order make a zero-read run pass as a fairness check.
            while started.getCount() != 0 do io.perform(prepareTimer(TimerRequest(Duration.Zero)))
            try io.perform(prepareTimer(TimerRequest(5.millis)))
            finally stopped.set(true)
          }
          ticker.await
          val completed = busy.await
          assert(completed > 0)
        } finally io.perform(backend.ops.close(in))
      }
    }
}
