package asyncio.gears

import munit.FunSuite
import asyncio.Backend
import asyncio.kqueue.KqueueBackend

/** Checks of the pool's parts under contention, each across several threads. */
class ReactorPoolSuite extends FunSuite {
  given Backend = KqueueBackend

  test("the run queue runs every task once while other threads take tasks from it") {
    val outcome = RunQueueCheck.run(tasks = 2_000_000, takers = 3)
    assertEquals(outcome.failures, 0L)
    assert(outcome.moved > 0, "nothing was moved, so the check proves little")
  }

  test("tasks queued for a worker in particular run on its thread, while another worker takes tasks") {
    val rounds = 500
    val outcome = InboxCheck.run(rounds)
    assertEquals(outcome.ran, rounds.toLong)
    assertEquals(outcome.elsewhere, 0L)
    assert(outcome.taken > 0, "no task was taken, so the check proves little")
  }

  test("a suspended future resumes on another thread with its state and that thread's signal state") {
    val outcome = MigrateCheck.run(futures = 4, resumes = 20_000)
    assertEquals(outcome.failures, 0L)
    assert(outcome.moved >= outcome.resumes / 2, s"only ${outcome.moved} of ${outcome.resumes} resumes moved threads")
  }
}
