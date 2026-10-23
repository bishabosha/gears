package gears.async.io
package testkit

/** What a backend that resolves host names must do (see `ReactorContract`). */
trait ResolveContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & ResolveBackend

  test("resolve finds localhost") {
    withReactor { r =>
      val into = Slot[List[ResolvedAddress]]()
      val outcome = Outcome()
      r.submit(backend.ops.resolve("localhost", into), outcome)
      turnUntil(r)(outcome.finished)
      assertEquals(outcome.completed, 1, String.valueOf(outcome.failure))
      val found = into.clear()
      assert(found.exists(a => a.host == "127.0.0.1" || a.family == AddressFamily.IPv6), found.toString)
    }
  }

}
