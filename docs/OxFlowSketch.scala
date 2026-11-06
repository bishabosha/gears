package gears.streaming.sketch.interop

import gears.async.Async
import gears.async.io.{Blocking, Io}
import gears.streaming.sketch.Stream
import ox.Ox
import ox.flow.{Flow as OxFlow}

/** Optional JVM-only ox-interop module, layered on javaFlow's Publisher bridge.
  * Proposals only; runtimes below remain `???`. Ox is not a core dependency.
  *
  * Ox already supplies Flow.fromPublisher and flow.toPublisher. The latter
  * requires a live Ox scope; both use BufferCapacity to bound their buffering.
  * API checked against cached Ox 1.0.2 sources and current official docs:
  * https://ox.softwaremill.com/latest/streaming/flows.html#reactive-streams-interoperability
  *
  * Running Ox Flow is a blocking operation. These Gears-owned helpers request
  * Io[Blocking] so the foreign run/scope stays off a backend's I/O workers.
  * Calling these constructors does not request or retain Async or Ox.
  */
object oxFlow:
  /** Construct the Ox source at traversal time, inside a fresh Ox owner scope.
    * Export it with Ox's toPublisher, consume using javaFlow.fromPublisher.
    *
    * Run the Ox owner within a blocking op in an owned Gears child task. Publish
    * the handoff via thread-safe state, then keep the Ox scope open until Gears
    * consumption ends. On cancellation cancel the subscription AND interrupt
    * that owner, then join it: cancelling Flow alone is not an acknowledgement
    * that the remote Ox forks have stopped. Failures must wake both sides.
    * The current BlockingOps contract lets perform return after its body stops.
    */
  def fromFlow[A](create: Ox ?=> OxFlow[A], capacity: Int = 16)(using Io[Blocking]): Stream[A] = ???

  /** A lazy island of Ox operators inside a Gears pipeline:
    *
    * Gears -> scoped Publisher -> Ox.fromPublisher -> transform
    *       -> Ox.toPublisher -> Gears.fromPublisher -> Gears
    *
    * Both owners stay alive for the whole traversal. Start receiving before
    * waiting for the other side to complete; teardown must wake both sides.
    * The transform receives that run's Ox, not a captured construction-time Ox.
    * It runs once per traversal, on the Ox side. Capacity is explicit
    * on BOTH libraries; independent bridge buffers add rather than disappear.
    */
  def via[A, B](source: Stream[A], capacity: Int = 16)(
      transform: OxFlow[A] => Ox ?=> OxFlow[B]
  )(using Io[Blocking]): Stream[B] = ???

  /** Safe terminal use from Gears. Export a scoped Gears Publisher, then run
    * Ox.supervised { consume(OxFlow.fromPublisher(publisher)) } on the backend's
    * blocking facility, with its own Ox capability. Await completion using the
    * caller's Async outside the blocking thunk; no Async is passed into Ox.
    */
  def run[A, R](source: Stream[A], capacity: Int = 16)(
      consume: OxFlow[A] => Ox ?=> R
  )(using Io[Blocking], Async): R = ???

/* Do not directly call oxFlow.runForeach(gearsEmitter.emit) on a Gears I/O
 * worker: Ox can block, Gears emission can suspend, and emitters are local to
 * their execution fiber. Interop should cross a bounded protocol boundary.
 *
 * Scope nesting does not establish cross-runtime cancellation by itself.
 * Gears cancellation must explicitly stop/join the Ox owner. Ox interruption
 * and failures must wake/fail the Gears bridge, without misreporting them as EOF.
 * Data/terminal/demand signals cross the boundary; Async, Ox, their thread-local
 * contexts, and pooled buffer ownership do not cross it implicitly.
 *
 * An application already owned by Ox can use javaFlow.withPublisher inside a
 * dedicated Gears run and OxFlow.fromPublisher on the Ox side. The API above is
 * specifically the Gears-owned entry point; it does not require an outer Ox.
 *
 * Start with the standard Publisher bridge. A specialized direct channel bridge
 * might remove protocol/buffer overhead, but needs the same lifecycle contract
 * and measurement before introducing a second implementation.
 */

object OxFlowExamples:
  /** No Async in scope while assembling a pipeline that uses Ox operators. */
  def transformed(input: Stream[Int])(using Io[Blocking]): Stream[String] =
    oxFlow.via(input, capacity = 16): flow =>
      flow.mapPar(4)(n => s"value=$n").filter(_.nonEmpty)
