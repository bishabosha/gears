package gears.async.io
package kqueue

import gears.async.Async
import gears.async.io.internal.ReactorRuns
import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks
import gears.io.testkit.SimpleIOIpStreamChecks
import gears.io.testkit.SimpleIOResolveChecks

/** The testkit's contracts over kqueue's reactors, on `Io.run`'s pool and on one worker, each suite seeing only the
  * capabilities its contracts ask for.
  */

/** Runs a suite's programs on a pool of `workers` workers driving the backend's reactors, rather than `Io.run`'s one
  * per processor.
  */
trait OnWorkers(workers: Int) extends ContractSuite {
  override protected def running[T](body: Io[backend.Interface] ?=> Async.Spawn ?=> T): T =
    ReactorRuns.run(workers)(using backend)(body)
}

// On Io.run's pool, a worker per processor.

class KqueueOpsSuite extends OpsContract {
  val backend: Backend = KqueueBackend
}

class KqueueThreadedTimerSuite extends ThreadedTimerContract {
  val backend: Backend = KqueueBackend
}

class KqueueBlockingSuite extends BlockingContract {
  val backend: Backend { type Interface <: Blocking } = KqueueBackend
}

class KqueuePipeSuite extends PipeContract {
  val backend: Backend { type Interface <: Pipes } = KqueueBackend
}

class KqueueSocketStreamSuite extends StreamContract with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = KqueueBackend
}

class KqueueIpStreamSuite extends IpStreamContract {
  val backend: Backend { type Interface <: IpStreams } = KqueueBackend
}

class KqueueIpDatagramSuite extends IpDatagramContract {
  val backend: Backend { type Interface <: IpDatagrams } = KqueueBackend
}

class KqueueUnixStreamSuite extends UnixStreamContract {
  val backend: Backend { type Interface <: UnixStreams } = KqueueBackend
}

class KqueueFileSuite extends FileContract {
  val backend: Backend { type Interface <: RegularFiles } = KqueueBackend
}

class KqueueResolveSuite extends ResolveContract {
  val backend: Backend { type Interface <: Resolving } = KqueueBackend

  override protected def namedScopedAddresses: List[(String, IpAddress)] = ScopedNames.linkLocal
}

class KqueueSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Backend { type Interface <: Pipes } = KqueueBackend
}

class KqueueSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = KqueueBackend
}

class KqueueSimpleIOIpStreamSuite extends SimpleIOIpStreamChecks {
  val backend: Backend { type Interface <: IpStreams } = KqueueBackend
}

class KqueueSimpleIOResolveSuite extends SimpleIOResolveChecks {
  val backend: Backend { type Interface <: Resolving } = KqueueBackend
}

class KqueueUnixDatagramSuite extends UnixDatagramContract {
  val backend: Backend { type Interface <: UnixDatagrams } = KqueueBackend
}

class KqueueFifoSuite extends FifoContract {
  val backend: Backend { type Interface <: Fifos } = KqueueBackend
}

class KqueueFifoFileSuite extends FifoFileContract {
  val backend: Backend { type Interface <: Fifos & RegularFiles } = KqueueBackend
}

// On one worker, where every future shares one reactor.

class KqueueOnOneOpsSuite extends OpsContract with OnWorkers(1) {
  val backend: Backend = KqueueBackend
}

class KqueueOnOnePipeSuite extends PipeContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Pipes } = KqueueBackend
}

class KqueueOnOneSocketStreamSuite extends StreamContract with IpStreamFixture with OnWorkers(1) {
  val backend: Backend { type Interface <: IpStreams } = KqueueBackend
}

class KqueueOnOneIpStreamSuite extends IpStreamContract with OnWorkers(1) {
  val backend: Backend { type Interface <: IpStreams } = KqueueBackend
}

class KqueueOnOneSimpleIOSuite extends SimpleIOChecks with PipeFixture with OnWorkers(1) {
  val backend: Backend { type Interface <: Pipes } = KqueueBackend
}

// Runs that share handles, and what a run leaves when it ends: on the pool and on one worker.
class KqueueConcurrentRunsSuite extends ConcurrentRunsContract {
  val backend: Backend { type Interface <: Pipes } = KqueueBackend
}

class KqueueOnOneConcurrentRunsSuite extends ConcurrentRunsContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Pipes } = KqueueBackend
}

class KqueueRunEndSuite extends RunEndContract {
  val backend: Backend { type Interface <: Blocking & Pipes } = KqueueBackend
}

class KqueueOnOneRunEndSuite extends RunEndContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Blocking & Pipes } = KqueueBackend
}

class KqueueOnOneBlockingSuite extends BlockingContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Blocking } = KqueueBackend
}
