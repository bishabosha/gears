package gears.async.io
package nio

import gears.async.Async
import gears.async.io.internal.ReactorRuns
import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks
import gears.io.testkit.SimpleIOIpStreamChecks
import gears.io.testkit.SimpleIOResolveChecks

/** The testkit's contracts over NIO's backends: `NioBackend` on `Io.run`'s pool and on one worker, and `NioOnCaller`,
  * each suite seeing only the capabilities its contracts ask for.
  */

/** Runs a suite's programs on a pool of `workers` workers driving the backend's reactors, rather than `Io.run`'s one
  * per processor.
  */
trait OnWorkers(workers: Int) extends ContractSuite {
  override protected def running[T](body: Io[backend.Interface] ?=> Async.Spawn ?=> T): T =
    ReactorRuns.run(workers)(using backend)(body)
}

// NioBackend on Io.run's pool, a worker per processor.

class NioOpsSuite extends OpsContract {
  val backend: Backend = NioBackend
}

class NioThreadedTimerSuite extends ThreadedTimerContract {
  val backend: Backend = NioBackend
}

class NioBlockingSuite extends BlockingContract {
  val backend: Backend { type Interface <: Blocking } = NioBackend
}

class NioPipeSuite extends PipeContract {
  val backend: Backend { type Interface <: Pipes } = NioBackend
}

class NioSocketStreamSuite extends StreamContract with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = NioBackend
}

class NioIpStreamSuite extends IpStreamContract {
  val backend: Backend { type Interface <: IpStreams } = NioBackend
}

class NioIpDatagramSuite extends IpDatagramContract {
  val backend: Backend { type Interface <: IpDatagrams } = NioBackend
}

class NioUnixStreamSuite extends UnixStreamContract {
  val backend: Backend { type Interface <: UnixStreams } = NioBackend
}

class NioFileSuite extends FileContract {
  val backend: Backend { type Interface <: RegularFiles } = NioBackend
}

class NioResolveSuite extends ResolveContract {
  val backend: Backend { type Interface <: Resolving } = NioBackend

  override protected def namedScopedAddresses: List[(String, IpAddress)] = ScopedNames.linkLocal
}

class NioSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Backend { type Interface <: Pipes } = NioBackend
}

class NioSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = NioBackend
}

class NioSimpleIOIpStreamSuite extends SimpleIOIpStreamChecks {
  val backend: Backend { type Interface <: IpStreams } = NioBackend
}

class NioSimpleIOResolveSuite extends SimpleIOResolveChecks {
  val backend: Backend { type Interface <: Resolving } = NioBackend
}

// NioBackend on one worker, where every future shares one reactor.

class NioOnOneOpsSuite extends OpsContract with OnWorkers(1) {
  val backend: Backend = NioBackend
}

class NioOnOnePipeSuite extends PipeContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Pipes } = NioBackend
}

class NioOnOneSocketStreamSuite extends StreamContract with IpStreamFixture with OnWorkers(1) {
  val backend: Backend { type Interface <: IpStreams } = NioBackend
}

class NioOnOneIpStreamSuite extends IpStreamContract with OnWorkers(1) {
  val backend: Backend { type Interface <: IpStreams } = NioBackend
}

class NioOnOneSimpleIOSuite extends SimpleIOChecks with PipeFixture with OnWorkers(1) {
  val backend: Backend { type Interface <: Pipes } = NioBackend
}

// NioOnCaller: gears' own virtual threads, each op performed on its own thread.

class NioOnCallerOpsSuite extends OpsContract {
  val backend: Backend = NioOnCaller
}

class NioOnCallerThreadedTimerSuite extends ThreadedTimerContract {
  val backend: Backend = NioOnCaller
}

class NioOnCallerBlockingSuite extends BlockingContract {
  val backend: Backend { type Interface <: Blocking } = NioOnCaller
}

class NioOnCallerPipeSuite extends PipeContract {
  val backend: Backend { type Interface <: Pipes } = NioOnCaller
}

class NioOnCallerSocketStreamSuite extends StreamContract with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = NioOnCaller
}

class NioOnCallerIpStreamSuite extends IpStreamContract {
  val backend: Backend { type Interface <: IpStreams } = NioOnCaller
}

class NioOnCallerIpDatagramSuite extends IpDatagramContract {
  val backend: Backend { type Interface <: IpDatagrams } = NioOnCaller
}

class NioOnCallerUnixStreamSuite extends UnixStreamContract {
  val backend: Backend { type Interface <: UnixStreams } = NioOnCaller
}

class NioOnCallerFileSuite extends FileContract {
  val backend: Backend { type Interface <: RegularFiles } = NioOnCaller
}

class NioOnCallerResolveSuite extends ResolveContract {
  val backend: Backend { type Interface <: Resolving } = NioOnCaller

  override protected def namedScopedAddresses: List[(String, IpAddress)] = ScopedNames.linkLocal
}

class NioOnCallerSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Backend { type Interface <: Pipes } = NioOnCaller
}

class NioOnCallerSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = NioOnCaller
}

class NioOnCallerSimpleIOIpStreamSuite extends SimpleIOIpStreamChecks {
  val backend: Backend { type Interface <: IpStreams } = NioOnCaller
}

class NioOnCallerSimpleIOResolveSuite extends SimpleIOResolveChecks {
  val backend: Backend { type Interface <: Resolving } = NioOnCaller
}

// Runs that share handles, and what a run leaves when it ends: on the pool, on one worker, and on the caller.
class NioConcurrentRunsSuite extends ConcurrentRunsContract {
  val backend: Backend { type Interface <: Pipes } = NioBackend
}

class NioOnOneConcurrentRunsSuite extends ConcurrentRunsContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Pipes } = NioBackend
}

class NioOnCallerConcurrentRunsSuite extends ConcurrentRunsContract {
  val backend: Backend { type Interface <: Pipes } = NioOnCaller
}

class NioRunEndSuite extends RunEndContract {
  val backend: Backend { type Interface <: Blocking & Pipes } = NioBackend
}

class NioOnOneRunEndSuite extends RunEndContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Blocking & Pipes } = NioBackend
}

class NioOnCallerRunEndSuite extends RunEndContract {
  val backend: Backend { type Interface <: Blocking & Pipes } = NioOnCaller
}

class NioOnOneBlockingSuite extends BlockingContract with OnWorkers(1) {
  val backend: Backend { type Interface <: Blocking } = NioBackend
}
