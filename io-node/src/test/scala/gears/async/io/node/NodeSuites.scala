package gears.async.io
package node

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks
import gears.io.testkit.SimpleIOIpStreamChecks

/** Each suite sees only the capabilities required by its contracts. */
class NodeContractSuite extends OpsContract with NodeSuite {
  val backend: Backend = NodeBackend
}

class NodePipeSuite extends PipeContract with NodeSuite {
  val backend: Backend { type Interface <: Pipes } = NodeBackend
}

class NodeSimpleIOSuite extends SimpleIOChecks with PipeFixture with NodeSuite {
  val backend: Backend { type Interface <: Pipes } = NodeBackend
}

/** Runs the same stream contracts using TCP, without exposing Pipes to the suite. */
class NodeSocketStreamSuite extends StreamContract with IpStreamFixture with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeIpStreamSuite extends IpStreamContract with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeSimpleIOIpStreamSuite extends SimpleIOIpStreamChecks with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeIpDatagramSuite extends IpDatagramContract with NodeSuite {
  val backend: Backend { type Interface <: IpDatagrams } = NodeBackend
}

class NodeUnixStreamSuite extends UnixStreamContract with NodeSuite {
  val backend: Backend { type Interface <: UnixStreams } = NodeBackend
}
