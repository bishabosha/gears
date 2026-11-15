package gears.async.io
package node

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks

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
