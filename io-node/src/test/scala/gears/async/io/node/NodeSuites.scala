package gears.async.io
package node

import gears.async.io.testkit.*

/** Each suite sees only the capabilities required by its contracts. */
class NodeContractSuite extends OpsContract with NodeSuite {
  val backend: Backend = NodeBackend
}
