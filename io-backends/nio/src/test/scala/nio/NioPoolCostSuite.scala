package gears.async.io.internal

import gears.async.io.nio.NioBackend

class NioPoolCostSuite extends PoolCostContract {
  val backend: NioBackend.type = NioBackend
}
