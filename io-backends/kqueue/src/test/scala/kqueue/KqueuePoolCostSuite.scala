package gears.async.io.internal

import gears.async.io.kqueue.KqueueBackend

class KqueuePoolCostSuite extends PoolCostContract {
  val backend: KqueueBackend.type = KqueueBackend
}
