package gears.async.io
package kqueue

import gears.async.io.testkit.OpsContract
import gears.async.io.testkit.ThreadedTimerContract

class KqueueContractSuite extends OpsContract {
  val backend: Backend = KqueueBackend
}

class KqueueThreadedTimerSuite extends ThreadedTimerContract {
  val backend: Backend = KqueueBackend
}
