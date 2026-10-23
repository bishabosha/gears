package gears.async.io
package kqueue

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ReadinessContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.UnixDatagramContract
import gears.async.io.testkit.UnixStreamContract

class KqueueContractSuite
    extends ReadinessContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract {
  val backend: KqueueBackend.type = KqueueBackend
}
