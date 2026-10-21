package asyncio.nwframework

import asyncio.FifoContract
import asyncio.LifecycleContract
import asyncio.PooledReadContract

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class NWFrameworkContractSuite extends FifoContract with PooledReadContract with LifecycleContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
}
