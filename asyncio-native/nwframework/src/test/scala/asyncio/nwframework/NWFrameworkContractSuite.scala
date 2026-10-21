package asyncio.nwframework

import asyncio.FifoContract

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class NWFrameworkContractSuite extends FifoContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
}
