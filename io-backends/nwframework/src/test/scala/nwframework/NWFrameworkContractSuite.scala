package asyncio.nwframework

import asyncio.FifoContract
import asyncio.ResolveContract
import asyncio.SocketContract

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class NWFrameworkContractSuite extends FifoContract with SocketContract with ResolveContract {
  val backend: NWFrameworkBackend.type = NWFrameworkBackend
}
