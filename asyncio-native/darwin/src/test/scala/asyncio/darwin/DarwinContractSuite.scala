package asyncio.darwin

import asyncio.FifoContract

/** The reactor contract over Network.framework and Dispatch: no readiness, and no Unix domain sockets. */
class DarwinContractSuite extends FifoContract {
  val backend: DarwinBackend.type = DarwinBackend
}
