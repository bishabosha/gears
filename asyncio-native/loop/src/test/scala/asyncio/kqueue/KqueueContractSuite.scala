package asyncio.kqueue

import asyncio.FifoContract
import asyncio.ReadinessContract
import asyncio.UnixDatagramContract
import asyncio.UnixStreamContract

class KqueueContractSuite
    extends ReadinessContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract {
  val backend: KqueueBackend.type = KqueueBackend
}
