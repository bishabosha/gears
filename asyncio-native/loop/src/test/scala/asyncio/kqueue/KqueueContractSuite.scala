package asyncio.kqueue

import asyncio.FifoContract
import asyncio.ReadinessContract
import asyncio.ResolveContract
import asyncio.UnixDatagramContract
import asyncio.UnixStreamContract

class KqueueContractSuite
    extends ReadinessContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract {
  val backend: KqueueBackend.type = KqueueBackend
}
