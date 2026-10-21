package asyncio.kqueue

import asyncio.FifoContract
import asyncio.LifecycleContract
import asyncio.PooledReadContract
import asyncio.ReadinessContract
import asyncio.UnixDatagramContract
import asyncio.UnixStreamContract

class KqueueContractSuite
    extends ReadinessContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with PooledReadContract
    with LifecycleContract {
  val backend: KqueueBackend.type = KqueueBackend
}
