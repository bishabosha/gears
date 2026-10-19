package asyncio.kqueue

import asyncio.FifoContract
import asyncio.UnixDatagramContract
import asyncio.UnixStreamContract

class KqueueContractSuite extends UnixStreamContract with UnixDatagramContract with FifoContract {
  val backend: KqueueBackend.type = KqueueBackend
}
