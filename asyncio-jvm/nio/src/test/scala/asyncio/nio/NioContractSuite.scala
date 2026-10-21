package asyncio.nio

import asyncio.LifecycleContract
import asyncio.PooledReadContract
import asyncio.ReadinessContract
import asyncio.UnixStreamContract

class NioContractSuite extends ReadinessContract with UnixStreamContract with PooledReadContract with LifecycleContract {
  val backend: NioBackend.type = NioBackend
}
