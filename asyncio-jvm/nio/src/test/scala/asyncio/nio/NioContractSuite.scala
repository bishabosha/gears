package asyncio.nio

import asyncio.ReadinessContract
import asyncio.UnixStreamContract

class NioContractSuite extends ReadinessContract with UnixStreamContract {
  val backend: NioBackend.type = NioBackend
}
