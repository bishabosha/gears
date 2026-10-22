package asyncio.nio

import asyncio.FileContract
import asyncio.ReadinessContract
import asyncio.ResolveContract
import asyncio.UnixStreamContract

class NioContractSuite extends ReadinessContract with UnixStreamContract with FileContract with ResolveContract {
  val backend: NioBackend.type = NioBackend
}
