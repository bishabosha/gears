package asyncio.nio

import asyncio.FileContract
import asyncio.ResolveContract
import asyncio.UnixStreamContract

class NioContractSuite extends UnixStreamContract with FileContract with ResolveContract {
  val backend: NioBackend.type = NioBackend
}
