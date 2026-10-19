package asyncio.nio

import asyncio.UnixStreamContract

class NioContractSuite extends UnixStreamContract {
  val backend: NioBackend.type = NioBackend
}
