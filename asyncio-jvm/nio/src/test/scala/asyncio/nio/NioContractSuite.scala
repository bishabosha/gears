package asyncio.nio

import java.nio.ByteBuffer

import asyncio.UnixStreamContract

class NioContractSuite extends UnixStreamContract {
  val backend: NioBackend.type = NioBackend

  def allocate(capacity: Int): ByteBuffer = ByteBuffer.allocateDirect(capacity)
}
