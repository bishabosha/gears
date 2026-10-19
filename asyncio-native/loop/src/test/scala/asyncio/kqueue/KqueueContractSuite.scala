package asyncio.kqueue

import java.nio.ByteBuffer
import scala.scalanative.unsafe.Zone

import asyncio.FifoContract
import asyncio.UnixDatagramContract
import asyncio.UnixStreamContract
import asyncio.unsafe.NativeBuffer

class KqueueContractSuite extends UnixStreamContract with UnixDatagramContract with FifoContract {
  val backend: KqueueBackend.type = KqueueBackend

  private val zone = Zone.open()

  def allocate(capacity: Int): ByteBuffer = NativeBuffer.allocate(capacity)(using zone)

  override def afterAll(): Unit = zone.close()
}
