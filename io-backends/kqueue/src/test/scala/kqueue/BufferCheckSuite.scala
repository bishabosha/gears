package gears.async.io
package kqueue

import gears.async.io.internal.NativeBuffer

import java.nio.ByteBuffer
import scala.scalanative.unsafe.Zone

/** kqueue's ops need buffers the kernel can address: one on the heap, as `ByteBuffer.allocate` and, on Scala Native,
  * `allocateDirect` make, or a read-only one, is refused as the op is built, not in its system call.
  */
class BufferCheckSuite extends munit.FunSuite {

  test("a buffer the kernel cannot address is refused as an op is built, with IllegalArgumentException") {
    Zone.acquire { implicit z =>
      val (in, out) = KqueueHandles.pipe()
      try {
        val usable = NativeBuffer.allocate(8)
        for unusable <- List(ByteBuffer.allocate(8), ByteBuffer.allocateDirect(8), usable.asReadOnlyBuffer()) do {
          intercept[IllegalArgumentException](KqueueOps.read(in, unusable))
          intercept[IllegalArgumentException](KqueueOps.write(out, unusable))
          intercept[IllegalArgumentException](KqueueOps.read(in, Array(usable, unusable)))
          intercept[IllegalArgumentException](KqueueOps.write(out, Array(usable, unusable)))
        }
        KqueueOps.read(in, usable)
        KqueueOps.write(out, usable)
      } finally {
        KqueueHandles.close(in)
        KqueueHandles.close(out)
      }
    }
  }
}
