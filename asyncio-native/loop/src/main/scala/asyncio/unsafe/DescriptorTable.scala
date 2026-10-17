package asyncio.unsafe

import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.AtomicReferenceArray

/** What every reactor in the process can see about a descriptor, for reactors that share descriptors, each on its own
  * thread: how many times its number has been closed, so a reactor can tell a reused number from the descriptor it
  * knew, and which reactor last registered a read filter for it, so one that registered one earlier can tell it should
  * drop its own. Descriptors are the process's, so the table is too.
  *
  * Each descriptor has one `Long`: its generation, counting closes, in the high 32 bits, and its read owner, the id of a
  * reactor, in the low 32 bits, 0 for none. Pages of descriptors are allocated as numbers are first used.
  */
object DescriptorTable {
  private inline val PageBits = 10
  private inline val PageSize = 1 << PageBits
  private inline val Pages = 4096 // so descriptors below 4 million

  private val pages = new AtomicReferenceArray[AtomicLongArray](Pages)

  private def page(fd: Int): AtomicLongArray = {
    val index = fd >>> PageBits
    require(index < Pages, s"Descriptor $fd is beyond the descriptor table")
    val existing = pages.get(index)
    if existing != null then existing
    else {
      val fresh = new AtomicLongArray(PageSize)
      if pages.compareAndSet(index, null, fresh) then fresh else pages.get(index)
    }
  }

  /** `fd`'s entry: see `generation` and `readOwner`. */
  def get(fd: Int): Long = page(fd).get(fd & (PageSize - 1))

  inline def generation(entry: Long): Int = (entry >>> 32).toInt

  inline def readOwner(entry: Long): Int = entry.toInt

  /** Records that reactor `id` holds `fd`'s read filter from now on. */
  def claimRead(fd: Int, id: Int): Unit = {
    val entries = page(fd)
    val i = fd & (PageSize - 1)
    var entry = entries.get(i)
    while !entries.compareAndSet(i, entry, (entry & 0xffffffff00000000L) | (id.toLong & 0xffffffffL)) do
      entry = entries.get(i)
  }

  /** Records that `fd` is being closed, before its number can be reused: a new generation, with no read owner. */
  def closing(fd: Int): Unit = {
    val entries = page(fd)
    val i = fd & (PageSize - 1)
    var entry = entries.get(i)
    while !entries.compareAndSet(i, entry, (generation(entry) + 1).toLong << 32) do entry = entries.get(i)
  }
}
