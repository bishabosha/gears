package asyncio.unsafe

/** Tells code that tracks descriptors, such as a reactor's kqueue registrations, when a descriptor is about to be
  * closed through `PosixSockets.close`, before its number can be reused. Listeners belong to a thread and only hear of
  * closes on that thread, as a reactor only hears of the descriptors closed on the thread that owns it.
  */
object DescriptorHooks {
  private val listeners = new ThreadLocal[List[Int => Unit]] {
    override def initialValue(): List[Int => Unit] = Nil
  }

  /** Calls `listener` with each descriptor closed on this thread from now on. */
  def onClose(listener: Int => Unit): Unit = listeners.set(listener :: listeners.get())

  def removeOnClose(listener: Int => Unit): Unit = listeners.set(listeners.get().filterNot(_ eq listener))

  /** Calls each listener with `fd`. `Int => Unit` is specialised, so this calls `apply$mcVI$sp` with an unboxed `fd`;
    * the loop, unlike `foreach`, allocates no closure per close.
    */
  private[unsafe] def closing(fd: Int): Unit = {
    var remaining = listeners.get()
    while remaining.nonEmpty do {
      remaining.head(fd)
      remaining = remaining.tail
    }
  }
}
