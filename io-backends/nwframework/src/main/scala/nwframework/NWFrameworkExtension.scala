package gears.async.io
package nwframework

import gears.async.io.kqueue.KqueueReactor
import gears.async.io.kqueue.ReactorExtension

import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** A kqueue reactor's Network.framework side (see `ReactorExtension`): its list on the C side of the handles with
  * events for it, which Network.framework's callbacks fill on Dispatch's threads, waking the reactor with a user event
  * on its kqueue; and, each turn, the drain of that list, each handle applying its events (`NWFrameworkHandle.drain`)
  * and notifying the ops they finish (`KqueueReactor.notified`). A reactor gets one with its first Network.framework
  * op, so a reactor that never runs one pays nothing for it.
  */
private[nwframework] final class NWFrameworkExtension private (reactor: KqueueReactor, maxDrained: Int)
    extends ReactorExtension {

  /** The C side's list of handles with events for this reactor. */
  val ptr: NWFrameworkIo.Reactor = NWFrameworkIo.nwf_reactor_create(reactor.kq)

  // The handles a turn drains, and the event a handle copies out, allocated once and freed by `close`.
  private val drained: Ptr[NWFrameworkIo.Handle] =
    stdlib.malloc((sizeOf[Ptr[Byte]] * maxDrained).toCSize).asInstanceOf[Ptr[NWFrameworkIo.Handle]]
  val event: Ptr[NWFrameworkIo.Event] =
    stdlib.malloc(sizeOf[NWFrameworkIo.Event].toCSize).asInstanceOf[Ptr[NWFrameworkIo.Event]]

  def deliver(): Boolean =
    NWFrameworkIo.nwf_reactor_ready(ptr) != 0 && {
      val n = NWFrameworkIo.nwf_reactor_drain(ptr, drained, maxDrained) // the rest, if more, on the next turn
      var i = 0
      while i < n do {
        val handle = NWFrameworkHandles.at(drained(i))
        if handle != null then handle.drain(event)
        i += 1
      }
      n > 0
    }

  def close(): Unit = {
    NWFrameworkIo.nwf_reactor_destroy(ptr)
    stdlib.free(drained.asInstanceOf[Ptr[Byte]])
    stdlib.free(event.asInstanceOf[Ptr[Byte]])
  }
}

private[nwframework] object NWFrameworkExtension {

  /** `reactor`'s extension, made if it has none. On its loop thread, as an op is attempted there. */
  def of(reactor: KqueueReactor): NWFrameworkExtension =
    reactor.extend(new NWFrameworkExtension(reactor, 256)).asInstanceOf[NWFrameworkExtension]

  /** `reactor`'s extension, which it has: from any thread, for a reactor an op waits on. */
  def existing(reactor: KqueueReactor): NWFrameworkExtension =
    reactor.extension.asInstanceOf[NWFrameworkExtension]
}
