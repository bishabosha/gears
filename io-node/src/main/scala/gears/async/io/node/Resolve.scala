package gears.async.io
package node

import java.io.IOException
import scala.scalajs.js

/** Resolves a host name: an address written out is read here, with its scope, by number or by an interface's name; any
  * other name is looked up by the system's resolver, with `dns.lookup`, on a thread of Node's. Node cannot stop a
  * lookup, so one cancelled or out of time is left to run on, its result dropped.
  */
private[node] final class ResolveOp(host: String, into: Slot[List[IpAddress]]) extends NodeOp {
  override protected def slots: List[Slot[?]] = List(into)

  protected def start(run: Run): Unit = {
    val bare = if host.startsWith("[") && host.endsWith("]") then host.substring(1, host.length - 1) else host
    val written = Addresses.parse(bare)
    if written != null then {
      into.set(List(Addresses.unmapped(written)))
      run.succeed()
    } else {
      val done: js.Function2[js.Dynamic, js.Dynamic, Unit] = (failure, found) =>
        if !run.done then
          if Errors.failed(failure) then
            run.fail(new IOException(s"Failed to resolve $host: ${Errors.io(failure).getMessage}"))
          else
            try {
              val addresses = found
                .asInstanceOf[js.Array[js.Dynamic]]
                .toList
                .flatMap(entry => Option(Addresses.parse(entry.address.asInstanceOf[String])).map(Addresses.unmapped))
                .distinct
              if addresses.isEmpty then
                throw new IOException(s"Failed to resolve $host: it has no IPv4 or IPv6 address")
              into.set(addresses)
              run.succeed()
            } catch case failure: Throwable => run.fail(failure)
      Errors.guard(Dns.lookup(host, js.Dynamic.literal(all = true), done))
    }
  }
}
