package gears.async.io
package node

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks
import gears.io.testkit.SimpleIOIpStreamChecks
import gears.io.testkit.SimpleIOResolveChecks

import scala.scalajs.js

/** Each suite sees only the capabilities required by its contracts. */
class NodeContractSuite extends OpsContract with NodeSuite {
  val backend: Backend = NodeBackend
}

class NodePipeSuite extends PipeContract with NodeSuite {
  val backend: Backend { type Interface <: Pipes } = NodeBackend
}

class NodeSimpleIOSuite extends SimpleIOChecks with PipeFixture with NodeSuite {
  val backend: Backend { type Interface <: Pipes } = NodeBackend
}

/** Runs the same stream contracts using TCP, without exposing Pipes to the suite. */
class NodeSocketStreamSuite extends StreamContract with IpStreamFixture with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeIpStreamSuite extends IpStreamContract with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeSimpleIOIpStreamSuite extends SimpleIOIpStreamChecks with NodeSuite {
  val backend: Backend { type Interface <: IpStreams } = NodeBackend
}

class NodeIpDatagramSuite extends IpDatagramContract with NodeSuite {
  val backend: Backend { type Interface <: IpDatagrams } = NodeBackend
}

class NodeUnixStreamSuite extends UnixStreamContract with NodeSuite {
  val backend: Backend { type Interface <: UnixStreams } = NodeBackend
}

class NodeFileSuite extends FileContract with NodeSuite {
  val backend: Backend { type Interface <: RegularFiles } = NodeBackend
}

class NodeResolveSuite extends ResolveContract with NodeSuite {
  val backend: Backend { type Interface <: Resolving } = NodeBackend

  /** A link-local address of an interface of the host's, written with the interface's name as its zone. */
  override protected def namedScopedAddresses: List[(String, IpAddress)] =
    (for
      (name, addresses) <- Os.networkInterfaces().iterator
      address <- addresses.iterator
      if js.typeOf(address.scopeid) == "number" && address.scopeid.asInstanceOf[Int] > 0
      unscoped <- Option(Addresses.parse(address.address.asInstanceOf[String]))
      if unscoped.protocol == Protocol.IPv6
    yield (name, IpAddress(Protocol.IPv6, unscoped.bytes, address.scopeid.asInstanceOf[Int])))
      .nextOption()
      .toList
      .flatMap { (name, expected) =>
        val compressed = IpAddress(Protocol.IPv6, expected.bytes).toString
        val expanded =
          expected.bytes.grouped(2).map(pair => f"${pair(0) & 0xff}%02x${pair(1) & 0xff}%02x").mkString(":")
        List(s"$compressed%$name", s"$expanded%$name", s"[$compressed%$name]").map(_ -> expected)
      }
}

class NodeSimpleIOResolveSuite extends SimpleIOResolveChecks with NodeSuite {
  val backend: Backend { type Interface <: Resolving } = NodeBackend
}

class NodeFifoSuite extends FifoContract with NodeSuite {
  val backend: Backend { type Interface <: Fifos } = NodeBackend

  // What the interface says of a FIFO's end, which this backend reads without a stream of Node's to keep to it.
  test("a FIFO is at its end before a writer has opened it, and after each writer, going on to the next one's bytes") {
    withTempDir { dir =>
      running { io ?=>
        val fifo = dir.resolve("fifo")
        mkfifo(fifo)
        val reader = openFifoForReading(io, fifo.toString)
        try {
          val buf = allocate(16)
          val read = io.ops.read(reader, buf)
          io.perform(read)
          assertEquals(buf.position(), 0, "before any writer")
          for word <- List("first", "second") do {
            val writer = openFifoForWriting(io, fifo.toString)
            writeAll(io, writer, bytes(word))
            close(io, writer)
            buf.clear()
            assertEquals(readToEnd(io, read, buf), word)
          }
        } finally close(io, reader)
      }
    }
  }
}

class NodeFifoFileSuite extends FifoFileContract with NodeSuite {
  val backend: Backend { type Interface <: Fifos & RegularFiles } = NodeBackend
}
