package gears.async.io
package testkit

import java.io.IOException
import java.nio.charset.StandardCharsets

/** What a backend with IP sockets must do (see `ReactorContract`). */
trait SocketContract extends ReactorContract {
  val backend: ReactorBackend & BlockingBackend & PipeBackend & SocketBackend

  /** A listener on `address`, listening: `handles.listener` and the `listening` op, whose failure is thrown, with the
    * handle closed.
    */
  protected def listen(r: R, address: IpAddress, reuseAddr: Boolean = true)(using munit.Location): Handle = {
    val handle = backend.handles.listener(address, reuseAddr)
    val failure = run(r, backend.ops.listening(handle))
    if failure != null then {
      close(r, handle)
      throw failure
    }
    handle
  }

  /** Opens something on a free port: no interface tells which port was bound, so it tries a few. */
  protected def onFreePort(open: Int => Handle): (Handle, Int) = {
    val random = new java.util.Random()
    var attempts = 0
    var result: (Handle, Int) | Null = null
    while result == null do {
      val port = 20000 + random.nextInt(30000)
      try result = (open(port), port)
      catch {
        case e: IOException =>
          attempts += 1
          if attempts > 10 then throw e
      }
    }
    result
  }

  test("an IPv4 stream socket accepts, connects and echoes") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Handle.Slot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = backend.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try {
          val connected = Outcome()
          r.submit(backend.ops.connect(client), connected)
          turnUntil(r)(acceptDone.finished && connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
          val server = accepted.clear()
          val sent = Outcome()
          r.submit(backend.ops.write(client, bytes("tcp ping")), sent)
          val buf = allocate(16)
          val got = Outcome()
          r.submit(backend.ops.read(server, buf), got)
          turnUntil(r)(sent.finished && got.finished)
          assertEquals(text(buf), "tcp ping")
          buf.flip()
          val echoed = Outcome()
          r.submit(backend.ops.write(server, buf), echoed)
          val reply = allocate(16)
          val replied = Outcome()
          r.submit(backend.ops.read(client, reply), replied)
          turnUntil(r)(echoed.finished && replied.finished)
          assertEquals(text(reply), "tcp ping")
          close(r, server)
          assertEquals(readToEnd(r, backend.ops.read(client, reply), reply), "tcp ping")
        } finally close(r, client)
      } finally close(r, listener)
    }
  }

  test("an IPv4 datagram goes to a bound socket, which learns its sender") {
    withReactor { r =>
      val (server, port) = onFreePort(port => backend.handles.datagram(IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val client = backend.handles.datagram(null, IpAddress.IPv4("127.0.0.1", port))
        try {
          val buf = allocate(64)
          val from = Slot[IpAddress]()
          val received = Outcome()
          r.submit(backend.ops.receive(server, buf, from), received)
          val sent = Outcome()
          r.submit(backend.ops.send(client, bytes("datagram")), sent)
          turnUntil(r)(received.finished && sent.finished)
          assertEquals(received.completed, 1, String.valueOf(received.failure))
          val copy = new Array[Byte](buf.remaining())
          buf.get(copy)
          assertEquals(new String(copy, StandardCharsets.UTF_8), "datagram")
          from.clear() match {
            case IpAddress.IPv4(host, senderPort) =>
              assertEquals(host, "127.0.0.1")
              assert(senderPort > 0)
            case other => fail(s"unexpected sender $other")
          }
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  test("listening on a datagram socket completes once it is bound, which may be at once") {
    withReactor { r =>
      val (server, port) = onFreePort(port => backend.handles.datagram(IpAddress.IPv4("127.0.0.1", port), null))
      try {
        val bound = Outcome()
        r.submit(backend.ops.listening(server), bound)
        turnUntil(r)(bound.finished)
        assertEquals(bound.completed, 1, String.valueOf(bound.failure))
        val client = backend.handles.datagram(null, IpAddress.IPv4("127.0.0.1", port))
        try {
          val connected = Outcome()
          r.submit(backend.ops.listening(client), connected)
          turnUntil(r)(connected.finished)
          assertEquals(connected.completed, 1, String.valueOf(connected.failure))
        } finally close(r, client)
      } finally close(r, server)
    }
  }

  /** The bytes a pooled read received, as text, consuming them. */
  private def textOf(received: Received): String = {
    val buf = received.buffer
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    new String(out, StandardCharsets.UTF_8)
  }

  test("a pooled read and a read into the caller's buffer take turns on one socket") {
    val pool = backend.pool(bufferSize = 64, buffers = 2)
    try
      withReactor { r =>
        val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
        try {
          val accepted = Handle.Slot()
          r.submit(backend.ops.accept(listener, accepted), Outcome())
          val client = backend.handles.connect(IpAddress.IPv4("127.0.0.1", port))
          try {
            val connected = Outcome()
            r.submit(backend.ops.connect(client), connected)
            turnUntil(r)(connected.finished && { accepted.toString; true })
            turnUntil(r)(connected.finished)
            val server = {
              var h = Handle.Opt.empty
              turnUntil(r) {
                try { h = Handle.Opt(accepted.clear()); true }
                catch case _: IllegalStateException => false
              }
              h.get
            }
            try {
              r.submit(backend.ops.write(client, bytes("first")), Outcome())
              val into = Slot[Received]()
              val pooled = Outcome()
              r.submit(backend.ops.read(server, pool, into), pooled)
              turnUntil(r)(pooled.finished)
              assertEquals(pooled.completed, 1, String.valueOf(pooled.failure))
              val received = into.clear()
              assertEquals(textOf(received), "first")
              received.release()
              r.submit(backend.ops.write(client, bytes("second")), Outcome())
              val buf = allocate(64)
              val plain = Outcome()
              r.submit(backend.ops.read(server, buf), plain)
              turnUntil(r)(plain.finished)
              assertEquals(plain.completed, 1, String.valueOf(plain.failure))
              assertEquals(text(buf), "second")
            } finally close(r, server)
          } finally close(r, client)
        } finally close(r, listener)
      }
    finally pool.close()
  }

  // ---- Listening and closing as ops

  test("a listener listens once its listening op completes, and accepts") {
    withReactor { r =>
      val (listener, port) = onFreePort { port =>
        val h = backend.handles.listener(IpAddress.IPv4("127.0.0.1", port))
        val listening = Outcome()
        r.submit(backend.ops.listening(h), listening)
        turnUntil(r)(listening.finished)
        if listening.failure != null then {
          close(r, h)
          throw new IOException(String.valueOf(listening.failure))
        }
        h
      }
      try {
        val accepted = Handle.Slot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = backend.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        try {
          val connected = Outcome()
          r.submit(backend.ops.connect(client), connected)
          turnUntil(r)(acceptDone.finished && connected.finished)
          assertEquals(acceptDone.completed, 1, String.valueOf(acceptDone.failure))
          close(r, accepted.clear())
        } finally close(r, client)
      } finally close(r, listener)
    }
  }

  test("listening on an address in use fails the listening op, not the handle's creation") {
    withReactor { r =>
      val (taken, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val h = backend.handles.listener(IpAddress.IPv4("127.0.0.1", port), reuseAddr = false)
        try {
          val listening = Outcome()
          r.submit(backend.ops.listening(h), listening)
          turnUntil(r)(listening.finished)
          assert(listening.failure.isInstanceOf[IOException], String.valueOf(listening.failure))
        } finally close(r, h)
      } finally close(r, taken)
    }
  }

  test("a closed listener's port can be listened on again once its close op completes") {
    withReactor { r =>
      val (first, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      val closed = Outcome()
      r.submit(backend.ops.close(first), closed)
      turnUntil(r)(closed.finished)
      assertEquals(closed.completed, 1, String.valueOf(closed.failure))
      val second = backend.handles.listener(IpAddress.IPv4("127.0.0.1", port), reuseAddr = false)
      try {
        val listening = Outcome()
        r.submit(backend.ops.listening(second), listening)
        turnUntil(r)(listening.finished)
        assertEquals(listening.completed, 1, String.valueOf(listening.failure))
      } finally close(r, second)
    }
  }

  test("a close op on a connection ends its peer's stream") {
    withReactor { r =>
      val (listener, port) = onFreePort(port => listen(r, IpAddress.IPv4("127.0.0.1", port)))
      try {
        val accepted = Handle.Slot()
        val acceptDone = Outcome()
        r.submit(backend.ops.accept(listener, accepted), acceptDone)
        val client = backend.handles.connect(IpAddress.IPv4("127.0.0.1", port))
        val connected = Outcome()
        r.submit(backend.ops.connect(client), connected)
        turnUntil(r)(acceptDone.finished && connected.finished)
        val server = accepted.clear()
        try {
          val closed = Outcome()
          r.submit(backend.ops.close(client), closed)
          turnUntil(r)(closed.finished)
          assertEquals(closed.completed, 1, String.valueOf(closed.failure))
          val buf = allocate(16)
          assertEquals(readToEnd(r, backend.ops.read(server, buf), buf), "")
        } finally close(r, server)
      } finally close(r, listener)
    }
  }
}
