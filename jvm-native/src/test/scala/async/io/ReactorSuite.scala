package gears.async.io

import munit.FunSuite

class ReactorSuite extends FunSuite {

  /** A reactor that only records whether it was closed. */
  private final class ClosingReactor extends Reactor {
    type Op = AnyRef
    type Handle = AnyRef
    var closed = false
    def ops: Ops[Op, Handle] = throw new UnsupportedOperationException
    def submit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Unit = ()
    def trySubmit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Boolean = false
    def cancel(op: Op): Boolean = false
    def attachment(op: Op): AnyRef | Null = null
    def attach(op: Op, value: AnyRef | Null): Unit = ()
    def runOnce(block: Boolean): Unit = ()
    def wakeup(): Unit = ()
    def run(): Unit = ()
    def stop(): Unit = ()
    def close(): Unit = closed = true
  }

  test("scoped closes the reactor its factory opened, even when the body fails") {
    val reactor = new ClosingReactor
    given Reactor.Factory[Reactor] = () => reactor
    intercept[IllegalStateException](Reactor.scoped(_ => throw new IllegalStateException("boom")))
    assert(reactor.closed)
  }

  test("a completion ignores cancellation by default") {
    val completion: Completion[AnyRef] = _ => ()
    completion.onCancel(new Object)
  }

  test("a completion's failures propagate by default") {
    val completion: Completion[AnyRef] = _ => ()
    val failure = new RuntimeException("boom")
    val thrown = intercept[RuntimeException](completion.onFailure(new Object, failure))
    assert(thrown eq failure)
  }

  test("an op built by one reactor cannot be submitted to another") {
    val errors = compileErrors(
      "def cross(a: Reactor, b: Reactor): Unit = b.submit(a.ops.timer(1), _ => ())"
    )
    assert(errors.contains("a.Op"), errors)
  }

  test("an op built by a backend can be submitted to any reactor it opens, and not to another backend's") {
    assertEquals(
      compileErrors(
        "def move(b: ReactorBackend): Unit = { val t = b.ops.timer(1); b.open().submit(t, _ => ()); b.open().submit(t, _ => ()) }"
      ),
      ""
    )
    val errors = compileErrors("def cross(a: ReactorBackend, b: ReactorBackend): Unit = b.open().submit(a.ops.timer(1), _ => ())")
    assert(errors.contains("a.Op"), errors)
  }

  test("a backend is a factory for its reactors") {
    val reactor = new ClosingReactor
    given ReactorBackend = new ReactorBackend {
      type Op = AnyRef
      type Handle = AnyRef
      def ops: Ops[Op, Handle] = throw new UnsupportedOperationException
        def attachment(op: Op): AnyRef | Null = null
      def attach(op: Op, value: AnyRef | Null): Unit = ()
      def open(): Reactor.Of[Op, Handle] = reactor
      def pool(bufferSize: Int, buffers: Int): BufferPool = throw new UnsupportedOperationException
    }
    Reactor.scoped(_ => ())
    assert(reactor.closed)
  }

  test("only ops marked Repeatable promise they can be resubmitted") {
    assertEquals(
      compileErrors("def repeat(b: SocketBackend)(h: b.Handle): Repeatable = b.ops.accept(h, Slot[b.Handle]())"),
      ""
    )
    assertEquals(compileErrors("def repeat(a: Reactor): Repeatable = a.ops.timer(1)"), "")
    assert(compileErrors("def repeat(b: BlockingBackend): Repeatable = b.ops.blocking(() => ())").nonEmpty)
  }

  test("sockets, files, pipes and name resolution are capabilities a backend has, not every backend") {
    assert(compileErrors("def c(b: Backend) = b.ops.connect(IpAddress.IPv4(\"h\", 1), Slot[b.Handle]())").nonEmpty)
    assert(compileErrors("def l(b: Backend) = b.ops.listen(IpAddress.IPv4(\"h\", 1), Slot[b.Handle]())").nonEmpty)
    assert(compileErrors("def p(b: Backend) = b.ops.pipe(Slot[b.Handle](), Slot[b.Handle]())").nonEmpty)
    assert(compileErrors("def u(b: SocketBackend) = b.ops.connect(UnixAddress(\"p\"), Slot[b.Handle]())").nonEmpty)
    assert(compileErrors("def r(b: Backend) = b.ops.resolve(\"h\", Slot())").nonEmpty)
    assert(compileErrors("def f(b: Backend) = b.ops.open(\"p\", false, Slot[b.Handle]())").nonEmpty)
    assertEquals(
      compileErrors("def c(b: SocketBackend) = b.ops.connect(IpAddress.IPv4(\"h\", 1), Slot[b.Handle]())"),
      ""
    )
    assertEquals(
      compileErrors("def l(b: SocketBackend) = b.ops.listen(IpAddress.IPv4(\"h\", 1), Slot[b.Handle]())"),
      ""
    )
    assertEquals(compileErrors("def u(b: UnixStreamBackend) = b.ops.connect(UnixAddress(\"p\"), Slot[b.Handle]())"), "")
    assertEquals(compileErrors("def u(b: UnixStreamBackend)(h: b.Handle) = b.ops.accept(h, Slot[b.Handle]())"), "")
    assertEquals(compileErrors("def p(b: PipeBackend) = b.ops.pipe(Slot[b.Handle](), Slot[b.Handle]())"), "")
    assertEquals(compileErrors("def r(b: ResolveBackend) = b.ops.resolve(\"h\", Slot())"), "")
    assertEquals(compileErrors("def f(b: FileBackend) = b.ops.open(\"p\", false, Slot[b.Handle]())"), "")
    assertEquals(compileErrors("def s(b: FileBackend)(h: b.Handle): Repeatable = b.ops.read(h, null, 0L)"), "")
    assertEquals(compileErrors("def t(b: Backend)(h: b.Handle): Repeatable = b.ops.read(h, null)"), "")
  }

  test("a reactor is a submitter with a loop; a submitter keeps its ops' attachments, a backend does not") {
    assertEquals(compileErrors("def s(r: Reactor): Submitter = r"), "")
    assertEquals(compileErrors("def sub(s: Submitter): Unit = s.submit(s.ops.timer(1), _ => ())"), "")
    assert(compileErrors("def turn(s: Submitter): Unit = s.runOnce(false)").nonEmpty)
    assertEquals(compileErrors("def a(s: Submitter)(op: s.Op) = s.attachment(op)"), "")
    assert(compileErrors("def a(b: ReactorBackend)(op: b.Op) = b.attachment(op)").nonEmpty)
    assert(compileErrors("def o(b: Backend) = b.open()").nonEmpty)
  }

  test("readiness, waitless use and blocking ops are capabilities a backend has, not every backend") {
    assert(compileErrors("def ready(b: Backend)(h: b.Handle) = b.ops.ready(h, Interest.Read)").nonEmpty)
    assert(compileErrors("def now(b: Backend)(h: b.Handle) = b.ops.readNow(h, null)").nonEmpty)
    assert(compileErrors("def block(b: Backend) = b.ops.blocking(() => ())").nonEmpty)
    assertEquals(
      compileErrors("def ready(b: ReadinessBackend)(h: b.Handle): Repeatable = b.ops.ready(h, Interest.Read)"),
      ""
    )
    assertEquals(compileErrors("def now(b: ReadinessBackend)(h: b.Handle) = b.ops.readNow(h, null)"), "")
    assertEquals(compileErrors("def block(b: BlockingBackend) = b.ops.blocking(() => ())"), "")
  }

  test("a slot is empty until an implementation writes to it") {
    val slot = Slot[String]()
    intercept[IllegalStateException](slot.clear())
    slot.set("written")
    assertEquals(slot.clear(), "written")
    intercept[IllegalStateException](slot.clear()) // clearing takes the value out
  }

  test("a slot must be cleared before it is written again") {
    val slot = Slot[String]()
    slot.set("first")
    intercept[IllegalStateException](slot.set("second"))
    assertEquals(slot.clear(), "first")
    slot.set("second")
    assertEquals(slot.clear(), "second")
  }

  test("completions carry no result") {
    assert(compileErrors("val c: Completion[AnyRef] = (_, _) => ()").nonEmpty)
  }

  test("an op built by a reactor can be submitted to it") {
    val errors = compileErrors(
      "def same(a: Reactor): Unit = a.submit(a.ops.timer(1), _ => ())"
    )
    assertEquals(errors, "")
  }
}
