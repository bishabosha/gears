# Handles that outlive their I/O object: a design note

A `Handle` names an open socket, pipe, FIFO or file by a number, and the number is reused once its handle is closed.
So a consumer still holding a closed handle can, without any error, reach whatever I/O object was opened under the
number since. This note records the problem, what protects against it today and what does not, and two proposals:
encoding a generation in the handle itself, checked as the program runs, or tying a handle to a gears scope that capture
checking enforces when compiling. Written on 2026-10-24; the capture checking findings come from small programs
compiled with Scala 3.9.0, `language.experimental.captureChecking` and `separationChecking`. Nothing in the
interface has changed for it yet.

## The problem

A handle is an opaque `Int` (`gears.async.io.Handle`), whose meaning belongs to the backend:

- **kqueue** uses the POSIX descriptor itself, and the kernel gives each new descriptor the lowest number free.
- **NIO** and **Network.framework** use an index into a process-wide table (`gears.async.io.internal.HandleTable`),
  which reuses the most recently freed number first.

Both reuse a number as soon as it is free. Take two futures sharing a handle:

1. Future A and future B both hold handle 7, a connection.
2. A closes 7, then opens a new socket, which is given 7.
3. B runs, and reads or writes handle 7: the new socket, not the connection it was given.

Nothing fails. B's bytes go to, or come from, another device, and A's new socket sees traffic it never asked for.
The same holds for an op built on a handle (ops are records a program keeps and resubmits, `Repeatable`): one built on
7 before the close, and submitted again after it, acts on the new 7.

## What protects against it today, and what does not

Each backend makes the ops **already pending** when a handle is closed fail, and never complete on what reuses the
number:

- **kqueue** keeps a generation per descriptor (`gears.async.io.internal.DescriptorTable`), counting closes. A close
  ends the descriptor's generation before the number can be reused and tells every reactor, which fails the commands
  pending on it and forgets its registrations; an event that arrives for the old generation is recognised as stale.
- **NIO** looks a handle's entry up when a command is submitted, and a closed entry fails what is pending on it.
- **Network.framework** resolves a command's handle when it is submitted and keeps that handle object for its
  attempts, since "the number may be closed and reused meanwhile".

`Ops.close` states the rule this enforces: the ops pending on a handle fail with an `IOException` before the close
completes, and only then is the number free. What none of it covers is an op **submitted after** the number was
reused: it is looked up afresh and finds the new object. The interface leaves that to the program, as POSIX does: a
handle must not be used once it is closed, or while it is being closed (`KqueueHandles.close`). That is documented,
not enforced.

## Proposal 1: a generation in the handle, checked as the program runs

Make the handle a pair: the number and a count of how many times that number has been closed. Every op checks the
count against the table's, and a stale handle fails with "Not an open handle" instead of reaching what reuses the
number. It is the generational index of slot maps and arenas; Tokio packs a generation into its registration tokens
for the same reason, and `DescriptorTable` already keeps one per descriptor for the pending case.

- **Representation.** `Handle` becomes an opaque `Long`: the number in the low 32 bits, the generation in the high
  32. Still unboxed. Packing both into an `Int` would leave too few bits for one of them: a generation of 8 bits wraps
  after 256 closes of one number, which a busy server reaches in seconds.
- **Per backend.** kqueue compares with `DescriptorTable`'s generation, which it reads already. `HandleTable` keeps a
  generation per entry, incremented by `remove`. Network.framework and NIO check it where they look the entry up.
- **What changes around it.** `Handle.Opt` and `Handle.Slot` hold a `Long`; `acceptAll`'s `Array[Handle]` becomes an
  array of `Long`s; `KqueueHandles.handle(fd)`, for kqueue's own demos, needs the descriptor's current generation.
- **Cost.** One comparison per op, against a value the backends mostly load anyway, and 4 more bytes per handle.
- **What it gives.** Detection, not prevention: B's read in the example fails cleanly instead of reading the wrong
  device, and it fails on every backend the same way. It covers the low level (`Ops`, `perform`) as well as anything
  built on it, and code that switches off the compiler's checks.
- **What it does not.** It finds the mistake when the program runs, possibly in production, and only if the stale
  handle is used at all. It does not stop a future from holding a handle past its close.

## Proposal 2: handles tied to a gears scope, enforced by capture checking

Make a handle impossible to use once its scope has closed it: the higher level opens a handle for a scope, the
scope closes it only once every future that may use it has finished, and the compiler rejects any use outside the
scope. While the scope is open the handle is open, so its number cannot be reused under anyone who holds it.

### What the compiler already does

Capture checking tracks a value whose type carries a capture set, `Handle^`, and rejects letting it outlive the scope
it belongs to. The opaque `Handle` can be tracked as it is: outside the file defining it, `Handle` is an abstract type,
and `Handle^` is tracked; inside, it is `Int`, which is pure, and writing `Handle^` there is an error. The backends get
the number with `caps.unsafe.unsafeAssumePure(h)`, the one place the checking is switched off, below the interface.

Checked with Scala 3.9.0:

| Program | Result |
| --- | --- |
| A scoped opener's handle returned out of its scope | rejected: the capability outlives its scope |
| A closure capturing the handle returned out of its scope | rejected |
| `close(consume h: Handle^)`, then a use of `h` | rejected: `h` was consumed |
| `close(h)` while another closure holding `h` is used afterwards | rejected |
| Opening, using, closing, or returning a handle (handing it on) | compiles |
| A future that uses `h` started, then `close(h)` | **compiles** |

### The gap: separation checking is not temporal

The last row is the problem this note is about. Separation checking tracks which names refer to a value, not when
another thread runs: once a closure capturing `h` has been handed to something that runs it later, nothing says the
close must wait for it. A future is exactly that.

Nor can the future take the handle over and close it itself: a function type cannot have a `consume` parameter, and a
closure cannot consume a value it captured ("argument to consume parameter refers to non-local value"). So closing
belongs to the scope that opened the handle, never to a future.

### Capturing the scope: spawners that say what their futures may capture

What closes the gap is to make a future's body capture only what its spawner allows, and to give a handle's scope a
spawner of its own that allows the handle, joined before the handle is closed:

```scala
final class Spawn[C^]                                       // stands in for gears' Async.Spawn
def future[C^](s: Spawn[{C}])(body: () ->{C} Unit): Unit   // a body may capture only what its spawner allows

def withConnection[T](using io: Io)(
    body: (h: Handle^) => (group: Spawn[{h, io}]) ->{h, io} T
): T                                                        // runs body, joins group, then closes h
```

The program's own spawner is a `Spawn[{io}]`, so a future started with it cannot capture `h`; the connection's
group is a `Spawn[{h, io}]`, so its futures can, and `withConnection` joins it before closing `h`. Checked:

| Program | Result |
| --- | --- |
| `future(group)(() => read(h))` inside `withConnection` | compiles |
| `future(outer)(() => read(h))` with the program's `Spawn[{io}]` | rejected: `h` cannot flow into `{io}` |
| `future(outer)(() => println("no handle"))` | compiles |

The capture set must be an **invariant** type parameter. Written as the spawner's own capture set, `Spawn^{C}`, it
widens: `Spawn^{io}` is a subtype of `Spawn^{h, io}`, and the compiler accepted the outer spawner's future.

Two shapes the scope would naturally take are not supported yet, and were curried around: a context function whose
second parameter depends on the first (`(h: Handle^) => Spawn[{h}] ?=> T`, "curried dependent context function
types are not supported"), and a function whose parameters depend on each other (`(h: Handle^, g: Spawn[{h}]) => T`).

### What it would take

- **gears.** `Async.Spawn`, or what `Future.apply` asks for, carries the invariant capture-set parameter, and a
  `Future`'s body is checked against it; `Async.group` gives a spawner whose set includes what the group's scope
  allows. This is a change to gears itself, not only to the I/O interface, and the larger part of the work.
- **The interface.** `Io` is a shared capability; handles are opened as fresh capabilities limited by it; an op
  captures its handle and buffer (`io.Op^{h, buf}`), so that a reusable op cannot outlive its handle either; `close`
  takes its handle as `consume`.
- **The higher level.** Scoped openers (`withConnection`, `withListener`, and a scope per accepted connection in a
  serve loop), each giving its body the handle and a group allowed to capture it.
- **Everything compiled with capture checking**: gears, the backends, the contract suites and the demos. A library
  compiled without it does not say what it captures, so the guarantee reaches only as far as code that is checked.

### What it gives, and what it does not

At compile time, for code over the higher level: a handle cannot be used after its scope closed it, cannot be closed
while one of its futures may still use it, and cannot escape into a future that outlives it. Hence its number cannot
be reused under anyone holding it. It does not cover the low level, where a program holds handles and ops directly,
nor code that switches the checking off; and a scope cannot close its handle early except in straight-line code that
owns it (`close(consume h)`).

## Recommendation

The two are complementary rather than alternatives:

- **Proposal 2 for programs**, at the higher level, since it prevents the mistake when compiling, which is the safety
  wanted; it is the larger change, starting with gears' spawners.
- **Proposal 1 under it**, as a runtime backstop for the low level and for code outside the compiler's checks: a
  stale handle then fails cleanly on every backend instead of reaching another device.

## Open questions

- Whether gears takes the invariant capture-set parameter on `Async.Spawn`, and how `Async.group`, `Future` and
  channels read with it; the first step is to try it on gears' own types in a branch.
- How `Handle.Opt`, `Handle.Slot` and `acceptAll`'s array hold tracked handles: a slot that receives a fresh capability,
  and an array of them, need capture checking's support for collections of capabilities.
- Whether proposal 1's `Long` is worth its cost before proposal 2 lands, or only once the low level is the only
  unchecked path left.
