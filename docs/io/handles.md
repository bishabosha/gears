# Handles that outlive their I/O object: a design note

A `Handle` names an open socket, pipe, FIFO or file by a number, and the number is reused once its handle is closed. So
a consumer still holding a closed handle can, without any error, reach whatever I/O object was opened under the number
since. This note records the problem, what protects against it today and what does not, and four proposals. Three change
what a handle is, so that a stale one fails cleanly as the program runs: a generation encoded in the handle, a number
that only grows and is never reused, or a reference to an object that cannot be forged. The fourth ties a handle to a
gears scope that capture checking enforces when compiling. Written on 2026-10-24; the capture checking findings come
from small programs compiled with Scala 3.9.0, `language.experimental.captureChecking` and `separationChecking`. Nothing
in the interface has changed for it yet.

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

- **Representation.** `Handle` becomes an opaque `Long`: the number in the low 20 bits, the generation in the high
  44. Still unboxed. Packing both into an `Int` would leave too few bits for one of them: a generation of 8 bits wraps
  after 256 closes of one number, which a busy server reaches in seconds.
- **The generation wraps, so it needs most of the bits.** Closes fall on few numbers: kqueue's descriptors are the
  lowest free, and `HandleTable` reuses the most recently freed first, so a server of short connections closes the
  same handful of numbers over and over. A generation of `b` bits wraps after 2^b closes of one number:

  | Closes of one number a second | 32 bits wrap after | 44 bits wrap after |
  | --- | --- | --- |
  | 10,000 | 5 days | 56 years |
  | 100,000 | 12 hours | 5.6 years |
  | 1,000,000 | 72 minutes | 200 days |

  A wrap does not turn the check off: a stale handle passes only when its number has been closed an exact multiple of
  2^b times since it was good. But that makes the check a likelihood rather than a guarantee, and a half split of the
  `Long` (32 and 32) wraps within a day on a busy server. 44 bits outlast any run of one.
- **The number then has 20 bits**, a million handles. That is Linux's default ceiling on a process's descriptors
  (`fs.nr_open`) and the most the Native runner raises the open-file limit to when the hard limit is unlimited
  (`FileLimit`), but not a bound the kernel keeps: with a finite hard limit `FileLimit` raises to it, whatever it is.
  So the runner keeps the limit at or below 2^20, and an open given a descriptor above it fails, as `select` refuses a
  descriptor past `FD_SETSIZE`. A 24-bit number, 16 million handles, would leave 40 bits, which wrap in four months
  at 100,000 closes a second: too short.
- **Or retire a number before it wraps,** as slot maps do: once its generation reaches the last value, the number is
  never given out again. `HandleTable` can, since it chooses its numbers. kqueue cannot while a handle is the
  descriptor, since the kernel chooses which number an open reuses; it would have to map handles to descriptors, which
  is proposal 2's cost. Retiring is what makes the generation exact rather than very likely; with 44 bits it is
  insurance against a server that runs for decades.
- **Not to be confused with `DescriptorTable`'s generation**, 32 bits today, which only tells the events and ops of a
  descriptor from just before a close apart from those just after it, a window of microseconds: wrapping is harmless
  there.
- **Per backend.** kqueue compares with `DescriptorTable`'s generation, which it reads already. `HandleTable` keeps a
  generation per entry, incremented by `remove`. Network.framework and NIO check it where they look the entry up.
- **What changes around it.** `Handle.Opt` and `Handle.Slot` hold a `Long`; `acceptAll`'s `Array[Handle]` becomes an
  array of `Long`s; `KqueueHandles.handle(fd)`, for kqueue's own demos, needs the descriptor's current generation.
- **Cost.** One comparison per op, against a value the backends mostly load anyway, and 4 more bytes per handle.
- **What it gives.** Detection, not prevention: B's read in the example fails cleanly instead of reading the wrong
  device, and it fails on every backend the same way. It covers the low level (`Ops`, `perform`) as well as anything
  built on it, and code that switches off the compiler's checks.
- **What it does not.** It finds the mistake when the program runs, possibly in production, and only if the stale
  handle is used at all. It does not stop a future from holding a handle past its close. And unless a number is
  retired before it wraps, which kqueue cannot do, it finds it with a likelihood, if a very high one, rather than
  always.

## Proposal 2: a number that only grows, never reused

Make a handle an id from a counter that only grows, a `Long`, so that no two I/O objects are ever given the same one.
A closed handle's id is simply absent from the table afterwards, and a stale handle fails with "Not an open handle"
however many objects were opened since.

- **Representation.** `Handle` becomes an opaque `Long`, as in proposal 1, but all 64 bits are the id. A counter of
  63 bits does not wrap: at a billion opens a second it would take three centuries. Unlike proposal 1's generation,
  which counts the closes of one number, the counter counts every open of the process, and still never runs out.
- **The table.** Ids are not dense, so the table can no longer be an array indexed by the handle: it becomes a map
  from id to object, a concurrent hash map, or an array of slots whose entry also records the id it holds, which is
  proposal 1 again under another name.
- **Per backend.** kqueue's handles stop being descriptors: every op maps the id to its descriptor first, where today
  the handle is the descriptor and costs nothing to resolve. NIO and Network.framework trade an array index for a hash
  lookup.
- **What it gives.** The same detection as proposal 1, and simpler to reason about: an id names one object for the life
  of the process, with nothing to compare. A handle printed in a log means one thing.
- **What it costs.** A hash lookup on every op, and on kqueue a lookup where there was none. The generation of
  proposal 1 gets the same answer with an array index and one comparison; this is the version to prefer only if the
  lookup measures as cheap, or if ids that are never reused matter for their own sake, as in logs and traces.

## Proposal 3: a reference to an object, which cannot be forged

Make a handle an object rather than a number: a final class the backend creates when it opens something, holding what
the backend needs (the descriptor, the channel, the connection) and whether it is closed. A program can only get one
from an op that opened it, so it cannot be forged from an `Int`, and a new open makes a new object, so a stale handle
can never be confused with a new one: it is the closed object, and an op on it fails.

- **Representation.** `Handle` becomes a class, allocated once per open. `Handle.Opt` and `Handle.Slot` hold a
  reference, and `acceptAll`'s array is an array of references. Nothing is unboxed any more.
- **No table lookup.** The op reaches the descriptor, channel or connection straight from the handle: NIO and
  Network.framework no longer look an index up in `HandleTable`, which could go, and kqueue reads the descriptor from
  a field.
- **Forging and mixing.** Today `Handle.wrap` is `private[io]`, which keeps programs from making a handle from a
  number, but a cast still can, and a handle of one backend passed to another is just a number, which the other reads
  as one of its own. An object belongs to the backend that made it, so an op can refuse another backend's handle, and a
  cast to it fails.
- **Closing while in use.** A closed flag alone races with a use on another thread: kqueue could check the flag, the
  close run and its descriptor number be reused, and then the use reach the new descriptor. The object can count the
  ops using it and close the descriptor only once the last has left, as the JDK does for a channel a thread is blocked
  in (`preClose`, a `dup2` onto a dead socket, then the real close when the last thread leaves). That is a cost every
  proposal here would pay to make a close concurrent with use safe, not this one's alone.
- **Capture checking.** An object can be a capability class (`caps.SharedCapability`, or an exclusive one), which an
  opaque `Int` cannot: the backend could then say how a handle may be shared, by classifier, rather than only whether
  it may escape (see proposal 4).
- **What it costs.** An allocation per open, which is small against the system call, and a reference to follow on each
  op, which replaces the table lookup NIO and Network.framework do today. kqueue pays both, having needed neither.

## Proposal 4: handles tied to a gears scope, enforced by capture checking

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

## Comparison

| | 1. Generation | 2. Number that only grows | 3. Reference object | 4. Scopes and capture checking |
| --- | --- | --- | --- | --- |
| A stale handle | fails as it is used | fails as it is used | fails as it is used | does not compile |
| Wraps around | after 2^44 closes of one number, unless retired | no (2^63 ids) | no | no |
| Handles at once | 2^20, enforced by the runner | unbounded | unbounded | as today |
| Representation | opaque `Long` | opaque `Long` | a class, one per open | as today, tracked |
| Per op | a comparison | a hash lookup | a field read | nothing |
| kqueue | compares with `DescriptorTable` | maps id to descriptor | reads the descriptor from the handle | as today |
| Forged or another backend's handle | read as one of its own | not found, or another's id | refused | as today |
| Covers the low level (`Ops`) | yes | yes | yes | no |
| Changes gears | no | no | no | yes: spawners' capture sets |

## Recommendation

Proposal 4 for programs, and one of proposals 1 to 3 under it:

- **Proposal 4 at the higher level**, since it prevents the mistake when compiling, which is the safety wanted; it is
  the larger change, starting with gears' spawners.
- **One runtime proposal under it**, as a backstop for the low level and for code outside the compiler's checks, so
  that a stale handle fails cleanly on every backend instead of reaching another device. Proposal 1 is the cheapest for
  kqueue, whose handles stay descriptors, but it needs its bits split with care, 20 for the number and 44 for the
  generation, and the open-file limit held to 2^20, and it is exact only where numbers are retired before they wrap,
  which kqueue cannot do; proposal 3 is the strongest, since it also refuses forged handles and another backend's,
  removes the table lookup on NIO and Network.framework, and gives capture checking a class to classify; proposal 2 sits
  between and buys its never-reused ids with a hash lookup. Which one is a measurement: the cost of an allocation per
  open and a reference per op on kqueue, against a comparison per op.

## Open questions

- Whether gears takes the invariant capture-set parameter on `Async.Spawn`, and how `Async.group`, `Future` and
  channels read with it; the first step is to try it on gears' own types in a branch.
- How `Handle.Opt`, `Handle.Slot` and `acceptAll`'s array hold tracked handles: a slot that receives a fresh capability,
  and an array of them, need capture checking's support for collections of capabilities.
- Which runtime proposal, measured on the bench: the generation's comparison, the growing id's hash lookup, or the
  reference object's allocation per open, on kqueue in particular, whose handles are free to resolve today.
- Whether the runtime proposal is worth its cost before proposal 4 lands, or only once the low level is the only
  unchecked path left.
- Whether a close concurrent with a use is made safe (a count of the ops using a handle, closing once the last leaves),
  which any of proposals 1 to 3 needs to be airtight on kqueue, where the descriptor number is the kernel's to reuse.
