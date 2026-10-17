# Kqueue reactor: syscall reduction TODO

Most operations used to pay for a `kevent` call of their own, or a trip through the wake pipe, even when the work could
be done right away on the reactor thread: a gears `perform(r, read)` that found data already waiting cost about 7
syscalls. Now it costs 1, the `read` itself, and an op that has to wait costs no `kevent` of its own either; see what
each operation costs now, below.

## Measuring

Measure each item with `kqueuedemo gears bench`: build the executables to compare with `kqueue-demo/bench/build.sh`
(release-fast, against the Scala Native nightly, since 0.5.12's GC can crash multithreaded release builds) and compare
them with `kqueue-demo/bench/compare.py` (see its header). Debug builds overstate the Scala-side cost, so measure
release builds. The scenarios map onto the items below: `pingpong` (items 2, 3 and the resume path),
`pingpong-fresh-ops` (the same, building its ops anew for every message), `pingpong-deadline` (item 5), `timers` (item
5: 1,000 timers pending at a time, half firing and half cancelled), `concurrent` (item 4), `connections` (item 7: a new
connection for each round trip, so an accept for each), `pipelined` (items 2 and 3 on
simultaneous reads and writes), `blocking` (item 6), and two that check that queued work is not starved: `mixed`,
ping-pong while a compute future on the same thread works in slices, reporting the compute side as `mixed-compute`, and
`fairness`, pipelined traffic while a ticker future on the same thread awaits zero-length timers, reporting the time
between ticks as `fairness-tick`. `kqueuedemo bench` runs the same scenarios against `KqueueLoop` directly, as a
reference for what a hand-written loop costs, except the gears-only `pingpong-fresh-ops`, `mixed` and `fairness`.
Medians from the same build differ by about 1% between runs (`blocking` by up to about 6%).

Every scenario has run since the first measurement, after one fix made for them: `mixed` never finished, because nothing
bounded the work a turn took on. `ReactorScheduler.drain` ran tasks until its queue was empty, including tasks they
queued, and the reactor delivered resolved promises the same way, while the scheduler then woke itself with a promise
per drain; so a computation yielding between slices kept the loop from ever polling. Both now take only what was queued
when they start. Nothing else changed beyond noise (release-fast, median of 7 runs, against the start with the same
scenarios: `blocking` +7%, the rest within 3%; `kqueue-demo/bench/results/starvation-fix.json`).

Median microseconds per operation over 5 runs of release-fast builds on the development machine, before and after items
1 and 6, with the hand-written reference (`kqueue-demo/bench/results/remove-wakeup-pipe.json`):

| scenario | before | after items 1 and 6 | raw `KqueueLoop` |
|---|---|---|---|
| pingpong | 28.5 | 18.6 (−35%) | 6.6 |
| pingpong-fresh-ops | 28.6 | 18.5 (−35%) | |
| pingpong-deadline | 31.9 | 20.4 (−36%) | 8.2 |
| concurrent | 7.7 | 7.4 (−4%) | 2.2 |
| pipelined | 9.0 | 7.3 (−19%) | 0.8 |
| blocking | 16.1 | 7.3 (−55%) | 4.7 |
| timers | 1.33 | 1.33 | 1.14 |
| fairness, per message | 12.8 | 9.8 (−24%) | |
| fairness-tick | 12.8 | 9.8 (−24%) | |
| mixed, per round trip | 45.6 | 37.0 (−19%) | |
| mixed-compute | 34.8 | 36.7 (+5%) | |

## Overall

From the first measurement (with the starvation fix) to now, release-fast, median of 5 runs, µs/op, with the
hand-written reference at each end (`kqueue-demo/bench/results/overall.json`):

| scenario | start | now | raw `KqueueLoop` at start | raw now |
|---|---|---|---|---|
| pingpong | 28.6 | 8.1 (−72%) | 6.6 | 5.4 |
| pingpong-fresh-ops | 28.6 | 8.2 (−71%) | | |
| pingpong-deadline | 32.0 | 8.2 (−74%) | 8.3 | 6.3 |
| concurrent | 8.1 | 4.0 (−50%) | 2.2 | 1.5 |
| pipelined | 9.5 | 0.67 (−93%) | 0.87 | 0.49 |
| blocking | 16.3 | 6.4 (−61%) | 4.8 | 4.0 |
| timers | 1.33 | 0.22 (−83%) | 1.15 | 0.66 |
| fairness, per message | 13.4 | 3.6 (−73%) | | |
| fairness-tick | 13.4 | 3.6 (−73%) | | |
| mixed, per round trip | 46.2 | 27.8 (−40%) | | |
| mixed-compute | 35.0 | 27.8 (−21%) | | |

Gears over the reactor went from 3.4–11× the hand-written loop's time to 1.3–2.6× (pingpong 4.3× → 1.5×, pipelined
11× → 1.4×, concurrent 3.7× → 2.6×). Pipelined traffic and timers now beat the hand-written loop as it was at the start,
and the timers scenario beats it as it is now, since user-space timers make no syscalls where the raw loop uses kernel
timers. The ticker in `fairness` still gets one tick per message, as at the start.

## What each operation costs now

By design, on the reactor thread:

| Operation | Syscalls | Where |
|---|---|---|
| Read, write, accept, receive or send that can proceed | 1 `read`/`write`/`accept`/…, tried before waiting | `KqueueReactor.submit` |
| A reactor read that has to wait | none, after the first on a descriptor, which adds its edge-triggered read filter (1 `kevent`); then the read when ready | `waitToRead` |
| Any other command that has to wait (writes, connects, `whenReady`) | 1 `kevent` to arm a one-shot filter, then the command when ready | `arm` |
| Each turn of the loop | none after a turn that delivered completions (it polls at least every 16 turns); otherwise up to 4 polls that return at once, then 1 waiting `kevent` | `runOnce`, `pollBeforeBlocking` |
| Set, fire or cancel a timer | none | `TimerHeap` |
| Cancel a reactor read | none; other commands: 1 `kevent` (`EV_DELETE`) | `cancel` |
| Close a descriptor | 1 `close`; the kernel drops its filters | `PosixSockets.close` |
| Gears task queued from the reactor thread, or a promise resolved there | none | `ReactorScheduler.execute`, `resolved` |
| Wake the loop from another thread | 1 `kevent(NOTE_TRIGGER)`, merged with any already pending | `wakeup` |
| Accept a connection | 1 `accept`: the accepted socket is already non-blocking (item 7) | `NonBlocking.accept` |
| Open a socket or pipe | 1 `ioctl(FIONBIO)` per descriptor to make it non-blocking, besides opening it | `PosixSockets.setNonBlocking` |
| Scala Native: each continuation resume, so each `perform` that suspends | `sigprocmask` + `sigaltstack`, from `setjmp` | see the proposed Scala Native changes |
| Scala Native: each `@blocking` call, here only the waiting `kevent` | `sigprocmask` + `sigaltstack`, from `setjmp` | the same |

Measured with `kqueue-demo/bench/syscalls.py` (lldb breakpoints on the system call wrappers; see its header), on the
client thread, per operation: per round trip for ping-pong and `mixed`, per message for `concurrent`, `pipelined` and
`fairness`, per task for `blocking` and per timer for `timers`. `kevent` counts both polls and waits; under the
debugger replies are slow, so the loop spins more than at full speed, and those counts are only indicative. The
signal columns are `sigprocmask` + `sigaltstack` pairs, so each is two syscalls
(`kqueue-demo/bench/results/syscalls-gears.json`, `syscalls-raw.json`):

| scenario | I/O calls | `kevent` changes | `kevent` polls and waits | signal pairs | raw: I/O, changes, polls and waits, signal pairs |
|---|---|---|---|---|---|
| pingpong | 2 | 0 | 4.1 | 1.3 | 2, 0, 1, 1 |
| pingpong-deadline | 2 | 0 | 4.1 | 1.3 | 2, 2, 1, 1 |
| concurrent | 2 | 0 | 0.2 | 1.9 | 2, 0, 0.13, 0.13 |
| pipelined | 1 | 0.01 | 0.03 | 0.06 | 1, 0.01, 0.03, 0.03 |
| blocking | 0, and 1 wake of the worker | 0 | 0.4 | 2 | 1, 0, 1, 2, and 1 wake |
| timers | 0 | 0 | 0.01 | 0 | 0, 1.5, 0, 0 |
| fairness | 1.1 | 0 | 0.07 | 2.1 | |
| mixed | 2 | 0 | 1.4 | 3.2 | |

- Registration changes are gone from every scenario: reads register once per descriptor, timers never reach the
  kernel, and ops that can proceed never register. Item 4 has nothing left to batch here.
- Most of what remains beyond the I/O itself is Scala Native's: the signal pairs, two syscalls each, outnumber the
  I/O calls in ping-pong (2.6 against 2), `concurrent` (3.7 against 2) and `mixed`. One pair comes per continuation
  resume. The fast path avoids it only for a future running alone, so in `concurrent`, with 64 connections, both the
  write and the read of each message still suspend, and in `mixed` the compute slices add theirs.
- The raw loop needs a waiting `kevent` per round trip, and its `@blocking` binding a signal pair; the reactor mostly
  spins instead, in polls that cost no signal pair.

## Proposals, in order of what they save

- [x] **1. Don't use the wake pipe for work queued on the reactor thread.**
  Done, in two steps. First `KqueueReactor.resolved` stopped writing to the pipe for promises resolved on the loop
  thread; on the gears key-value client (200,000 pipelined commands, about 400 resumes) that removed all 406 pipe writes
  and their draining reads, and cut `kevent` waits from 646 to 399. Then the reactor's promise op was removed: gears code
  uses gears promises, and the scheduler drives the loop itself with `Reactor.runOnce`, running its tasks between turns
  and blocking only when it has none. `ReactorScheduler.execute` calls `Reactor.wakeup` only from another thread, so
  resuming a continuation on the reactor thread costs no syscalls and no promise allocation.

- [x] **2. Try the operation before waiting for readiness.**
  Done in `KqueueReactor.submit`: a read, write, accept, receive, or send is performed at once, and only registered if
  it would block. `Connect` and `WhenReady` still wait (`Command.tryFirst`): a connect's error check passes while the
  connection is in progress, and `whenReady` promises its body runs once the descriptor is ready. A command that finished
  at once has its completion queued and run on the next turn, never inside `submit`; a turn delivers only what was queued
  when it began, so one busy descriptor cannot starve the rest.
  - Trying first alone made things worse (release: `pingpong` +67%, `pipelined` +108%): each finished command then cost
    a turn that still polled kqueue. A turn that delivered completions now skips the poll, polling at least every 16
    turns (`KqueueReactor.PollEvery`) so other descriptors are still seen.
  - Trying reads helps pipelined traffic but wastes a read per round trip in request/response traffic, where the reply
    is not there yet; item 3's readiness bits would remove that waste.
  - Release-fast, median of 5 runs, µs/op before → after: `pingpong` 18.5 → 12.1 (−35%), `pingpong-deadline` 20.4 → 17.6
    (−13%), `concurrent` 7.4 → 7.0 (−6%), `pipelined` 7.4 → 3.1 (−59%), `mixed` 36.7 → 32.3 (−12%, its compute slices
    36.5 → 32.2), `blocking` unchanged (`kqueue-demo/bench/results/try-first.json`).
  - Skipping polls cost fairness: `fairness-tick` 9.8 → 53.0 µs, one tick per 17 messages instead of one, as the
    ticker's kernel timers were only seen when a turn polled. Item 5's timers, checked every turn, restored it.

- [x] **3. Persistent edge-triggered registration instead of one-shot filters.**
  Done for reads, in `KqueueReactor`: a read that has to wait registers the descriptor's read filter once, with
  `EV_ADD | EV_CLEAR`, and later waits cost no syscall; cancelling it only clears the slot. Each slot tracks read
  readiness (unknown, ready, not ready), so item 2 now tries a read only when it is likely to succeed:
  - An edge with no read pending marks the descriptor ready. After a read, readiness comes from the bytes it took
    against the bytes the event reported available, or, for a read tried without an event, from whether it came back
    short. A read that would block marks it not ready, and the next read waits for an edge.
  - End of stream (`EV_EOF`) is reported only once by an edge-triggered filter but lasts, so a descriptor stays ready
    after it.
  - Only commands whose `false` means "would block" use it (`Command.edgeTriggered`: `ReadIntoBuffer`, `Accept`,
    `ReceiveFrom`). Other commands, such as the plain demos' own and `whenReady` bodies, may return false meaning
    "not finished" with data still waiting, and an edge-triggered filter would never fire again for them, so they keep
    one-shot, level-triggered filters. Writes, which rarely wait, also stay one-shot.
  - Descriptors must be closed through `PosixSockets.close` on the reactor's thread: its `DescriptorHooks` tell the
    reactor to forget the registration before the number can be reused.
  - Release-fast, median of 5 runs, µs/op from item 2 → item 3: `pingpong` 12.2 → 13.6 (+11%), `pingpong-deadline` 17.6
    → 15.5 (−12%), `concurrent` 6.9 → 4.9 (−30%), `pipelined` 3.1 → 2.8 (−12%), the rest within 2%
    (`kqueue-demo/bench/results/edge-triggered-reads.json`). Ping-pong is slower because the thread now blocks right
    after sending, without the syscalls that used to give the reply time to arrive, and so pays a wake-up per round
    trip; see item 8.

- [x] **4. Batch remaining changes into the wait call — decided against, for now.**
  `kevent` takes a list of changes and an array for events in one call, but the reactor issues a separate call per change.
  - Append `EV_ADD`/`EV_DELETE` entries (and timers, if kernel timers stay) to a pending change list, and pass it to the
    next waiting `kevent`.
  - Errors for individual changes then arrive as `EV_ERROR` events in the event array. Today `run` throws on any
    `EV_ERROR`: dispatch must instead fail the matching op, and ignore `ENOENT`/`EBADF` from deletes on closed
    descriptors.
  - Saves one syscall per change that items 2 and 3 don't remove (cancels, timers, first registrations).
  - Counted after items 2, 3 and 5 (see what each operation costs now), there is almost nothing left to batch:
    registration changes are about 0 per operation in every steady-state scenario, as reads register once per
    descriptor, timers never reach the kernel, and ops that can proceed never register. Only new connections still make
    them: the client registers twice per connection (the connect's write filter and the first read's filter) and the
    server once, each just before the loop waits, so batching would save about 2 of the client's roughly 20 system calls
    per connection in `connections`, and 1 of the server's.
  - Not worth the error handling it needs for that. Worth revisiting for workloads dominated by short connections, or by
    cancelling one-shot waits (writes, connects), each of which still costs an `EV_DELETE`.

- [x] **5. Timers in user space.**
  Done in `KqueueReactor`: a timer gets a deadline from `System.nanoTime` and goes into a binary min-heap on the loop
  thread (deadline, then submission order), and each `KqueueTimer` keeps its heap index, so cancelling it is an
  O(log n) removal. Setting or cancelling a timer makes no syscall: a blocking poll waits no longer than the earliest
  deadline, and due timers fire after the turn's events, only those submitted before the turn began, so a completion
  that resubmits a zero-length timer cannot stall the loop. `KqueueTimer` no longer has a kernel ID. The plain
  `timers` demo (and its test) checks 2,000 timers with every third cancelled: they fire in deadline order, and no
  cancelled one fires.
  - Release-fast, median of 5 runs, µs/op before → after, with the raw benchmark's kernel timers for reference: `timers`
    1.27 → 0.22 (−83%; raw 1.09), `pingpong-deadline` 10.9 → 9.4 (−14%), so a timer armed and cancelled per round trip
    now costs nothing measurable over plain `pingpong` (9.4). Due timers fire every turn, polled or not, so
    `fairness-tick` went 46.9 → 4.1 µs: the ticker gets one tick per message again, and the traffic beside it 2.8 → 4.1
    µs a message. `blocking` +7%, at the edge of its noise; everything else within 1.1%. Saved as
    `kqueue-demo/bench/results/user-space-timers.json`.

- [x] **6. Cheaper wake-ups from other threads.**
  Done: the wake pipe is replaced by an `EVFILT_USER` event, so waking is one `kevent(NOTE_TRIGGER)` call with nothing to
  drain, and the reactor no longer holds two pipe descriptors. Wake-ups are merged with an atomic flag, which the loop
  clears when it sees the event, so only the first `wakeup` since then makes the syscall. `wakeLock` remains, so that a
  trigger cannot reach the kqueue's descriptor number after `close` has released it.

- [x] **7. Accept with fewer syscalls.**
  Done in `NonBlocking.accept` and `PosixSockets.setNonBlocking`. On macOS and FreeBSD an accepted socket has the
  listening socket's properties, `O_NONBLOCK` among them (accept(2)), so accepting takes the `accept` alone, without the
  two `fcntl` calls; elsewhere it still sets the mode. Setting non-blocking mode, for each socket connected or listening
  and each pipe, takes one `ioctl(FIONBIO)` there instead of `fcntl(F_GETFL)` and `fcntl(F_SETFL)`, through `Unblocked`,
  since posixlib's `ioctl` is `@blocking` and has no `FIONBIO`.
  - Counted per new connection in the `connections` scenario: the server went from 2 `accept` + 2 `fcntl` to 2 `accept`,
    and the client from 2 `fcntl` to 1 `ioctl` (`kqueue-demo/bench/results/accept-fewer-syscalls-counts.json`).
    Release-fast, median of 5 runs: `connections` 46.6 → 45.1 µs (−3%), everything else unchanged
    (`kqueue-demo/bench/results/accept-fewer-syscalls.json`).
  - What remained per connection was mostly elsewhere: the raw loop took 20.4 µs to gears' 45.1. The server made a
    second `accept` per connection, the next accept tried at once and finding none (item 10 removed it); the client made
    two registrations (the connect's one-shot write filter and the first read's edge-triggered filter) and a read tried
    before the echo arrived (item 11 removed one registration and the read); and each side resumes several
    continuations, about 3.5 signal pairs on the client and 2 on the server.

- [x] **8. Poll a few times without blocking before sleeping.**
  Done in `KqueueReactor.runOnce`: before blocking, it polls without blocking up to `spinPolls` times
  (`KqueueReactor.open(spinPolls = ...)`, by default `DefaultSpinPolls`, 4; 0 blocks at once). In request/response
  traffic the reply often arrives microseconds after the loop runs out of work, and sleeping then cost a thread wake-up
  per round trip. The cost is at most a few extra `kevent` calls each time the loop goes idle.
  - The trial that chose 4 (release-fast, median of 5 runs, µs/op), by number of non-blocking polls before blocking:

    | polls | pingpong | pingpong-deadline | concurrent | pipelined |
    |---|---|---|---|---|
    | 0 | 14.5 | 16.2 | 5.5 | 3.0 |
    | 1 | 12.7 | 14.6 | 5.9 | 3.1 |
    | 4 | 12.3 | 12.4 | 5.3 | 2.8 |
    | 16 | 11.6 | 14.3 | 5.8 | 3.1 |
    | 64 | 11.8 | 12.1 | 5.2 | 3.0 |

  - Release-fast, median of 5 runs, µs/op before → after: `pingpong` 13.8 → 9.8 (−29%), `pingpong-deadline` 15.7 → 11.5
    (−27%), `mixed` 32.9 → 29.6 (−10%, its compute slices 32.7 → 29.6), the rest within 1.4%
    (`kqueue-demo/bench/results/spin-before-blocking.json`).
  - Spinning cannot delay queued work on the reactor thread: the loop only blocks, and so only spins, when the scheduler
    has no task and the turn delivered nothing. The `mixed` scenario checks it, comparing builds with 0 and 4 spin polls
    (release-fast, median of 5 runs, µs/op): `mixed` 34.4 → 28.8 (−16%) and `mixed-compute` 34.4 → 28.8 (−16%), so the
    compute work got faster rather than slower, alongside `pingpong` 13.1 → 9.5 and `pingpong-deadline` 15.0 → 11.0.

- [x] **9. Skip the GC's state switch for calls that cannot block.**
  Found by profiling: every posixlib `@blocking` call (`read`, `write`, `accept`, `sendto`, `recvmsg`, `kevent`)
  switches the thread's GC state, which on arm64 captures registers with libc `setjmp`, and macOS's `setjmp` saves the
  signal mask and alternate stack, two more system calls each. `asyncio.unsafe.Unblocked` binds those calls without
  `@blocking` where they cannot block: non-blocking descriptors, and `kevent` calls that only register or poll without
  waiting. A `kevent` that waits keeps posixlib's binding. Release-fast, median of 5 runs, µs/op before → after:
  `pingpong` 9.4 → 7.8 (−17%), `pingpong-deadline` 9.5 → 7.9 (−17%), `concurrent` 4.4 → 3.9 (−12%), `pipelined` 2.6 →
  2.3 (−12%), `blocking` 6.9 → 6.3 (−8%), `fairness` −6%, `mixed` −4%. Saved as
  `kqueue-demo/bench/results/unblocked-syscalls.json`.

- [x] **10. Know when a descriptor has drained, from kqueue's counts.**
  Done in `KqueueReactor.settleAfterRead`. A read event reports how much is waiting, bytes or, on a listening socket,
  pending connections. The slot keeps that count (`Slot.readAvailable`), from the event that delivered a read or that
  came with no read pending, less what each read or accept takes since (`Command.transferred`, one for `Accept`), and
  marks the descriptor not ready once it runs out, so the next read waits for an edge instead of trying a call that
  would block. Anything arriving later triggers the edge-triggered filter again, so nothing is missed. Before, a read
  with no event to compare against guessed from its own result, and an accept always guessed there might be more.
  - The server's accept loop made two `accept` calls per connection, the second finding none; now one
    (`kqueue-demo/bench/results/readable-counts-counts.json`). Release-fast, median of 5 runs: `connections` 47.5 → 45.8
    µs (−3.5%), the rest unchanged or slightly faster, within 3.5% (`kqueue-demo/bench/results/readable-counts.json`).

- [x] **11. Add a connecting socket's read filter with its connect.**
  Done in `KqueueReactor.armWithReads`: waiting for a connect (`Command.registersReads`) adds the descriptor's
  persistent read filter in the same `kevent` call as the connect's one-shot write filter. Nothing can then arrive
  unreported, as kqueue reports data already waiting when a filter is added, so the socket starts out not ready to read
  and its first read waits for an edge instead of trying.
  - Per new connection the client went from 2 registrations and 2 reads (one tried before the echo arrived) to 1 and 1
    (`kqueue-demo/bench/results/connect-registers-reads-counts.json`). Two system calls fewer, but no measurable time:
    release-fast, median of 5 runs, `connections` 43.5 → 43.1 µs (−0.8%), the rest within noise (`blocking` +4.7%, which
    this does not touch; `kqueue-demo/bench/results/connect-registers-reads.json`).

## Profiling findings

Sampling `pipelined` (macOS `sample`, release-fast) after item 9, with Scala Native's continuation resume patched as
below, self time is: I/O system calls 42%, thread-local lookups in the Scala Native runtime (`_tlv_get_addr`, mostly
per allocation) 21%, GC 14%, continuation stack copying 8%, gears 3%, and the reactor with the gears bridge about 1.4%.
What remains is mostly the runtime's cost per allocation, so allocating less per operation is the lever left here:
the scheduler's `ConcurrentLinkedQueue`, which computes a `ClassTag` per operation in Scala Native, and the objects
`perform` creates per call.

- `ReactorScheduler` now queues tasks from the reactor thread on a plain `ArrayDeque`, keeping a `ConcurrentLinkedQueue`
  only for tasks from other threads: release-fast, median of 5 runs, `concurrent` −9%, `blocking` −3%,
  `pingpong-deadline` and `mixed` −2%, the rest within 2% (`kqueue-demo/bench/results/scheduler-local-queue.json`).
  Small, as the profile predicted.

- Allocations per ping-pong round trip (four `perform`s; counted with lldb on `scalanative_GC_alloc*`, 200 round
  trips less 100) went from 113 to 58:
  - each op is now its own record while pending (`KqueueOp.completion`), so `submit` allocates nothing;
  - ops have an attachment (`Reactor.attachment`/`attach`, like kqueue's `udata` or io_uring's `user_data`), where
    `perform` keeps its `OpWaiter` and reuses it for the op's next submission, with a generation counter so a late
    cancel cannot reach a later submission;
  - `OpWaiter` overrides gears' allocating no-argument `poll()`; `ReactorFutures.run` tracks the program's completion
    with a flag instead of polling it every turn; polls without waiting share one zero `timespec`; each reactor
    allocates its event buffer once instead of a zeroed stack buffer per turn; `KqueueLoop`'s buffer helpers are
    `inline`, so they allocate no closures.
  The 58 left are nearly all gears' and Scala Native's (await closures and pointer boxing, `NativeSuspend`, `Try`, the
  `Continuation` and its stack fragments, `withGroup`, `CompletionGroup`'s hash set, `AwaitListener`). Release-fast,
  median of 5 runs: `pipelined` −10%, `fairness` −6%, `timers` −5%, `concurrent` −4%, `blocking` −2%, ping-pong within
  2% (`kqueue-demo/bench/results/reuse-op-records.json`).

- Building ops per call is cheap, so per-descriptor ops were tried and decided against. The `pingpong-fresh-ops`
  scenario builds its write and read ops anew for every message (`perform(r, r.ops.read(fd, buf))`, as code that
  does not keep them does); it ran 8.29 µs a round trip against `pingpong`'s 8.20 with ops kept, about 1% more.
  Letting the reactor keep one op of each kind per descriptor, retargeted each time it is built (so building it
  allocated nothing and its attachment, holding the gears waiter, carried over), left it at 8.36: no measurable
  change, and only about 2.5 fewer allocations per round trip. Not worth the cost: building `ops.read(fd, bufB)`
  would retarget the op an earlier `ops.read(fd, bufA)` returned, ops could only be built on the reactor's thread,
  and building a pending op had to fail. Callers who want no allocation can keep their ops, which the attachment
  already makes free in `perform`.

- Thread-local lookups (`_tlv_get_addr`, 18–20% of CPU in the throughput scenarios) are mostly the safepoint polls
  Scala Native emits: with trap-based yieldpoints, the default in release builds, every poll is a volatile load of the
  thread-local `scalanative_GC_yieldpoint_trap`, and on macOS every thread-local access is a call. By caller: safepoint
  polls 66%, `currentThread` 11%, the allocator 10%, monitors 8%.
  - Linking with `SCALANATIVE_GC_TRAP_BASED_YIELDPOINTS=0` (conditional yieldpoints, which check a global flag) gained
    7–14% on the throughput scenarios (release-fast against the nightly, median of 5 runs: `pipelined` −10%,
    `concurrent` −7%, `blocking` −7%, `timers` −14%, ping-pong −1%;
    `kqueue-demo/bench/results/conditional-yieldpoints.json`), and ran without failure on the nightly. This is
    not made the default here; it is worth recommending to applications that only use thread-owned reactors, with
    futures that stay on their reactor's thread and no work-stealing workers.
  - In our code, `runOnce` no longer calls `Thread.currentThread()` every turn, and `perform` takes its scheduler from
    its `Async` context instead of a `ThreadLocal`. Simpler, but no measurable difference
    (`kqueue-demo/bench/results/fewer-current-thread.json`).

- `perform` no longer suspends for an op that finishes at once. `Reactor.trySubmit` finishes such an op inside the call
  and returns true without calling its completion; `perform` then returns, skipping the await, the continuation switch
  and the scheduler round trip. It does so only while no other task is queued (`ReactorScheduler.isIdle`), so a runnable
  future never waits behind it, and at most `ReactorScheduler.FastPathBudget` (32) times per scheduler round, so the
  reactor still polls for futures waiting on events. Release-fast, median of 5 runs, µs/op before → after: `pipelined`
  2.23 → 0.67 (−70%), `concurrent` 4.08 → 4.00 (−2%), ping-pong −2 to −3%, `fairness-tick` 3.61 → 3.68, `mixed-compute`
  28.2 → 27.8, `blocking` and `timers` unchanged (`kqueue-demo/bench/results/fast-path.json`).
  - The budget alone was tried first. Fairness is per scheduler turn, not per op, so traffic whose ops finish at once
    then got a budget's worth of ops per turn of the futures sharing its thread: with a budget of 8, `fairness-tick`
    3.85 → 8.56 µs (one tick per ~8.6 messages instead of one), and in `mixed` ping-pong took one turn per round trip
    instead of two, so the compute future's slices went 29.0 → 40.5 µs while ping-pong went 29.1 → 20.4. Budgets of 1,
    4, 8 and 32 gave `pipelined` −29%, −46%, −66%, −72% and `fairness-tick` +37%, +87%, +122%, +375%
    (`kqueue-demo/bench/results/fast-path-budgets.json`, 3 runs). Requiring an idle scheduler as well removed the cost
    to fairness entirely while keeping the gain, which comes from futures that run alone.

## Proposed changes to Scala Native

Costs found while profiling that only Scala Native can remove, all on macOS arm64. The `scala-native-setjmp-issue`
branch has a draft issue (`scala-native-issue-setjmp.md`) covering them.

- **Continuation resume: `_setjmp` instead of `setjmp`.**
  Problem: every continuation resume, so every `perform` that suspends, costs two extra system calls.
  `scalanative_continuation_resume` (`delimcc.c`) calls libc `setjmp`, which on macOS and the BSDs also saves the
  signal mask and alternate stack (`sigprocmask` and `sigaltstack`); glibc's does not. The mask needs no saving there.
  Fix: `_setjmp` in `delimcc.c`, with the matching `_longjmp` in `eh.c` and `eh.cpp`. Measured by patching the
  nightly's `delimcc.c` and `eh.c` in the link output (release-fast, median of 5 runs): `pipelined` −13%,
  `concurrent` −10%, `pingpong-deadline` −11%, `pingpong` −8%, `blocking` −2%
  (`kqueue-demo/bench/results/patched-setjmp.json`). No workaround on our side.
- **GC register capture on arm64: `_setjmp` or inline assembly instead of `setjmp`.**
  Problem: every `@blocking` extern call costs two extra system calls, so posixlib's `read`, `write` and `kevent` cost
  three each. Around every `@blocking` call the thread's GC state switches, and `RegistersCapture`
  (`gc/immix_commix/RegistersCapture.h`) uses inline moves on x86 but falls back to libc `setjmp` everywhere else.
  Fix: `_setjmp`, or capturing x19–x28, the frame pointer and the stack pointer inline as on x86-64. Our workaround is
  item 9: `asyncio.unsafe.Unblocked` binds the calls that cannot block without `@blocking`, which gained 8–17%
  (`kqueue-demo/bench/results/unblocked-syscalls.json`). The kevent call that waits still pays. With the fix, those
  bindings could go and every library's `@blocking` calls would get cheaper.
- **Safepoint polls without a thread-local on macOS.**
  Problem: thread-local lookups (`_tlv_get_addr`) take 18–20% of CPU in the throughput scenarios, two thirds of it from
  safepoint polls. With trap-based yieldpoints, the default in release builds, every poll is a volatile load of the
  thread-local `scalanative_GC_yieldpoint_trap`, and on macOS every thread-local access is a function call where Linux
  has a register-relative load (see the profiling findings above). Fix, for Scala Native to choose: a process-wide trap
  page where per-thread stopping is not needed, or storing the cell where Darwin can read it without `_tlv_get_addr`.
  Conditional yieldpoints, which poll a global, show what is at stake: 7–14% on the throughput scenarios
  (`kqueue-demo/bench/results/conditional-yieldpoints.json`). Our workaround is only a recommendation: link with
  `SCALANATIVE_GC_TRAP_BASED_YIELDPOINTS=0` when using thread-owned reactors only.
- **The GC's interior-pointer roots** were fixed upstream ([scala-native#5080](https://github.com/scala-native/scala-native/pull/5080)).
  The problem: release builds crashed on 0.5.12. Waiting for 0.5.13; see the last item below.

## Related, not syscalls

- [x] **Read event fields in one call — tried, and decided against.**
  `dispatch` reads each event through posixlib's `scalanative_kevent_get`, one call per field, each with its own
  `stackalloc`. Reading `struct kevent` directly instead, at the offsets its leading fields share on 64-bit macOS and
  FreeBSD (with a start-up check against posixlib, to catch a platform with a different layout), gained only about 1%
  (release-fast, median of 7 runs, µs/op before → after: `pingpong` 10.6 → 10.5, `concurrent` 5.5 → 5.5, `pipelined`
  3.1 → 3.1, all −0.6% to −1.6%, at the edge of the noise). Not worth depending on the struct's layout for that, so the
  accessors stay on posixlib. It also suggests per-event C calls are not where the remaining time goes.
- [x] **SIGPIPE (correctness).**
  Done in `PosixSockets`: every socket gets `SO_NOSIGPIPE` when created (posixlib does not define it: 0x1022 on macOS,
  0x0800 on FreeBSD), so writing to a peer that has gone fails that connection with `EPIPE` instead of raising
  `SIGPIPE`, which ended the whole process: a client resetting its connection before the reply killed the plain server
  (exit 141). Accepted sockets inherit it from the listening socket, so `accept` needs no extra call. The stream test
  now has clients go away before their reply and checks the server still answers. Pipes and FIFOs, which
  `KqueueHandles.pipe` and `openFile` also open, get macOS's per-descriptor `F_SETNOSIGPIPE` on their write ends (posixlib
  does not define it: 73), checked by the plain `broken-pipe` demo and its test. FreeBSD has no per-descriptor
  equivalent, so there a write to a pipe or FIFO whose reader has closed still raises `SIGPIPE`, unless the application
  ignores the signal process-wide, as the JVM does.
- [ ] **Move to Scala Native 0.5.13 once it is released.** On 0.5.12, multithreaded release builds of the gears demos
  crash intermittently in `ReactorFutures.perform` just after a continuation resumes: the GC ignored conservative roots
  that point partway into an object, so an object referenced only by such an interior pointer, as optimised code can
  hold across a suspension, was collected while live ([scala-native#5080](https://github.com/scala-native/scala-native/pull/5080),
  the fix for [#5046](https://github.com/scala-native/scala-native/issues/5046)). Debug builds are unaffected. The
  `gears-release-repro` branch reproduces it with gears alone: two threads awaiting through `Async.withGroup`. Until then,
  `SCALA_NATIVE_NIGHTLY=1` builds against a nightly that includes the fix, and `kqueue-demo/bench/build.sh` uses it for
  benchmark executables. When moving, set the plugin version in `project/plugins.sbt` and remove the nightly switch
  there and in `build.sbt`, along with the nightly check in `build.sh`.
