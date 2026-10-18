# Kqueue reactor: syscall reduction TODO

Most operations used to pay for a `kevent` call of their own, or a trip through the wake pipe, even when the work could
be done right away on the reactor thread: a gears `perform(r, read)` that found data already waiting cost about 7
syscalls. Now it costs 1, the `read` itself, and an op that has to wait costs no `kevent` of its own either; see what
each operation costs now, below.

## Measuring

Measure each item with `kqueuedemo gears bench`: build the executables to compare with `kqueue-demo/bench/build.sh`
(release-fast, against the Scala Native nightly, since 0.5.12's GC can crash multithreaded release builds) and compare
them with `kqueue-demo/bench/compare.py` (see its header). From the thread-pool work on, `build.sh` links with
conditional yieldpoints, since trap-based ones can deadlock a collection on macOS (see the GC's signal blocking, under
the proposed Scala Native changes); `YIELDPOINTS=trap` links trap-based ones, which every earlier result used, and each
build's provenance says which. At the switch, conditional against trap-based (release-fast, median of 5 runs, under a
load average of 10): `timers` −9.5%, `fairness` −7%, `mixed` −4.5%, the rest −1 to −3%, `pingpong-deadline` +0.4%
(`kqueue-demo/bench/results/conditional-yieldpoints-default.json`). Debug builds overstate the Scala-side cost, so measure
release builds. The scenarios map onto the items below: `pingpong` (items 2, 3 and the resume path),
`pingpong-fresh-ops` (the same, building its ops anew for every message), `pingpong-deadline` (item 5), `timers` (item
5: 1,000 timers pending at a time, half firing and half cancelled), `concurrent` (item 4), `connections` (item 7: a new
connection for each round trip, so an accept for each), `pipelined` (items 2 and 3 on
simultaneous reads and writes), `blocking` (item 6), and two that check that queued work is not starved: `mixed`,
ping-pong while a compute future on the same thread works in slices, reporting the compute side as `mixed-compute`, and
`fairness`, pipelined traffic while a ticker future on the same thread awaits zero-length timers, reporting the time
between ticks as `fairness-tick`. `blocking-io` alternates a trivial blocking task with a round trip on one connection. `kqueuedemo bench` runs the same scenarios against `KqueueLoop` directly, as a
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
    futures that stay on their reactor's thread and no workers taking tasks from one another.
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
  - Letting writes take the fast path even with other tasks queued was tried and reverted: writes rarely wait, and the
    hope was to save a suspension per message where many futures run at once. Release-fast, median of 5 runs:
    `concurrent` 3.74 → 2.95 µs (−21%), but `fairness-tick` 3.41 → 17.59 µs (+416%, one tick per 25 messages instead of
    one) and `mixed-compute` 26.7 → 37.4 µs (+40%), as ping-pong took one turn per round trip instead of two; the rest
    within 3% (`kqueue-demo/bench/results/fast-path-writes.json`). It is the same imbalance as the budget alone: an op
    that finishes without suspending gives the turn to no one, so the futures beside it fall behind. Recovering
    `concurrent`'s gain fairly needs fewer resumes rather than skipped turns, such as cheaper resumes in Scala Native
    (see the proposed Scala Native changes).

## Gears itself

What gears does per suspension, beyond the continuation itself, measured on `concurrent`, where 64 futures share the
thread and every op suspends. Each step is measured against the one before (release-fast, median of 5 runs, scenarios
`concurrent`, `pingpong`, `pipelined`, `fairness` and `mixed`).

- Await without a lock or group link where nothing can cancel the wait (`FutureAsync.await` in the `Unlinked` group,
  which `perform` uses): `AwaitListener` takes a `ReentrantLock` and a number from a global counter, links into the
  group and unlinks again, each under `synchronized`, and resumes through a closure, all so that a cancellation cannot
  race the completion. With nothing to cancel, `UnlinkedAwaitListener` has no lock and no link, and is itself the task
  that resumes the suspension. A future's `Unlinked` view of its context is also made once, instead of on every
  `withGroup`. `concurrent` 4.30 → 3.85 µs (−10.5%), `fairness` −11%, the rest −1 to −3%
  (`kqueue-demo/bench/results/gears-unlinked-await.json`).
- Keep `perform`'s `OpWaiter` state in a plain field: every access is on the reactor thread (completions, the awaiting
  future, and a cancel, which hops there first), and Scala Native's `AtomicReference` computes a `ClassTag` on every
  operation, which showed in the profile. `concurrent` 3.95 → 3.85 µs (−2.5%), `fairness` −4.7%, the rest within 3%
  (`kqueue-demo/bench/results/gears-plain-waiter.json`).
- Keep a completion group's lone member in a field (`CompletionGroup.single`), using the set only for more: a group
  mostly has at most one member at a time, such as a future's wait in progress, and `perform` links its waiter into the
  caller's group and out again on every op, each time allocating a hash set node. Also `await` matches the polled
  `Option` instead of passing a closure to `getOrElse`. `concurrent` 3.32 → 3.28 µs (−1.2%), `fairness` −3%, the rest
  within 3% either way (`kqueue-demo/bench/results/gears-single-member.json`). Marginal, but it removes two allocations
  per op.
- Together, against every scenario (release-fast, median of 5 runs): `concurrent` 3.69 → 3.20 µs (−13%; −13.1% again
  over 7 runs), `fairness` −15%, ping-pong −2 to −3%, `blocking` −2%, `mixed` −1%, `connections` and `timers` unchanged,
  and `pipelined` +4.5%, which 7 runs put at +1.5%, one step of the benchmark's resolution
  (`kqueue-demo/bench/results/gears-climb.json`).
- What is left per suspension is mostly Scala Native's: about 8 allocations in `Continuations.suspend` (the
  `Continuation`, its `ArrayBuffer` of stack fragments and the fragments, closures and `Try`s), the signal-state system
  calls of each resume, and thread-local lookups, against about 3 allocations in gears (the listener, the
  `NativeContinuation` wrapper and the suspend closure). Gears' own remaining cost is small: `perform` linking its
  waiter into the caller's group and out again takes four uncontended monitors per op, worth at most about 2% by the
  profile. Doing fewer suspensions would gain more, but taking the fast path for writes under load showed what that
  costs in fairness (see the fast path, under profiling findings).

### Fewer suspensions

Fairness decided per turn of a future rather than per op: each task the scheduler runs is one future's turn, so a
budget reset before each task bounds how long one future keeps the thread, whatever the other futures are doing. Each
step is measured against the one before, as above, with `connections` as well.

- A fast path budget per turn (`ReactorScheduler.turnFastPathsLeft`, 1 op): `perform` finishes an op without
  suspending once in each turn of its caller, even with other tasks queued, and beyond that only as before, while none
  is. A future whose op has just completed usually issues the next at once: in `concurrent` the write after a read's
  echo, or the read after a write, so most round trips take one turn instead of two. `concurrent` 3.49 → 2.16 µs
  (−38%), `fairness` traffic −40%, `mixed` ping-pong −30%, `pingpong` −11%, `pipelined` and `connections` unchanged.
  The futures beside a busy one get the same share of turns, but a turn now does more: `fairness-tick` +20% (one tick
  per two messages, the resumed one and the fast one, instead of one per message), and `mixed-compute` +39%, one slice
  per round trip instead of two (`kqueue-demo/bench/results/turn-fast-path.json`). This is what Tokio's cooperative
  budget does, per task poll.
- A voluntary yield (`ReactorFutures.yieldNow`): suspends the caller and queues its resume behind the tasks already
  queued, by awaiting a source that completes its listener as soon as it is added. gears has no yield of its own, so
  `mixed` yielded by awaiting each slice as a future of its own, which took two turns per slice, the slice's and the
  resume's; with `yieldNow` it takes one. `mixed-compute` 37.0 → 28.1 µs (−24%), and `mixed` ping-pong 18.5 → 28.1 µs
  (+52%): both back to where they were before the turn budget, as the thread now alternates one slice with one round
  trip, so this changes the share rather than the total. The rest within 2% (`kqueue-demo/bench/results/yield-now.json`,
  under a load average of 9). The benchmark itself changed here: the before side runs the old `mixed`.
- Together, against every scenario (release-fast, median of 5 runs): `concurrent` 3.10 → 2.24 µs (−28%), `fairness`
  traffic −38%, `blocking` −2%, the rest within 1.5%, but `fairness-tick` +24% and `mixed` +7% on both sides, which 7
  runs confirmed (+19% and +7%): the thread still alternates one compute slice with one round trip, but each pair takes
  about 2 µs longer, for a reason not yet found (`kqueue-demo/bench/results/turns-combined.json` and
  `turns-combined-mixed-check.json`). The ticker's cost is the trade the turn budget makes: the futures beside a busy
  one get as many turns as before, each a little further apart.

## Against cats-effect and fs2

`kqueue-demo/ce-bench` writes the same scenarios with cats-effect 3.7.1 and fs2-io 3.14.0, the current releases, whose
Scala Native runtime has the design planned below: a pool whose workers take tasks from one another and each poll their
own kqueue (`KqueueSystem`). Same message size, counts and result lines; the echo server runs on a runtime of its own
and the clients on another, each with one worker unless `--workers` says otherwise. Build it with `PROJECT=ceBench
kqueue-demo/bench/build.sh` and compare it as `<label>=<path>@raw`.

Release-fast, conditional yieldpoints, median of 5 runs, µs/op (`kqueue-demo/bench/results/vs-cats-effect.json`):

| scenario | gears | hand-written loop | cats-effect |
|---|---|---|---|
| pingpong | 7.51 | 4.97 | 23.0 |
| pingpong-deadline | 7.50 | 5.87 | 26.8 |
| concurrent | 2.11 | 1.11 | 9.85 |
| connections | 41.5 | 18.7 | 150 |
| pipelined | 0.65 | 0.48 | 2.15 |
| blocking | 5.30 | 3.71 | 0.07 |
| timers | 0.18 | 0.60 | 1.01 |
| fairness (tick) | 1.63 (3.25) | | 2.25 (3.30) |
| mixed (compute) | 27.1 (27.0) | | 1173 (19.2) |

- I/O costs 3–5 times what gears does. In ping-pong, both sides of cats-effect sleep in `kevent64` for every message,
  two thread wake-ups per round trip, where gears' spin-before-blocking (item 8) leaves 0.6. Per round trip it makes
  about 24 system calls to gears' 17: 8 `@blocking` calls (`read` twice on each side, one of them finding nothing yet,
  `write`, `kevent64`), each with the `sigprocmask`/`sigaltstack` pair that gears avoids with item 9. When busy
  (`concurrent`), its CPU goes to the GC (23%), its run loop (21%), clearing memory (10%) and thread-locals (9%).
  More client workers made no difference (1, 2, 4 and 10 workers within 6%).
- `mixed`: with a compute fiber always runnable, the worker polls its kqueue rarely, so a round trip waits about 60
  compute slices (1.2 ms); the compute side runs faster for it. gears polls after every round of tasks.
- Not like for like: `blocking` (`IO.blocking` runs on the calling thread, handing the worker's role to another, so
  a run of them stays put; gears hands each task to a worker thread and back), and `timers` (a fiber per timer).
- `blocking-io` shows what that hand-over costs when each blocking task is followed by I/O, so each one needs it again.
  Release-fast, median of 5 runs, µs/op, with the blocking task's share as `blocking-io` minus `pingpong`: gears 15.08
  (6.96), the hand-written loop 12.57 (7.37), cats-effect 25.89 (1.97) (`kqueue-demo/bench/results/blocking-io.json`).
  cats-effect's hand-over is off the task's path: the task runs the blocking code at once, while the cached thread
  that takes over the worker's queue and poller wakes up in parallel. gears' task waits for two hops, to the blocking
  worker and back through the reactor's wake-up. The pool (step 5 below) could do the same: a worker is a thread, its
  reactor and its queue, so they can pass to a spare thread while the task blocks on its own.
- The echo server is a plain read/write loop, as gears' is. Writing it as `reads.through(writes)` cost cats-effect a
  further 18% in ping-pong and 34% in `concurrent`.
- Its run on more workers is the comparison to make once gears has a pool too.

## A thread-pooled scheduler

Planned: a pool of worker threads, each owning a reactor and running futures as `ReactorScheduler` does now, which
take runnable tasks from one another. A task's next op goes to the reactor of whichever worker runs it, rather than a
reactor it is pinned to, so a moved task's descriptors follow it; pinning stays for what cannot move, such as
io_uring's ring-bound ops. Taking tasks lives in the scheduler, above the `Reactor` interface, the same for every backend.
Whether moving tasks that are doing I/O costs more than keeping them where they are is to be measured, with a policy
switch: take any task, take only tasks not resumed by I/O, or those too once a worker's backlog persists.

1. [x] **A task resumed on another thread.** `kqueuedemo gears migrate` hands every resume to the other of two threads
   and checks, after each: the locals of the frames the resume restored, an exception thrown after the move and caught
   in one of those frames, one that ends the future, a `@blocking` call and an allocation, and the new thread's own
   alternate signal stack and mask. No failures in 4 million moved resumes in a release-fast build, nor in debug. The
   one thing it found is Scala Native's GC blocking signals process-wide (see the proposed Scala Native changes), which
   happens whether tasks move or not; the check ignores those two signals.
2. [x] **A local run queue that other workers can take tasks from.** `RunQueue`, after Tokio's: a fixed ring of 256
   tasks, first in first out for its owner, from which another thread claims about half with one compare-and-swap on the
   head, which packs two indices so that the owner can go on taking tasks while the other thread copies.
   `ReactorScheduler` uses it in place of its `ArrayDeque`; a task that finds it full goes to the shared queue.
   `kqueuedemo gears runqueue` checks it: an owner pushing and taking tasks while three other threads take tasks from
   it, every task run exactly once and seeing what the owner wrote before queueing it (5 million tasks, ten release-fast
   runs, no failures). With one worker it costs little: release-fast, median of 5 runs, the scenarios within noise
   except `pipelined` +3% and +6.3% over 7 runs (0.64 → 0.68 µs), the scenario that queues the most tasks per message;
   each pop is a compare-and-swap and each push a volatile write, where the `ArrayDeque` had neither
   (`kqueue-demo/bench/results/run-queue.json`, `run-queue-check.json`). Tokio pays the same. Claiming a drain's tasks
   with one compare-and-swap would save most of it, but would also keep other workers from them for the whole drain, a
   trade to measure with the pool.
3. [x] **Ops built by the backend rather than one reactor.** The interface has a `Backend` (kqueue's is
   `KqueueBackend`): it builds the ops, names the handles, keeps the attachments and opens reactors of type
   `Reactor.Of[Op]`, so an op it builds may go to any of its reactors, one submission at a time; ops of another backend
   are still a different type, and a backend is also a `Reactor.Factory`. Kqueue's ops already kept no state of one
   reactor between submissions, so this is mostly types. gears' `perform(b, op)` takes the backend only for the op's
   type and submits to the reactor of the worker running the caller; `OpWaiter` follows the submission, recording its
   scheduler so that a cancel goes to the reactor the op is pending on. Programs started by `ReactorFutures.run` get the
   backend instead of a reactor. Where they submit ops directly with completions of their own (the bench's deadline
   timers and timer stress, the cancel demo), `ReactorFutures.reactor(b)` gives the current worker's reactor, which is
   only theirs until they next suspend: with a pool, such code will need to stay on one worker. No cost: release-fast,
   median of 5 runs, every scenario within 1.4% (`kqueue-demo/bench/results/backend-ops.json`).
4. [x] **Descriptors shared between kqueue reactors.** `DescriptorTable`, process-wide like the descriptors, keeps one
   `Long` per descriptor: a generation that every close bumps (from `DescriptorHooks.closing`, on any thread) and the
   id of the reactor that last registered a read filter for it. Registering any read filter claims the descriptor. A
   reactor brings a descriptor's slot up to date when it submits: it starts over, as for a new descriptor, if the
   generation changed or another reactor has claimed it since. A read event with no read pending, for a descriptor
   another reactor owns now, makes the reactor delete its filter, one `kevent` call per moved descriptor, and forget
   the slot. Write filters need none of this: they are one-shot, and an op cannot move while pending. The thread-local
   close hook could only reach the closing thread's reactor, so before this, a descriptor closed on one thread and
   reused on another left the first reactor sure that its filter was still registered, waiting for ever.
   `kqueuedemo gears share` checks both on two reactor threads sharing a pipe: the old reactor must not wake for data
   the new one owns, and a read of a reused number must complete; without the changes it fails on both. No cost with
   one worker: release-fast, median of 5 runs, every scenario within 3% (`kqueue-demo/bench/results/shared-descriptors.json`).
   Left to measure with the pool: deleting the filter eagerly instead, by message to the old reactor, and `EV_DISPATCH`
   filters, which disable themselves after each event, so that a moved descriptor leaves nothing that can wake anyone.
   - **What a move costs, before the pool.** `kqueuedemo gears move-bench` ping-pongs against an echo server on its
     own reactor thread, driven by completions on two client reactors directly: `stay` on one; `hand-off`, taking
     turns every N round trips with a connection each, so work changes thread but no descriptor moves; and `move`,
     taking turns on one shared connection. Release-fast, median of 5 runs of 100,000 round trips, µs per round trip:
     `stay` 6.32; every 1, `hand-off` 9.18 and `move` 10.17; every 4, 7.65 and 7.87; every 16, 6.83 and 7.06; every
     64, 6.37 and 6.48 (`kqueue-demo/bench/results/move-descriptors.json`). So handing work to another thread costs
     about 2.9 µs, and moving the descriptor with it about 1 µs more (0.99 at every 1, 0.88 per move at every 4; at
     16 and 64 the difference is within the noise). System calls per round trip at every 1, all threads: `stay` 8.6,
     `hand-off` 19.1, `move` 19.9. A move adds one `read`, the new reactor's first try finding nothing yet, and two
     registration changes, the new reactor's filter and the old one's deletion; a hand-off adds the cross-thread
     wake-up and the spinning and blocking waits around it. The wake-up, not the descriptor, is the expensive part
     of moving work, which favours letting tasks move more than expected; fairness and contention need the pool.
     A descriptor a reactor has just taken over could start out registered and not ready, as after `connect`, since
     kqueue reports data already waiting when a filter is added: that would save the wasted `read`.
   - **Done: a descriptor taken over starts out not ready.** `currentSlot` forgets it and marks it `NotReady`, so a
     read registers a filter, claiming the descriptor, before trying anything; a reused number still starts out
     `Unknown`, as a new descriptor does. `move` every 1 makes 2.0 `read`s per round trip instead of 2.92, and each
     move costs about 0.8 µs beyond the hand-off instead of 1.05 (every 1) and 1.56 (every 4), release-fast, median of
     5 runs, the two builds alternating (`kqueue-demo/bench/results/taken-over-not-ready.json`). The bench's scenarios,
     with one reactor, are within noise (within 3.4% over 7 runs where 5 had shown 4%;
     `taken-over-not-ready-bench.json`).
5. [x] **The pool.** `ReactorPool` is the scheduler futures see; each `ReactorWorker` is a thread with its own
   reactor, a `RunQueue` other workers may take from, and an inbox for tasks that must run there (cancels, timers).
   The thread calling `ReactorFutures.run` is worker 0; worker threads hold their worker, so finding it costs
   `Thread.currentThread()` and a type test (a `ThreadLocal`, a map lookup on Scala Native, had cost 5–10% in
   `fairness` and `timers`). Tasks from outside go to a shared queue. A worker out of work takes from the shared
   queue, then half of another's queue, never its last task; an idle worker is woken only when a queue holds more
   than one task and none is searching. Ops submitted directly use `ReactorFutures.submit`, whose handle cancels on
   the right worker. The benchmarks take `--workers` (clients and server alike) and `--policy`; `GEARS_POOL_STATS=1`
   prints per-worker counters. One worker costs nothing against the single-threaded scheduler (within 2%,
   `kqueue-demo/bench/results/pool-one-worker.json`). Two fixes found on the way:
   - A worker runs completions in the turn it blocked in, still marked idle, so offering the work they queued woke
     itself, a wasted `kevent` per resume, and used up the wake-up meant for another worker. It now skips itself.
   - The `fairness` ticker, alone on a worker, took 60–100 µs a tick: that worker's four spin polls before blocking
     each took 10–20 µs, against about 1 µs on busy workers (not GC, thread priority, oversubscription or stale
     filters, each ruled out; why the kernel call is slower there is not established). A turn with a timer already
     due now skips the spin polls: ticks on 2 and 4 workers 72 → 20 and 103 → 31 µs (debug), and `timers` 0.19 →
     0.16 µs on one worker.
6. [ ] **Compare the policies.** First comparison, release-fast, median of 5 runs, µs/op, workers-policy
   (`kqueue-demo/bench/results/pool-policies.json`):

   | scenario | 1-free | 1-affine | 2-free | 2-affine | 2-hybrid | 4-free | 4-affine | 4-hybrid |
   |---|---|---|---|---|---|---|---|---|
   | pingpong | 7.74 | 7.41 | 7.77 | 7.46 | 7.63 | 7.86 | 7.46 | 7.61 |
   | concurrent | 2.38 | 2.14 | 2.76 | 2.84 | 2.51 | 5.92 | 3.40 | 3.92 |
   | connections | 41.9 | 23.5 | 42.5 | 21.1 | 24.2 | 42.7 | 23.1 | 23.6 |
   | pipelined | 0.70 | 0.69 | 0.87 | 0.71 | 0.69 | 0.77 | 0.69 | 0.66 |
   | fairness (tick) | 1.71 (3.42) | 1.84 (3.68) | 0.71 (12.0) | 1.85 (3.70) | 1.78 (3.57) | 0.82 (14.4) | 1.85 (3.71) | 1.78 (3.55) |
   | mixed (compute) | 26.8 (26.8) | 24.0 (24.0) | 9.71 (33.9) | 23.7 (23.7) | 24.2 (24.2) | 8.73 (34.6) | 24.0 (24.0) | 23.6 (23.6) |

   - On one worker, affine is simply better: resumes go to an owner-only `ArrayDeque`, with no compare-and-swap,
     and run first; `connections` −44%, `concurrent` −10%, `mixed` −10%.
   - Free spreads work, which pays where work is independent (`mixed` ping-pong 26.8 → 9.7 with the computation on
     another worker; `fairness` traffic 2.4 times faster), but moves tasks and descriptors constantly (on 2 workers,
     about half of `concurrent`'s round trips involve a task taken), so `concurrent` gets slower with more workers.
   - Affine barely spreads: a burst of new futures starts and suspends on its first I/O before an idle worker wakes,
     and afterwards they are resumes, which stay; and a lone takeable task, like `mixed`'s computation, is never
     taken, since a worker's last task is left to it. Hybrid spreads a little more.
   - None yet makes `concurrent` faster on more workers.
   - **Done: a lone task behind resumes may be taken.** A worker whose one task in `local` waits behind resumes in
     `home` says so, in a volatile flag set when that happens and cleared once a drain has run the resumes, and offers
     it; `hasSpare` is what other workers check. Publishing a resume count after every turn and drain instead, the
     first version, cost `connections` 7–9% and `pingpong` 4% with no task taken, a volatile write per turn
     (`pool-policies-spare-connections.json`). The
     owner keeps its own copy of the flag, so the usual case neither writes nor reads it, which brought both back
     within noise. Release-fast, median of 5 runs, µs/op, `base` the previous commit, `ce` cats-effect with as many
     threads (`kqueue-demo/bench/results/pool-policies-spare.json`; base against the change on the first five
     scenarios, 7 runs, `pool-policies-spare-connections-2.json`):

     | scenario | 1-affine | ce-1 | 2-affine base | 2-free | 2-affine | 2-hybrid | ce-2 | 4-affine base | 4-free | 4-affine | 4-hybrid | ce-4 |
     |---|---|---|---|---|---|---|---|---|---|---|---|---|
     | pingpong | 7.58 | 22.8 | 7.90 | 7.64 | 7.66 | 7.60 | 23.1 | 7.50 | 7.73 | 7.89 | 7.63 | 24.0 |
     | concurrent | 2.16 | 10.0 | 2.69 | 2.76 | 2.47 | 2.52 | 7.49 | 3.48 | 5.48 | 4.04 | 3.85 | 9.80 |
     | connections | 24.0 | 157 | 24.4 | 42.9 | 24.5 | 22.9 | 121 | 23.8 | 42.9 | 22.5 | 24.6 | 118 |
     | pipelined | 0.71 | 2.27 | 0.72 | 0.73 | 0.66 | 0.72 | 2.96 | 0.68 | 0.87 | 0.71 | 0.68 | 2.30 |
     | blocking-io | 15.1 | 24.5 | 14.4 | 14.8 | 14.4 | 14.4 | 28.1 | 14.4 | 14.5 | 14.6 | 14.3 | 24.6 |
     | timers | 0.16 | 1.03 | 0.16 | 0.16 | 0.16 | 0.16 | 0.86 | 0.16 | 0.16 | 0.16 | 0.16 | 0.98 |
     | fairness (tick) | 1.81 (3.62) | 2.27 (4.22) | 1.78 (3.56) | 0.74 (11.3) | 1.74 (3.62) | 1.82 (3.64) | 2.52 (0.50) | 1.88 (3.77) | 0.89 (14.9) | 1.87 (3.75) | 1.83 (3.67) | 3.04 (0.47) |
     | mixed (compute) | 23.8 (23.8) | 1187 (19.5) | 23.4 (23.4) | 8.49 (35.1) | 8.26 (36.0) | 8.52 (35.1) | 21.1 (21.1) | 24.3 (24.3) | 8.98 (34.5) | 8.49 (35.6) | 8.40 (34.4) | 109 (21.2) |

     - `mixed`'s computation now moves to an idle worker under affine and hybrid: the ping-pong 23.4 → 8.3 µs on 2
       workers and 24.3 → 8.5 on 4, a little better than free, which also moves the I/O. Everything else is within
       noise of the base (`concurrent` alternates between about 2.2 and 2.6 µs from run to run in every build).
     - `fairness` does not spread under affine. The first version did (traffic 1.85 → 0.72 µs), since it also
       offered a task queued by the last resume of a drain; but the ticker alone on a worker then ticks in 14 µs
       instead of 3.7, the slow polls of a lone worker again (step 5), so this was left as is.
     - `concurrent` is still slower on more workers under every policy, but not for want of spreading: its 64
       futures do spread under affine (the client's workers take 43 tasks on 2 workers, 152 on 4), and placing new
       futures round-robin on the workers as they are spawned, tried behind a switch, gained nothing (2.31 → 2.45 µs
       on 2 workers, 3.73 → 3.84 on 4) and was dropped. The cause is the kernel; see below. Cats-effect gains from a
       second worker there (10.0 → 7.49 µs), though not from four (9.80), and is slower throughout.
     - Gears is 3–6 times faster on `pingpong`, `connections`, `pipelined` and `timers`, and faster on `blocking-io`.
       Cats-effect keeps `fairness` ticks at 0.5 µs on 2 and 4 workers, its ticker on a thread of its own without the
       slow-poll problem; and with one thread its ping-pong is starved by the computation (1187 µs) and its
       `mixed` on 4 threads is erratic (109). Its `blocking` (0.07 µs, not shown) is not the same measurement as
       gears', so is left out.
   - **Found: macOS serialises descriptor system calls per process.** `concurrent` on 1, 2 and 4 workers spends 0.70,
     1.47 and 7.56 s in the kernel for the same 400,000 round trips, whatever the spin count (0, 4 or 16 polls before
     blocking); sampled, the threads are mostly in `read` and `write` (54% of samples), then `kevent`. Four one-worker
     benches as threads of one process run at 15.5 µs a round trip with 39.5 s in the kernel, whereas as four processes
     they run at 3.4–3.9 µs: one process tops out at about 260,000 round trips a second however its threads share the
     work, below one worker alone (470,000), and separate processes reach about 1.1 million. Plain C shows the same,
     so it is neither gears nor Scala Native: `kqueue-demo/bench/descriptor-contention.c`, each thread or process on
     descriptors of its own that nothing else touches, µs per call (macOS 27.0.1, `xnu-13432.1.9`, M1 Pro, 10
     cores):

     | call | 1 | 2 threads | 4 threads | 8 threads | 2 processes | 4 processes | 8 processes |
     |---|---|---|---|---|---|---|---|
     | `getppid`, no descriptor | 0.16 | 0.13 | 0.18 | 0.25 | 0.13 | 0.13 | 0.14 |
     | `fstat` of its own descriptor | 0.23 | 0.51 | 2.69 | 6.51 | 0.24 | 0.24 | 0.41 |
     | 1-byte `write` and `read` on its own socket pair | 0.61 | 0.94 | 5.33 | 14.8 | 0.62 | 0.64 | 0.81 |

     Non-blocking 64-byte ping-pong over socket pairs, busy-polling: 510k, 297k and 185k round trips a second in all
     on 1, 2 and 4 pairs of threads, against 510k, 835k and 1.36M as processes. The calls do not just queue: 8
     threads make 1.2 million `fstat`s a second between them, where one thread alone makes 4.3 million.
   - **Why: one lock per process, taken twice by every call on a descriptor.** In XNU's source (the latest
     published, `xnu-12377`, macOS 26; the running `xnu-13432` is not published yet, but behaves the same):
     - `fp_lookup` and `fp_get_ftype` (`bsd/kern/kern_descrip.c`), which turn a descriptor into its file and take a
       reference to it, hold `proc_fdlock_spin(p)`, which is `lck_mtx_lock_spin(&p->p_fd.fd_lock)`: one mutex per
       process over the whole descriptor table. `fp_drop`, which gives the reference back when the call ends, takes
       it again.
     - `read` and `write` go through `preparefileread` and its write counterpart (`bsd/kern/sys_generic.c`), which
       take that lock and call `fp_lookup` under it; `kevent` finds its kqueue with `kevent_get_kqfile`, which calls
       `fp_get_ftype` (`bsd/kern/kern_event.c`), and registering a filter looks up its descriptor with `fp_lookup`
       too. So a round trip's `write`, `read` and `kevent` take the same lock six times, more with registrations.
     - Nothing in that code finds a file without the lock, and there is no sysctl or flag for it. Linux finds files
       without a lock shared by the process (RCU over the descriptor table, and a cheaper path still while a process
       has one thread), and FreeBSD with `fget_unlocked`, so a pool there would not meet this.
     - **FreeBSD fixed it in 2009.** Jeff Roberson's commit `bf422e5f27` (14 May 2009, "Implement a lockless file
       descriptor lookup algorithm in fget_unlocked()", in FreeBSD 8.0) made lookups lock-free; Mateusz Guzik
       refined it since (2014, retrying only the reference count; 2020, `e10f063b30`, "fd: streamline
       fget_unlocked"). Today's `fget_unlocked_flags` (`sys/kern/kern_descrip.c`) reads the file from the table
       without a lock, takes a reference with an atomic increment of that file's own count
       (`refcount_acquire_if_not_zero(&fp->f_count)`), then checks with the entry's sequence counter that the entry
       did not change meanwhile, falling back to a slower path if it did. Old tables are kept until the whole table is
       freed and files' memory is never returned, so a stale pointer is safe to follow. Threads therefore contend only
       on the same file, never on separate sockets; `fget_only_user` skips even the atomic when nothing else can
       change the table, as in a process of one thread. Linux did the same with RCU in 2005. XNU's `fp_lookup` is the
       pattern FreeBSD left: lock the table, look up, count the reference under the lock, lock again to drop it. XNU
       already uses lock-free lookups (SMR) for Mach port names, just not for descriptors.
     - **But FreeBSD's `sigprocmask` takes a per-process lock on every call.** `kern_sigprocmask`
       (`sys/kern/kern_sig.c`) takes `PROC_LOCK(p)` even to read the mask, though it changes only the calling
       thread's (`td->td_sigmask`), where XNU changes every thread's but reads without a lock. FreeBSD's libc
       `setjmp` saves the mask and `longjmp` restores it, so there both halves of every continuation resume would take
       the process lock: from the source, not measured. The `_setjmp` fix proposed to Scala Native matters there as
       much as on macOS; our `setjmp_override.S` is for macOS on arm64 only and would need a FreeBSD version.
     - Kqueues without a descriptor, the workloops of `kevent_id` that libdispatch uses, are looked up under another
       lock, `fd_kqhashlock`, also one per process, so they would only take `kevent`'s own lookup off `fd_lock`, not
       those of `read` and `write`; and they are a private interface, made for libdispatch's threads. Not pursued.
     - Searching found no current account of this for macOS; older notes say `fp_lookup` takes `proc_fdlock`, and
       there are anecdotal reports of multithreaded socket I/O on macOS behaving badly. The source and these
       measurements are the evidence.
   - **A second per-process lock: setting the signal mask.** `sigprocmask` that sets a mask (`kern_sig.c`:
     `block_procsigmask`, `unblock_procsigmask`, `set_procsigmask`) takes `proc_lock(p)` and rewrites the mask of
     every thread in the process, not only the caller's; reading the mask takes no lock. libc's `longjmp` sets the
     mask it saved, so each `setjmp`/`longjmp` pair contends on that lock, and every continuation resume in Scala
     Native makes one (the first proposed Scala Native change below): about 1.9 per round trip in `concurrent`, as
     many as its descriptor calls. This is also why the masks seen in step 1 changed on threads that had not touched
     them. `descriptor-contention.c`, µs per call:

     | call | 1 | 2 threads | 4 threads | 8 threads | 8 processes |
     |---|---|---|---|---|---|
     | `sigprocmask`, reading the mask | 0.16 | 0.17 | 0.17 | 0.29 | 0.16 |
     | `sigprocmask(SIG_SETMASK)`, to the same mask | 0.16 | 0.27 | 1.63 | 7.53 | 0.22 |
     | `setjmp` then `longjmp` | 0.59 | 0.78 | 1.54 | 7.17 | 0.65 |
     | `_setjmp` then `_longjmp` | 0.022 | 0.021 | 0.024 | 0.024 | 0.029 |

   - **Done: `setjmp` and `longjmp` overridden in the demo's binary.**
     `kqueue-demo/src/main/resources/scala-native/setjmp_override.S` defines `setjmp` and `longjmp` as branches to
     `_setjmp` and `_longjmp`; references within the binary bind to them before libc's, so Scala Native's
     continuation resume, its exceptions and the GC's register capture all skip the mask and the alternate stack.
     This is the workaround that the first proposed Scala Native change said we did not have. Release-fast, median of
     5 runs, µs/op, affine, without → with it (`kqueue-demo/bench/results/pool-setjmp-override.json`):

     | scenario | 1 worker | 2 workers | 4 workers |
     |---|---|---|---|
     | pingpong | 7.39 → 6.72 (−9%) | 7.44 → 6.79 (−9%) | 7.37 → 6.77 (−8%) |
     | concurrent | 2.15 → 1.84 (−14%) | 2.32 → 2.10 (−9%) | 3.73 → 3.75 |
     | connections | 23.5 → 22.3 (−5%) | 24.2 → 22.7 (−6%) | 24.1 → 22.9 (−5%) |
     | pipelined | 0.67 → 0.69 | 0.65 → 0.63 | 0.67 → 0.62 (−7%) |
     | blocking | 5.58 → 5.09 (−9%) | 6.20 → 5.40 (−13%) | 6.24 → 5.47 (−12%) |
     | blocking-io | 14.1 → 13.0 (−8%) | 14.1 → 13.0 (−8%) | 14.0 → 13.1 (−7%) |
     | fairness (tick) | 1.72 → 1.38 (3.43 → 2.75) | 1.73 → 1.41 (3.46 → 2.81) | 1.75 → 1.47 (3.51 → 2.95) |
     | mixed | 23.2 → 22.7 | 8.21 → 7.65 (−7%) | 8.23 → 7.68 (−7%) |

     `timers` is unchanged. `concurrent` on 4 workers does not move, being held by the descriptor lock. The pool's
     checks (`migrate`, which throws after moves and checks signal masks, `runqueue`, `share`) and
     `kqueueDemoTests/testFull` pass with it. It changes what `setjmp` means for all code in the binary: anything
     relying on `longjmp` restoring the signal mask would break, which nothing in Scala Native's runtime or the demo
     does. The fix belongs in Scala Native (`_setjmp` in `delimcc.c` and `RegistersCapture.h`, `_longjmp` in `eh.c`
     and `eh.cpp`); until then, the override is the workaround.
   - **Linux does not have either lock.** The same program on Linux 7.0.14 (Docker Desktop's VM on the same M1 Pro,
     10 virtual CPUs, so virtualised, and the host under load about 7): threads and processes cost the same in every
     mode. µs per call, 1 → 8 threads: `fstat` 0.21 → 0.25 (macOS 0.23 → 6.5), `write` and `read` 0.73 → 0.90,
     setting the signal mask 0.16 → 0.17; `setjmp` and `longjmp` 0.043, the same as `_setjmp`, since glibc's leave
     the mask alone. Ping-pong over socket pairs, median of 3 runs, µs a round trip and round trips a second in all,
     on 1, 2 and 4 pairs:

     | mode | 1 pair | 2 pairs, threads | 4 pairs, threads | 2 pairs, processes | 4 pairs, processes |
     |---|---|---|---|---|---|
     | busy `read` | 1.77 (563k) | 2.35 (851k) | 2.51 (1.59M) | 2.28 (876k) | 3.28 (1.22M) |
     | epoll, edge-triggered | 2.48 (404k) | 3.73 (536k) | 4.39 (911k) | 3.53 (566k) | 4.89 (818k) |
     | io_uring | 2.41 (415k) | 3.14 (637k) | 4.90 (816k) | 3.00 (668k) | 4.34 (921k) |
     | io_uring, registered descriptors | 2.32 (432k) | 2.92 (685k) | 4.29 (933k) | 2.74 (730k) | 3.96 (1.01M) |

     The time per round trip grows with pairs for processes as much as for threads, so on Linux it is the shared
     machine (8 threads spinning on 10 virtual CPUs), not a lock. io_uring, submitting each side's write with its
     next read in one `io_uring_enter` and reaping with deferred task work, is level with epoll, and 5–10% ahead
     with its descriptors and ring registered. A first version, one `io_uring_enter` per read and per write and
     spinning on more to reap, was 2–4 µs slower a round trip than epoll; how io_uring is driven matters more than
     using it.
   - **On macOS, spinning on `kevent` is costly to the rest of the process.** The same ping-pong with a kqueue per
     thread, polled without blocking, then `read`, as gears' reactor does but spinning without limit: 5.93 µs a
     round trip on one pair (busy `read`, 1.87; Linux's epoll, 2.48), 7.86 and 24.6 on 2 and 4 pairs of threads,
     and even as processes only 7.67 and 18.6, each process's two threads spinning on its one descriptor lock
     (median of 3 runs, host under load about 7). Gears spins 4 polls and then blocks. Re-running the trial of spin
     counts (0, 1, 2, 4, 8) with the `setjmp` override on 1 and 2 workers was too noisy to use, the machine's load
     rising from 3.7 to 13.8 during it (`kqueue-demo/bench/results/spin-polls-retrial.json`): only 0 polls stood
     out, `connections` 30% slower; to repeat on a quiet machine.
   - **Nothing else in XNU carries socket I/O without the lock.** From the same source, what a reactor could be built
     on instead:

     | mechanism | per-process lock | usable by a reactor |
     |---|---|---|
     | `read`, `write`, `readv`, `sendmsg` on sockets | `fd_lock`, twice a call | unavoidable for socket data |
     | `sendmsg_x`, `recvmsg_x` (private) | `fd_lock`, once a call | batch within one socket only, and truly only for connected datagram sockets; on a stream socket they loop over the messages; no help at one message a round trip |
     | `kevent` on a kqueue | `fd_lock`, to find the kqueue | what the reactor does now |
     | workloops, `kevent_id` | none for the thread the kernel has bound to the workloop; otherwise a hash lookup under `fd_kqhashlock`, also per process | only that bound thread may poll (others get `EXDEV`); the kernel's workqueue creates and binds such threads, libdispatch's model, through private interfaces |
     | the workqueue's kqueue, `KEVENT_FLAG_WORKQ` | none, a plain pointer | registration only: polling it returns `EINVAL`, events going to the workqueue's threads |
     | Mach ports, `mach_msg` | none per task: rights are looked up under SMR (`ipc_right_lookup_read`), with locks per port | carry no socket data; could wake workers, and a kqueue can watch one (`EVFILT_MACHPORT`) |
     | `os_sync_wait_on_address` (ulock) | none: a global hash with a lock per bucket | waits without a descriptor, but a thread waiting there cannot also wait for I/O |
     | Skywalk channels | `__channel_sync` finds its channel with `fp_get_ftype`, `fd_lock`, but once per batch of packets | shared-memory rings that could spread the lock over many packets; private, and where the entitlement is checked was not found |

     So socket data always pays the lock, once a call at least, and no call does I/O on more than one socket. What
     can avoid it is around the I/O: polling, by letting the kernel's workqueue drive bound workloop threads (that is,
     libdispatch, whose `read` and `write` still pay), and wake-ups between workers. Sources: `bsd/kern/kern_event.c`
     (`kevent_id`, `kevent_get_kqwq`, `kqworkloop_get_or_create`), `bsd/kern/uipc_syscalls.c`, `osfmk/ipc/ipc_right.c`,
     `bsd/kern/sys_ulock.c`, `bsd/skywalk/channel/channel_syscalls.c`.
   - **Waking workers by Mach port gains nothing.** `kqueue-demo/bench/wakeup-contention.c` ping-pongs a wake-up
     between the two threads of each of N pairs, as threads of one process or a process per pair: by `EVFILT_USER`
     triggered with `kevent`, as the pool wakes workers now (`user`); by a message to a Mach port that the waiter's
     kqueue watches (`port`); by `mach_msg` alone, no kqueue (`mach`); and by `os_sync_wait_on_address` (`ulock`);
     each blocking, or polling (`-poll`). Median of 3 runs, µs a round trip (the load rising from 2.8 to 12 during
     them; 8 pairs, 16 threads on 10 cores, oversubscribe the polling modes):

     | mode | 1 pair | 2 threads | 4 threads | 8 threads | 2 processes | 4 processes | 8 processes |
     |---|---|---|---|---|---|---|---|
     | user | 3.00 | 5.74 | 16.4 | 30.5 | 5.55 | 15.1 | 28.1 |
     | user-poll | 3.87 | 6.52 | 17.7 | 50.2 | 6.06 | 17.4 | 28.0 |
     | port | 4.01 | 6.43 | 17.1 | 27.2 | 6.27 | 16.9 | 28.5 |
     | port-poll | 5.55 | 7.66 | 18.1 | 31.0 | 7.04 | 18.1 | 30.1 |
     | mach | 3.01 | 6.55 | 15.4 | 27.9 | 5.51 | 16.2 | 27.6 |
     | mach-poll | 1.32 | 2.41 | 5.28 | 13.8 | 1.95 | 2.61 | 7.45 |
     | ulock | 2.78 | 5.52 | 14.6 | 27.4 | 5.32 | 14.6 | 27.0 |

     Blocking wake-ups cost the same whatever carries them, and as much between processes as between threads: what
     limits them is system-wide, presumably the scheduler's wake-up path, not a lock per process. A Mach port watched
     by the kqueue is slower than `EVFILT_USER` (4.01 against 3.00 µs on one pair), so the pool's wake-ups stay as
     they are. Only polling tells the mechanisms apart: polling `mach_msg` is three times cheaper than polling
     `kevent` (1.32 against 3.87 µs on one pair), and the per-process lock shows in polled `kevent` on 8 threads
     (50.2 against 28.0 µs as processes). But a reactor must poll its kqueue for sockets anyway.
   - **SwiftNIO meets the same lock; Apple's way round it is Network.framework.** SwiftNIO's POSIX transport
     (`NIOPosix`, `SelectorKqueue.swift`) is built as this pool is: a `MultiThreadedEventLoopGroup` of threads, each
     with a kqueue it polls with `kevent`, woken by an `EVFILT_USER` filter triggered with `NOTE_TRIGGER`; so its
     threads share the descriptor lock as ours do. On Apple platforms SwiftNIO offers NIOTransportServices instead,
     Network.framework for the connections and Dispatch for the threads. Network.framework uses Apple's user-space
     networking stack, over Skywalk, whose shared-memory channels need a system call per batch of packets rather
     than per message (`__channel_sync`, which takes the descriptor lock once for the batch); Skywalk is undocumented
     and left out of XNU's published source. Apple introduced the stack on iOS and tvOS (WWDC 2018, "Introducing
     Network.framework"); according to Apple's developer forums it reached macOS in macOS 12, is used by default, and
     falls back to the kernel's sockets in some cases (a firewall being on is said to be one, unconfirmed). That is the
     closest to a fix Apple has: not a lock-free lookup, but fewer system calls by keeping TCP and UDP out of the
     kernel.
   - **Measured: over loopback, Network.framework uses the kernel's sockets, and is slower and scales worse.**
     `kqueue-demo/bench/network-framework.c`, through its C API: an `nw_listener_t` and N `nw_connection_t`s
     ping-ponging 64-byte messages over loopback TCP, each connection, either side, on a serial dispatch queue of its
     own, as one process or a process per connection; against BSD sockets with a client and an echo thread per
     connection, blocking in `read` (busy-polling both ends of a connection in one process was slower still, at
     64 µs a round trip, their two threads contending for the descriptor lock). Sampled, Network.framework's threads
     are in `sendmsg` and `recvmsg`, its events come through libdispatch's workloops (`kevent_id`), and there is no
     `__channel_sync`: no user-space stack for loopback. Median of 3 runs, µs a round trip per connection (round
     trips a second in all), the load about 8 throughout:

     | | 1 | 2 threads | 4 threads | 8 threads | 2 processes | 4 processes | 8 processes |
     |---|---|---|---|---|---|---|---|
     | BSD sockets, blocking | 27.1 (37k) | 31.2 (64k) | 50.8 (79k) | 71.0 (113k) | 31.6 (63k) | 50.7 (79k) | 72.0 (111k) |
     | Network.framework | 59.9 (17k) | 96.0 (21k) | 151 (26k) | 338 (24k) | 75.2 (27k) | 82.0 (49k) | 108 (74k) |

     Loopback TCP is slow here anyway (gears' own TCP `pingpong` is 30 µs a round trip, against 7 over Unix
     sockets). Blocking BSD sockets cost the same in one process as in several, as blocking wake-ups do: what limits
     them is the TCP path and waking threads, system-wide. Network.framework is twice as slow on one connection and,
     in one process, stops at about 25,000 round trips a second however many connections, where separate processes
     reach 74,000: something of its own, or of Dispatch's, is serialised per process. Whether it uses the user-space
     stack over a real interface, and does better there, needs a second machine. As a backend, it would not help.
   - **A design per OS, not per backend.** macOS and FreeBSD share kqueue, and so `KqueueBackend`, but not these
     limits. On FreeBSD, as on Linux, a pool of reactor threads in one process should scale I/O, and the take
     policies matter again; on macOS it cannot, and I/O scales only across processes. So what the pool is for, its
     default size and whether to shard across processes depend on the OS, not on the backend: the kqueue backend
     needs to know which it runs on. Both need the signal mask out of continuation resume, macOS because `longjmp`
     takes its process lock, FreeBSD because `setjmp` and `longjmp` both take its. To confirm by measuring on
     FreeBSD, with `descriptor-contention.c` (its `kqueue` mode builds there too) and the pool.
   - **What this changes.** On macOS, a pool of reactors in one process cannot scale I/O beyond about one busy thread,
     and each extra thread making descriptor calls slows all the others: a worker spinning on `kevent` with nothing
     to do slows the busy ones, and is itself slowed by them, which may be the lone worker's slow polls (step 5).
     Cats-effect is under the same limit. The pool helps computation on macOS, as in `mixed`, not I/O. What would
     help, in order:
     - Several processes, each with its own pool, sharing the listening socket, inherited across `fork` or passed
       over a Unix socket: the only way measured to scale (510k → 1.36M round trips a second on 1 → 4 processes).
       Futures cannot move between them, so it is sharding, not one pool. An architectural choice for later.
       Prior art, searched for: well-known servers are multi-process (nginx, a single-threaded event loop per worker
       process; Apache's prefork; PostgreSQL, a process per connection; PHP-FPM; Node's `cluster`; Gunicorn and Puma
       clustered), but for isolation, robustness or a runtime's global lock, and with Linux in production, where
       threads scale; none says it does so for macOS. nginx does scale with worker processes on a Mac (an informal,
       noisy laptop test: about 13,600 → 20,100 requests a second on 1 → 8 workers, flattening after 4,
       https://www.peterbe.com/plog/experimenting-with-nginx-worker_processes). The threaded runtimes used on macOS,
       Tokio, SwiftNIO (an event loop and kqueue per thread, as here), Go's and cats-effect, are threads of one
       process and document no workaround, macOS being mostly where they are developed rather than deployed.
       Nothing found describes `proc_fdlock` limiting throughput, only its correctness (an exploit write-up,
       https://www.exploit-db.com/exploits/41794). Linux had the same lock over a process's descriptor table and
       made lookups lock-free with RCU in 2005 (https://lkml.iu.edu/0506.1/1621.html), which is why its threads
       scale where XNU's do not.
     - Fewer descriptor calls per op, each now saving two takes of the lock for every thread: registrations batched
       into one `kevent`, no wasted `read`, `readv` and `writev` where several messages wait. Worth it with one worker
       too.
     - Fewer threads making them: I/O on one or two workers and computation on the rest, and idle workers blocking at
       once rather than spinning on `kevent`.
       **Measured, and reverted: I/O on one worker, computation on the rest.** An experiment, not kept in the pool:
       `ReactorFutures.run(..., ioWorkers = n)` (bench `--io-workers n`, with the affine or hybrid policy) kept ops
       to the first `n` workers. A task on another worker
       that performed an op or armed a timer first moved to one of them: it suspended, and that worker resumed it
       from its inbox, run as though from its reactor's turn, so the resume stayed there under the affine policy.
       The other workers ran new futures and yields they took, and made no non-blocking `kevent` turn between
       rounds, only the blocking one when idle. Two scenarios were added, and kept, `concurrent-compute`, as `concurrent` with
       2,000 steps of computation (about 4 µs) after each round trip in the connection's own future, and
       `concurrent-offload`, the same steps in a future of its own, awaited. Release-fast, median of 5 runs, µs/op,
       client and server each a pool of that size, the same build throughout
       (`kqueue-demo/bench/results/pool-io-workers.json`):

       | scenario | 1 | 2-affine | 2-io1 | 4-affine | 4-io1 |
       |---|---|---|---|---|---|
       | pingpong | 6.97 | 6.86 | 6.85 | 6.86 | 6.80 |
       | concurrent | 1.84 | 2.05 | 1.84 | 3.78 | 1.86 |
       | concurrent-compute | 6.28 | 3.89 | 6.27 | 3.91 | 6.32 |
       | concurrent-offload | 7.32 | 4.65 | 4.56 | 4.05 | 5.42 |
       | connections | 40.1 | 22.8 | 22.0 | 22.4 | 22.4 |
       | pipelined | 0.67 | 0.66 | 0.67 | 0.67 | 0.63 |
       | blocking-io | 13.1 | 13.0 | 13.2 | 13.0 | 13.3 |
       | fairness (tick) | 1.38 (2.76) | 1.40 (2.80) | 1.41 (2.83) | 1.44 (2.88) | 1.43 (2.85) |
       | mixed (compute) | 26.6 (26.6) | 7.60 (35.6) | 7.23 (20.6) | 7.76 (35.6) | 7.23 (20.6) |

       - Pure I/O costs what one worker does, however many workers there are: `concurrent` 1.86 µs on 4 workers
         against 3.78 with all four doing I/O.
       - Computation runs faster beside it: `mixed`'s slices take 20.6 µs on a worker doing no I/O, against 35.6 on
         one making `kevent` turns, and its ping-pong is a little faster too (7.23 against 7.6–7.8).
       - But computation inside an I/O task stays on the I/O worker: `concurrent-compute` is the one-worker number,
         where all-I/O pools spread it with the connections (3.9). Code must put it in a future of its own to spread
         it, and then each round trip costs a move back: the future's completion resumes the connection's task on the
         computing worker, whose next op moves it to the I/O worker again, with a wake-up. That is even with all-I/O
         pools on 2 workers (4.56 against 4.65), behind on 4 (5.42 against 4.05).
       - `connections` on one worker is 40 µs today in this build and in the previous one alike (42.2 in both,
         rechecked), against 23–24 in earlier runs; with two workers or more it is 22. Not from this change.
       - The default, every worker doing I/O, was unchanged by running the inbox as from the turn: within noise of
         the previous build on 4 affine workers.
       - So on macOS, one I/O worker is the better default for I/O and for computation in futures of its own; it loses
         where a connection's own task computes. Left to try: resuming a task whose awaited future finished on a
         computing worker back on the I/O worker it last ran on, rather than where the future finished, which would
         remove the move back in `concurrent-offload`; and two I/O workers a pool (the bench's client and server
         pools already make two I/O threads in the process with one each).
       **Measured, and reverted: a dedicated I/O worker that runs no tasks.** Also an experiment, not kept:
       `ReactorFutures.run(..., dedicatedIo = true)` (bench `--dedicated-io`): the first worker only submitted ops,
       ran their completions and handled cancels; every `perform` on another worker handed its op over (the op's
       waiter, with an atomic state, posted to the I/O worker's inbox) and suspended; the completion, on the I/O
       worker, queued the resume on the pool's shared queue, which the other workers took from, and from one
       another. `1+n` is the I/O worker and `n` others; same
       build as the other columns, median of 5 runs, µs/op (`kqueue-demo/bench/results/pool-dedicated-io.json`):

       | scenario | 1 | 4-affine | 4-io1 | 1+1 | 1+2 | 1+4 |
       |---|---|---|---|---|---|---|
       | pingpong | 6.96 | 6.99 | 7.03 | 21.9 | 22.0 | 21.8 |
       | concurrent | 1.96 | 3.69 | 1.88 | 3.42 | 4.92 | 7.48 |
       | concurrent-compute | 6.29 | 3.94 | 6.30 | 7.32 | 4.98 | 6.25 |
       | concurrent-offload | 7.29 | 4.03 | 5.37 | 7.88 | 5.02 | 6.09 |
       | connections | 39.7 | 19.9 | 19.9 | 54.0 | 56.5 | 57.2 |
       | pipelined | 0.70 | 0.64 | 0.65 | 8.39 | 9.63 | 10.0 |
       | blocking-io | 13.3 | 13.2 | 13.4 | 36.1 | 35.8 | 36.2 |
       | fairness (tick) | 1.41 (2.82) | 1.44 (2.88) | 1.46 (2.92) | 8.25 (8.31) | 10.2 (10.8) | 12.2 (13.1) |
       | mixed (compute) | 26.8 (26.8) | 7.82 (34.0) | 7.35 (20.6) | 89.6 (22.5) | 30.9 (21.8) | 31.4 (21.8) |

       - Every op now crosses threads twice, to the I/O worker and back, and the fast path is gone: an op that would
         finish at once still waits for both. Ping-pong triples, pipelined is 13–15 times slower. On 1+1, the pool's
         counters show the computing worker blocking about twice a round trip, and as many wake-ups.
       - `concurrent` gets worse with more workers, as with a kqueue each: the computing workers block and are woken
         in their own kqueues, `kevent` calls under the same per-process lock.
       - Tried, one run each on a loaded machine (load 5–12), so indicative only:
         - Posting to the I/O worker woke it only when it was blocking or about to (a volatile flag, checked after
           queueing), not on every op: no measurable change.
         - The computing workers spinning, without system calls, for work before blocking
           (`GEARS_COMPUTE_SPINS`): ping-pong unchanged on 1+1 (21.5–22.9), worse on 1+4 (45–52), where ten
           threads spin on ten cores.
         - The I/O worker polling `kevent` without blocking for a number of turns before it blocks
           (`GEARS_IO_POLLS`): worse, ping-pong 22 → 35, the client's and the server's I/O workers polling in one
           process and so contending for its descriptor lock.
         - The computing workers parking their threads (`LockSupport`, `GEARS_PARK_COMPUTE`) rather than blocking
           in their kqueues, so that neither blocking nor waking them is a
           descriptor call: on 1+4, `concurrent` 7.33 → 4.76, `concurrent-compute` 6.20 → 4.47,
           `concurrent-offload` 6.22 → 4.76; ping-pong 22 → 25. With I/O on one of 4 workers, `concurrent-offload`
           5.57 → 4.91, the rest unchanged.
       - So handing every op to one thread loses to a kqueue per worker everywhere but the computation's own speed:
         the hand-offs cost more than the descriptor lock they save. This is not Tokio's design either. Tokio's
         multi-threaded runtime has one I/O driver (one epoll or kqueue) but no I/O thread: a task makes its `read`
         or `write` on whichever worker runs it, and only when that would block does it register for readiness
         with the shared driver, which an idle worker polls, waking the tasks that are ready. Nothing crosses threads
         for an op that can finish at once. That design is still to measure here: ops tried on the task's own
         worker, readiness registered in one kqueue shared by the pool (`kevent` is safe from any thread), polled by
         whichever worker is idle.
       **Measured: one kqueue per pool, polled by whichever worker is idle.** An experiment, not committed: the
       pool's workers share one kqueue (`SharedDriver` in the demo) for socket readiness. A task performs its
       command on the worker running it; only if it would block does it wait there, on a persistent edge-triggered
       read filter added once per descriptor, or a one-shot write filter added for that wait. An edge with no task
       waiting is not lost: each event counts a tick per descriptor and direction, and a task that finds the tick
       moved after saying it waits tries again. A worker out of work takes the shared kqueue's lock and blocks in it;
       the others block in their own kqueues, which still have their timers, blocking ops and wake-ups, and which the
       shared kqueue watches, so the worker blocked there hears of those too. Busy workers poll the shared kqueue
       without waiting each round when no one holds it. A woken task is queued on the worker that polled, any
       policy's other workers taking from there (the free policy throughout). Same build as the other columns, median
       of 5 runs, µs/op (`kqueue-demo/bench/results/pool-shared-kqueue.json`):

       | scenario | 1 | 2-affine | 4-affine | shared-1 | shared-2 | shared-4 |
       |---|---|---|---|---|---|---|
       | pingpong | 6.98 | 6.88 | 7.01 | 6.76 | 6.98 | 6.94 |
       | concurrent | 1.87 | 2.13 | 3.67 | 1.71 | 1.90 | 4.40 |
       | concurrent-compute | 6.26 | 3.70 | 3.75 | 6.16 | 3.30 | 4.51 |
       | concurrent-offload | 7.27 | 4.59 | 3.92 | 7.31 | 4.09 | 5.02 |
       | connections | 40.5 | 21.3 | 21.2 | 41.7 | 35.6 | 35.2 |
       | pipelined | 0.67 | 0.65 | 0.67 | 2.00 | 1.16 | 1.09 |
       | blocking | 5.15 | 5.51 | 5.63 | 5.66 | 5.88 | 6.16 |
       | blocking-io | 13.2 | 13.3 | 13.4 | 13.8 | 13.8 | 13.6 |
       | fairness (tick) | 1.39 (2.78) | 1.43 (2.85) | 1.48 (2.97) | 10.5 (21.1) | 0.88 (17.7) | 1.02 (18.0) |
       | mixed (compute) | 26.9 (26.9) | 7.73 (35.8) | 7.93 (34.0) | 28.4 (28.4) | 28.7 (29.1) | 24.4 (27.5) |

       - Ping-pong costs the same as a kqueue per worker: an op that can finish at once does, on the task's own
         thread, and nothing crosses threads for it.
       - `concurrent` is a little faster on 1 and 2 workers (1.71 and 1.90 against 1.87 and 2.13), and
         `concurrent-compute` the fastest of all on 2 (3.30). But it does not scale to 4 either: every worker still
         makes its own `read` and `write`, under the per-process descriptor lock, so sharing readiness does not
         lift the limit, and 4 workers are worse than with a kqueue each (4.40 against 3.67).
       - Worse, as first written: `connections` (35 against 21), `pipelined` (1.1–2.0 against 0.67), the
         `fairness` ticker (18–21 µs against 2.8; its zero-length timers are on the worker's own reactor, turned only
         when it has work ready), and `mixed`, whose computation does not move to another worker (28 against 7.7).
         These are the scheduling around the driver rather than the driver: not tuned further.
       - Getting there took three fixes, each a finding:
         - A persistent edge-triggered write filter fires whenever the peer reads, with nothing waiting, so the
           worker blocked in the shared kqueue woke for nothing: ping-pong 40 µs. Writes wait on one-shot filters,
           as the reactor's do.
         - Polling the worker's own kqueue as well as the shared one, without waiting, every round, cost a round
           trip about 13 µs and tripled the involuntary context switches (55,558 against 18,224 for 10,000 round
           trips; ping-pong 38 → 11.8 µs without them, one worker). So the worker turns its own reactor only when it
           has something to run that needs no poll (a command finished at once, a promise resolved, a timer due, all
           seen without a system call: `KqueueReactor.hasReadyWork`), or when the shared kqueue reports its kqueue
           readable.
         - Not polling its own kqueue leaves its wake-up event untaken, and `wakeup` skips the system call while one
           is pending, so a blocking op's result never woke the worker blocked in the shared kqueue. Reported
           readable, a worker turns its reactor twice, since a turn that delivers something skips its poll.
       - So on macOS neither design scales I/O past a worker or two; a shared kqueue matches a kqueue per worker
         where the per-process lock is the limit, and costs more scheduling to get right. It is the design to try
         on Linux and FreeBSD, where `read` and `write` from several threads do scale.
     - Before more work on the take policies, measure the pool's I/O scaling on Linux, where neither lock exists,
       which needs an epoll or io_uring backend; driven as above, io_uring with registered descriptors is the one
       to build.
     - Left as is: four client workers connecting 512 sockets at once fill the server's listen backlog
       (`kern.ipc.somaxconn`, 128) faster than it accepts, and a Unix socket with a full backlog refuses the
       connection; the bench's `--connections` should stay at or under 128 with more than one worker.

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
  (`kqueue-demo/bench/results/patched-setjmp.json`). It matters more with threads: `longjmp`'s `sigprocmask` takes the
  process lock and rewrites every thread's mask, so the calls slow each other down (7.2 µs a pair on 8 threads against
  0.6 on one). Workaround on our side since: overriding `setjmp` and `longjmp` in the binary (see step 6 of the pool),
  5–20% on most scenarios.
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
- **The GC's signal blocking: `pthread_sigmask` instead of `sigprocmask` (deadlocks collections).**
  Problem: on macOS, multithreaded release builds can deadlock in a collection, and debug builds block SIGINT and
  SIGTERM in every thread for longer than a collection. `Synchronizer_acquire` and `Synchronizer_release` (`gc/immix`
  and `gc/commix`, `Synchronizer.c`) call `sigprocmask`, which on macOS changes every thread's mask, and does so without
  synchronising with another thread's return from a signal handler: even with an empty set, it can write back a
  thread's mask as it was inside the handler. With trap-based yieldpoints, the default in release builds, the set is
  empty (`Synchronizer_init` fills it only on the conditional-yieldpoint path), so the calls do nothing but race. A
  thread returning from the safepoint trap handler at that moment keeps SIGBUS blocked; at the next collection its trap
  fault cannot be delivered, it faults forever on the poll, and the GC waits until `SCALANATIVE_GC_SYNC_TIMEOUT_MS`
  aborts. Three threads calling `sched_yield` in a loop beside an allocating one hang 4 runs in 5, on 0.5.12 and the
  nightly; not at all with conditional yieldpoints, nor with the calls redirected to `pthread_sigmask`. A C program
  racing `sigprocmask(SIG_BLOCK/SIG_UNBLOCK, empty)` against handler returns leaves the handler's signal blocked in 552
  of 948k returns, and in none with `pthread_sigmask`. Found by `kqueuedemo gears migrate`, which saw SIGINT and SIGTERM
  blocked after about 1 in 5,000 resumes in a debug build. libc `longjmp` also restores its mask with `sigprocmask`, so
  the continuation resume's `setjmp`, when an exception escapes, resets every thread's mask. Fix: `pthread_sigmask` in
  both synchronizers; the draft issue has the analysis and both repros. Until then a pool of reactor threads, all
  making `@blocking` calls, risks this in release builds; linking with `SCALANATIVE_GC_TRAP_BASED_YIELDPOINTS=0`
  avoids it, and measured faster here anyway (see the safepoint polls above).
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
- [x] **Persistent write filters — measured, and kept one-shot.** Read filters are registered once, persistent and
  edge-triggered (item 11), because reads wait in every round trip. Writes are tried first and nearly always succeed,
  so a write filter is armed only when one would block, one-shot, and is gone once it fires; nothing is left to clean
  up when a descriptor moves to another reactor. A persistent edge-triggered write filter would save that arming, but
  `kqueue-demo/bench/write-events.c` shows what it costs: on a connection in ping-pong, with no write ever waiting, it
  fires once per turn, when the peer reads and frees send-buffer space, before the echo arrives (1.00 per turn over a
  Unix socket pair, 0.99 over TCP on loopback; the same with 16 messages per turn, so once per drain rather than per
  message). In a reactor that is a wake-up or a poll for nothing in every round trip, against a registration saved only
  when a write blocks, which the benchmark's scenarios almost never do.
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
