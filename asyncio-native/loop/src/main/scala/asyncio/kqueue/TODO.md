# Kqueue reactor: syscall reduction TODO

Most operations pay for a `kevent` call of their own, or a trip through the wake pipe, even when the work could be done
right away on the reactor thread. A gears `perform(r, read)` that finds data already waiting costs about 7 syscalls
today; with items 1–3 below it would cost 1 (just the `read`), and about 2 when the data isn't there yet.

## Measuring

Measure each item with `kqueuedemo gears bench`: build the executables to compare with `kqueue-demo/bench/build.sh`
(release-fast, against the Scala Native nightly, since 0.5.12's GC can crash multithreaded release builds) and compare
them with `kqueue-demo/bench/compare.py` (see its header). Debug builds overstate the Scala-side cost, so measure
release builds. The scenarios map onto the items below: `pingpong` (items 2, 3 and the resume path),
`pingpong-fresh-ops` (the same, building its ops anew for every message), `pingpong-deadline` (item 5), `timers` (item
5: 1,000 timers pending at a time, half firing and half cancelled), `concurrent` (item 4), `pipelined` (items 2 and 3 on
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

## What each operation costs now

| Operation | Syscalls | Where |
|---|---|---|
| Submit a read, write, accept, or connect | 1 `kevent` to register a one-shot filter | `KqueueReactor.arm` |
| Each pass of the loop | 1 `kevent` to wait | `KqueueReactor.run` |
| Perform the op | 1 `read`/`write`/`accept`/… | `Command.perform` |
| Not ready after all (would-block) | 1 more `kevent` to register again | `KqueueReactor.dispatch` |
| Cancel a command or timer | 1 `kevent` (`EV_DELETE`) | `KqueueReactor.cancel` |
| Submit a timer | 1 `kevent` | `KqueueReactor.submit` |
| Complete a promise or blocker | 1 `write` to the wake pipe, then 2+ `read`s to drain it (the last returns `EAGAIN`) | `KqueueReactor.resolved`, `drainWakePipe` |
| Gears task queued with `ReactorScheduler.execute`, even from the reactor thread | the promise path above: `write` + a `kevent` wake + 2 `read`s | `ReactorScheduler.arm`/`execute` |
| Accept a connection | `accept` + `fcntl(F_GETFL)` + `fcntl(F_SETFL)` | `NonBlocking.accept` |

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

- [ ] **4. Batch remaining changes into the wait call.**
  `kevent` takes a list of changes and an array for events in one call, but the reactor issues a separate call per change.
  - Append `EV_ADD`/`EV_DELETE` entries (and timers, if kernel timers stay) to a pending change list, and pass it to the
    next waiting `kevent`.
  - Errors for individual changes then arrive as `EV_ERROR` events in the event array. Today `run` throws on any
    `EV_ERROR`: dispatch must instead fail the matching op, and ignore `ENOENT`/`EBADF` from deletes on closed
    descriptors.
  - Saves one syscall per change that items 2 and 3 don't remove (cancels, timers, first registrations).

- [ ] **5. Timers in user space.**
  Each timer submit and cancel is a `kevent` call, and gears creates and cancels timers constantly (`sleep`, timeouts that
  lose an `Async.select`).
  - Keep a deadline heap on the reactor thread, and pass the nearest deadline as the wait's timeout
    (`KqueueLoop.pollEventsTimeout` already exists). libuv and tokio work this way.
  - Saves both syscalls per timer.

- [x] **6. Cheaper wake-ups from other threads.**
  Done: the wake pipe is replaced by an `EVFILT_USER` event, so waking is one `kevent(NOTE_TRIGGER)` call with nothing to
  drain, and the reactor no longer holds two pipe descriptors. Wake-ups are merged with an atomic flag, which the loop
  clears when it sees the event, so only the first `wakeup` since then makes the syscall. `wakeLock` remains, so that a
  trigger cannot reach the kqueue's descriptor number after `close` has released it.

- [ ] **7. Accept with fewer syscalls.**
  - On macOS a socket returned by `accept` inherits `O_NONBLOCK` from the listening socket, so the two `fcntl` calls in
    `NonBlocking.accept` are unnecessary there.
  - Where they are needed, `ioctl(FIONBIO)` sets the mode in one call instead of two; the same applies to
    `PosixSockets.setNonBlocking`.
  - Saves 2 syscalls per accepted connection.

- [ ] **8. Poll a few times without blocking before sleeping.**
  Once a turn has nothing to do, the loop blocks in `kevent` straight away. In request/response traffic the reply
  often arrives microseconds later, and sleeping then costs a thread wake-up per round trip. Polling a few times
  without blocking first catches it. A trial on top of item 3 (release-fast, median of 5 runs, µs/op), by number of
  non-blocking polls before blocking:

  | polls | pingpong | pingpong-deadline | concurrent | pipelined |
  |---|---|---|---|---|
  | 0 | 14.5 | 16.2 | 5.5 | 3.0 |
  | 1 | 12.7 | 14.6 | 5.9 | 3.1 |
  | 4 | 12.3 | 12.4 | 5.3 | 2.8 |
  | 16 | 11.6 | 14.3 | 5.8 | 3.1 |
  | 64 | 11.8 | 12.1 | 5.2 | 3.0 |

  The cost is CPU spent polling when nothing arrives. A handful of polls (about 4) gets most of the gain; a
  time-bounded spin would behave more predictably across machines than a fixed count.

## Related, not syscalls

- [ ] **Read event fields in one call.** `dispatch` reads each event through about five separate
  `scalanative_kevent_get` calls, each with its own `stackalloc`. Fetch all fields at once, or read the struct directly
  (32 bytes on macOS).
- [ ] **SIGPIPE (correctness).** No socket sets `SO_NOSIGPIPE`, and writes don't pass `MSG_NOSIGNAL`, so writing to a peer
  that has closed raises `SIGPIPE` and kills the process. Set `SO_NOSIGPIPE` once per socket, at creation or accept.
- [ ] **Move to Scala Native 0.5.13 once it is released.** On 0.5.12, multithreaded release builds of the gears demos
  crash intermittently in `ReactorFutures.perform` just after a continuation resumes: the GC ignored conservative roots
  that point partway into an object, so an object referenced only by such an interior pointer, as optimised code can
  hold across a suspension, was collected while live ([scala-native#5080](https://github.com/scala-native/scala-native/pull/5080),
  the fix for [#5046](https://github.com/scala-native/scala-native/issues/5046)). Debug builds are unaffected. The
  `gears-release-repro` branch reproduces it with gears alone: two threads awaiting through `Async.withGroup`. Until then,
  `SCALA_NATIVE_NIGHTLY=1` builds against a nightly that includes the fix, and `kqueue-demo/bench/build.sh` uses it for
  benchmark executables. When moving, set the plugin version in `project/plugins.sbt` and remove the nightly switch
  there and in `build.sbt`, along with the nightly check in `build.sh`.
