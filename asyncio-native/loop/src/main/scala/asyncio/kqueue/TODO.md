# Kqueue reactor: syscall reduction TODO

Most operations pay for a `kevent` call of their own, or a trip through the wake pipe, even when the work could be done
right away on the reactor thread. A gears `perform(r, read)` that finds data already waiting costs about 7 syscalls
today; with items 1–3 below it would cost 1 (just the `read`), and about 2 when the data isn't there yet.

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

- [ ] **1. Don't use the wake pipe for work queued on the reactor thread.**
  `ReactorScheduler.execute` always completes a reactor promise, so every resumed continuation costs a pipe `write`, a
  `kevent` wake, and draining `read`s, even though nearly every `execute` comes from the reactor thread itself.
  - When `execute` is called on the owner thread, add the task to the queue and set a plain "work pending" flag.
  - The loop drains the queue after each batch of events, and polls with a zero timeout while the queue isn't empty.
  - Only `execute` from another thread needs to wake the loop.
  - Saves about 4 syscalls per resumed continuation, which is almost every `perform`.

- [ ] **2. Try the operation before waiting for readiness.**
  Sockets are usually writable, and a request-response or pipelined peer usually has data waiting already. Go and tokio
  both try first.
  - On `submit`, call `perform()` straight away. If it would block, register and wait as now.
  - If it succeeds, put the completion on the ready queue from item 1, rather than calling it inside `submit`, so
    completions still run from the loop and there's no recursion. Depends on item 1.
  - Saves the registering `kevent`, the wait, and the event for a successful attempt.
  - Cost: a wasted `read` when nothing is waiting. Keep it cheap by not retrying right after a would-block on the same
    descriptor.

- [ ] **3. Persistent edge-triggered registration instead of one-shot filters.**
  `EV_ONESHOT` means one `kevent` per submit, and another after each spurious wake-up.
  - Register each descriptor and direction once with `EV_ADD | EV_CLEAR`, at its first submit or when the handle opens.
  - Edges set a per-slot "ready" bit; a submit checks the bit and performs at once, and a would-block clears it.
  - Closing the descriptor removes its filters automatically.
  - A cancel only clears the slot, so it needs no `EV_DELETE`.
  - Saves every registration and re-registration in steady state.

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

- [ ] **6. Cheaper wake-ups from other threads** (for what still needs them after item 1).
  - Coalesce: an atomic "wake pending" flag, so only the first `resolved` since the last drain writes to the pipe.
  - Replace the pipe with `EVFILT_USER`: waking is one `kevent(NOTE_TRIGGER)` call, and with `EV_CLEAR` there is nothing
    to drain. That also removes the pipe's two descriptors and `wakeLock`.
  - If the pipe stays, read it once instead of until `EAGAIN`; with `EV_CLEAR`, any new write triggers another event.

- [ ] **7. Accept with fewer syscalls.**
  - On macOS a socket returned by `accept` inherits `O_NONBLOCK` from the listening socket, so the two `fcntl` calls in
    `NonBlocking.accept` are unnecessary there.
  - Where they are needed, `ioctl(FIONBIO)` sets the mode in one call instead of two; the same applies to
    `PosixSockets.setNonBlocking`.
  - Saves 2 syscalls per accepted connection.

## Related, not syscalls

- [ ] **Read event fields in one call.** `dispatch` reads each event through about five separate
  `scalanative_kevent_get` calls, each with its own `stackalloc`. Fetch all fields at once, or read the struct directly
  (32 bytes on macOS).
- [ ] **SIGPIPE (correctness).** No socket sets `SO_NOSIGPIPE`, and writes don't pass `MSG_NOSIGNAL`, so writing to a peer
  that has closed raises `SIGPIPE` and kills the process. Set `SO_NOSIGPIPE` once per socket, at creation or accept.
