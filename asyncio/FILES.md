# File IO in the reactor: a design note

The reactor will eventually have operations on real files, not only sockets, pipes and FIFOs. This note records what
each backend does with a regular file today, what macOS offers for asynchronous file IO and why it is no better than
the blocker pool, and the shape the file ops should take so that Linux can do better later. Written on 2026-10-22
from the xnu sources (`bsd/kern/kern_aio.c`, `bsd/kern/kern_event.c` on `apple-oss-distributions/xnu` at `main`) and
the machine (Darwin 27.0.0); nothing in it has been built.

## Today: a regular file is "always ready"

`Handles.openFile` documents that a regular file never waits, and every backend acts on that:

- **kqueue** marks the descriptor in `DescriptorTable` as always worth trying, since kqueue reports a file readable only
  while the offset is short of the end, and runs the `read` or `write` on the reactor thread.
- **Network.framework** opens the same descriptor and reads and writes it at submission.
- **NIO** opens a `FileChannel` and reads and writes it at submission.

So a read of a file not in the page cache stalls the loop for the disk's latency, and every op behind it. For small,
warm files that is invisible; for a file-serving workload it is the whole cost. The question is where to run the IO
instead.

## POSIX aio on macOS is a kernel blocker pool, with four threads

`aio_read`, `aio_write` and `aio_fsync` are implemented in `kern_aio.c` as a queue of requests served by kernel threads:

- **Four threads, for the whole system.** `aio_work_thread` takes a request, switches to the submitting process's
  address space, and runs the same read or write path a `pread` or `pwrite` does (`do_aio_read` with `FOF_OFFSET`).
  The count is `kern.aiothreads`, 4 on this machine, shared by every process. Four cold reads anywhere on the system
  hold all four, and every other process's aio queues behind them.
- **Request limits.** `kern.aiomax` is 1024 system-wide and `kern.aioprocmax` 256 per process; past them, submission
  fails with EAGAIN.
- **No cancellation once started.** `aio_cancel` returns `AIO_NOTCANCELED` for a request a worker has taken. The
  blocker pool cannot interrupt a native `pread` either, so this is a tie.
- **Delivery through kqueue works.** `SIGEV_KEVENT` is accepted, with the kqueue descriptor in `sigev_signo`, and
  `EVFILT_AIO` has real filter ops (`aio_filtops`), so completions could land in the reactor's `kevent` wait. That
  is the only attraction, and it does not pay for the rest.

Against `asyncio.Blockers`, which every backend already shares:

| | aio | `Blockers` |
|---|---|---|
| Threads doing the IO | 4 kernel threads, system-wide | as many as the pool grows, per process |
| Per op | a syscall to submit, a kevent, and `aio_return` to collect | a queue handoff and a wake-up each way |
| Cold reads in flight | at most 4 on the machine | bounded by the pool |
| A cached read | a memcpy on a kernel thread, then the round trip back | a memcpy on a blocker thread, then the round trip back |

Both do a synchronous positional read on a thread that is not the reactor's and hand the result back. aio removes the
user-space threads and replaces them with fewer threads nobody controls, plus two syscalls per op. Nothing in it
overlaps IO at the device that the blocker pool does not, and macOS has no io_uring equivalent. The JDK's
`AsynchronousFileChannel` is the same design on Unix: a thread pool running positional reads.

## The ops, as built on 2026-10-22

`FileOps`, the ops of a `FileBackend`, which kqueue, NIO and Network.framework all are:

- `open(path, write, into)`: opens the regular file and writes its handle into a `Handle.Slot`, as `accept` does a
  connection's; anything but a regular file fails the op. Opening is an op, not a `Handles` method, because opening
  does disk I/O for the path lookup as reads do, and some environments can only open asynchronously (Node's `fs.open`
  on its threadpool, a browser's OPFS). The handle number is not allocated before the open: kqueue's handles are the
  descriptors themselves, so there is no number until `open(2)` returns.
- `read(handle, buf, offset)` and `write(handle, buf, offset)`: positional, as `pread` and `pwrite` are, completing
  with at least one byte transferred or the end of the file, as the stream `read` and `write` do. Both `Repeatable`:
  the op's offset advances by the bytes each completion transferred, so submitting it again goes on from there, and
  `while buf.hasRemaining do perform(read)` fills a buffer from a file.
- `sync(handle)`, for `fsync`. `Repeatable`.

The stream `read` and `write` on a file handle keep their meaning, reading and writing at the position the handle
carries, on the reactor thread as before; moving them onto the positional ops is still to do. Cancellation follows
`blocking`: an op not yet started is skipped, one running finishes on its thread and its result is discarded, and
`onCancel` runs once the thread has stopped touching the buffer. The positional ops are the first `Repeatable`
blocking ops, so `BlockingRun` is restarted on each submission, and a task cancelled before it started is now settled
by the pool when it skips it, rather than being cancelled at once, so that a resubmission cannot find it still queued.

## Implementations

- **kqueue, Network.framework, NIO**: each file op is a `BlockingRun` on `Blockers`, doing `open`, `pread`, `pwrite`
  or `fsync` (`FileChannel.open`, `read(buf, position)`, `write(buf, position)` and `force` on the JVM) and publishing
  into the op through the existing `settled` path, as `resolve` does. No new threads, no aio. On Scala Native the
  calls go through posixlib's `@blocking` bindings, so a collection need not wait for the disk.
- **Linux, later**: io_uring's `IORING_OP_OPENAT`, `IORING_OP_READ` and `IORING_OP_WRITE` with an offset are
  asynchronous at the device, with the op's buffer registered or not; the same ops, no thread hop. This is the reason
  the ops are positional: io_uring has no file position to share.

## Two refinements worth more than aio

- **Try a cached read inline.** A `pread` of resident pages is a memcpy that returns at once; sending it to a blocker
  costs two thread wake-ups for nothing. `trySubmit` could run the read on the reactor thread when the data is
  resident, with `mincore` on a mapped range or `fcntl(F_RDADVISE)` having been issued earlier as the signal. A
  cheaper heuristic, running the first read inline under a size cap and falling back to a blocker if it stalled, costs
  one stall each time it guesses wrong, so it needs measuring before it is kept.
- **Readahead.** `fcntl(F_RDADVISE)` on macOS and `posix_fadvise(POSIX_FADV_WILLNEED)` on Linux ask the kernel to
  bring a range in, so the blocker's later read hits the cache. That is how to overlap file IO with the loop on
  macOS. `F_NOCACHE` is the opposite hint, for streaming a file once without evicting the cache.

## Measuring

The bench scenarios to add: reads of a warm file and of a cold one (`purge` drops the page cache on macOS, needs
root), sequential at several block sizes and random at 4 KB, with the number of ops in flight at 1, 8 and 64, on each
backend, against a plain `pread` loop on one thread as the floor. The questions: what the blocker round trip adds to a
warm read, whether inline-first recovers it, and how in-flight depth scales on the blocker pool against the kernel's
own queue depth, which is where io_uring would show its difference on Linux.
