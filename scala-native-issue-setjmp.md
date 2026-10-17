# On macOS, every continuation resume costs two extra system calls

*Draft issue for [scala-native/scala-native](https://github.com/scala-native/scala-native).*

## Summary

`scalanative_continuation_resume` (`nativelib/src/main/resources/scala-native/delimcc.c`) calls libc `setjmp` on every
resume, to have somewhere to `longjmp` to if an exception escapes the resumed computation. On macOS (and the BSDs),
`setjmp` also saves the signal mask and the alternate signal stack state, each with a system call (`sigprocmask` and
`sigaltstack`), and `longjmp` restores them. `_setjmp`/`_longjmp` behave the same without touching signal state, and
glibc's `setjmp` already behaves like that, so Linux is not affected.

On Apple Silicon this makes a suspend/resume pair about 60% slower than it needs to be (725 → 449 ns below). Anything
built on continuations pays it on every resume; in a direct-style async I/O library (gears) it cost 8–13% of
end-to-end throughput.

## Environment

- macOS 27 (Darwin 27.0.0) on Apple Silicon (arm64), Apple clang 21
- Scala 3.9.0, Scala Native 0.5.12, and the nightly 0.5.13-20261015-c3918a9-SNAPSHOT
- `Mode.releaseFast`, Immix, multithreading enabled

## Reproduction

```scala
import scala.scalanative.runtime.Continuations.*

/** Suspends and resumes a continuation `n` times, and reports the time per suspend and resume. */
object SetjmpRepro {
  def main(args: Array[String]): Unit = {
    val n = args.headOption.fold(2_000_000)(_.toInt)
    val start = System.nanoTime()
    var i = 0
    while i < n do {
      // The boundary returns the continuation, which is then resumed: one suspend and one resume.
      val resume = boundary[(() => Any) | Null] {
        suspend[(() => Any) | Null](k => k)
        null
      }
      if resume != null then resume()
      i += 1
    }
    val nanos = System.nanoTime() - start
    println(f"$n suspend/resume pairs: ${nanos / 1e6}%.0f ms, ${nanos.toDouble / n}%.0f ns each")
  }
}
```

On 0.5.12, release-fast, three runs each of 2,000,000 pairs:

| | ns per suspend/resume pair |
|---|---|
| as released | 725, 735, 724 |
| with the change below | 453, 448, 446 |

## Where the time goes

`sample` on the unpatched loop, top of stack:

```
sigprocmask        444
__sigaltstack      378
_tlv_get_addr      374
lh_longjmp         104
_platform_memset   101
lh_setjmp           74
...
```

442 of the 444 `sigprocmask` samples are under libc `setjmp`; `__sigaltstack` likewise. The only libc `setjmp` on this
path is the one in `scalanative_continuation_resume`:

```c
// delimcc.c, scalanative_continuation_resume (line 773 in 0.5.12, 781 in the nightly above)
    if (setjmp(exception_env) != 0) {
```

and the jumps back to it are in `eh.c`, and in `eh.cpp` for builds with C++ exceptions:

```c
// eh.c (line 300 in 0.5.12, 338 in the nightly above), and eh.cpp (line 66 in 0.5.12)
            longjmp(*env, 1);
```

## Proposed fix

Use `_setjmp`/`_longjmp`, which save and restore only the registers. The signal mask does not need saving here: nothing
between the `setjmp` and the jump back changes it on purpose, and restoring it would hide a change made by the
resumed code. Linux's glibc `setjmp` already does not save it, so this makes macOS and the BSDs behave like Linux.

```diff
--- a/nativelib/src/main/resources/scala-native/delimcc.c
+++ b/nativelib/src/main/resources/scala-native/delimcc.c
-    if (setjmp(exception_env) != 0) {
+    if (_setjmp(exception_env) != 0) {
--- a/nativelib/src/main/resources/scala-native/eh.c
+++ b/nativelib/src/main/resources/scala-native/eh.c
-            longjmp(*env, 1);
+            _longjmp(*env, 1);
--- a/nativelib/src/main/resources/scala-native/eh.cpp
+++ b/nativelib/src/main/resources/scala-native/eh.cpp
-                longjmp(*env, 1);
+                _longjmp(*env, 1);
```

(The measurements below patched `delimcc.c` and `eh.c` only, as the build did not use C++ exceptions.
`_setjmp`/`_longjmp` are not in Windows' CRT, so the change would need guarding there, where continuations are not
supported anyway.)

## Effect on an application

In an async I/O reactor for [gears](https://github.com/lampepfl/gears) (direct-style futures on Scala Native
continuations, kqueue on macOS), where every awaited operation suspends and resumes once, patching the extracted
runtime sources as above changed its benchmark (release-fast, median of 5 runs, µs per operation) as follows:

| scenario | as released | patched |
|---|---|---|
| request/response, one message in flight | 9.24 | 8.47 (−8%) |
| the same with a timeout per round trip | 9.36 | 8.37 (−11%) |
| 64 connections at once | 4.77 | 4.27 (−11%) |
| pipelined writes and reads | 2.75 | 2.39 (−13%) |

## Related: the GC's register capture on non-x86 targets

`gc/immix_commix/RegistersCapture.h` captures registers with inline moves on x86 and x86-64, and falls back to
`setjmp` everywhere else, arm64 included:

```c
#else
#define CAPTURE_SETJMP
#include <setjmp.h>
typedef jmp_buf RegistersBuffer;
#endif

#ifdef CAPTURE_SETJMP
#define RegistersCapture(out) (void)setjmp(out);
```

`MutatorThread_switchState` (`gc/immix/MutatorThread.c`, and the same in Commix) calls it on every switch to
`GC_MutatorThreadState_Unmanaged`, which happens around every `@blocking` extern call. On macOS arm64 that adds the same
two system calls to every `@blocking` call, so posixlib's `read`, `write` or `kevent` cost three system calls each. The
reactor above worked around it by binding the calls that cannot block (on non-blocking descriptors) without
`@blocking`, which gained 10–18%. Using `_setjmp` there too, or capturing x19–x28, the frame pointer and the stack
pointer with inline assembly as on x86-64, would remove the cost for everyone.

## Related: thread-local access on macOS, mostly from safepoint polls

On macOS, every access to a C thread-local (`SN_ThreadLocal`, i.e. `_Thread_local`) is a call to `_tlv_get_addr`,
where Linux uses a register-relative load. In the reactor's throughput benchmarks, after the fixes above,
`_tlv_get_addr` was the largest cost after the I/O system calls themselves: 18–20% of CPU time. Attributing its
samples by caller (`sample`, release-fast, the pipelined scenario):

| share of `_tlv_get_addr` | caller |
|---|---|
| 66% | compiled Scala functions: the safepoint poll in their prologues and loops |
| 11% | `scalanative_currentThread` |
| 10% | the allocator (`currentMutatorThread`) |
| 8% | monitor enter and exit |
| ~5% | continuation handler bookkeeping (`handlers_load`/`handlers_store`), stack overflow guards |

The safepoint polls dominate because, with trap-based yieldpoints (the default in release builds), every poll is a
volatile load of the thread-local `scalanative_GC_yieldpoint_trap` (`codegen/llvm/AbstractCodeGen.scala`, the
`Lower.GCYield` case). The comment there explains why it must be volatile: a continuation may resume on another
carrier thread, so the cell pointer cannot be cached across polls. On Linux that is a `tpidr_el0`/`fs` relative load;
on macOS it is a function call at every poll of every function with more than a few instructions.

Possible directions, for whoever knows the GC's constraints better:

- Stop-the-world stops every mutator, so a single, process-wide trap page would need no thread-local at all. The
  per-thread cell presumably exists for a reason (stopping threads individually, or virtual-thread carriers); if
  that is only needed in some configurations, the global form could be the default elsewhere.
- On Darwin, store the cell pointer where it can be read without `_tlv_get_addr`, for example via the thread's own
  register-addressed storage, as libpthread does for its keys.
- Conditional yieldpoints (`SCALANATIVE_GC_TRAP_BASED_YIELDPOINTS=0`) poll a global through `scalanative_GC_yield`,
  with no thread-local; measured against trap-based yieldpoints below where available.

## Related: the GC's `sigprocmask` calls deadlock collections on macOS

This one is more serious than the rest, and may deserve an issue of its own. On macOS, a multithreaded release build
can stop for good during a collection: the GC waits for a thread that never reaches its safepoint, until
`SCALANATIVE_GC_SYNC_TIMEOUT_MS` aborts the process. A thread that calls a `@blocking` function in a loop makes it
happen within seconds:

```scala
import scala.scalanative.posix.sched.sched_yield

/** Three threads call a blocking function in a loop while the main thread allocates, so the GC collects repeatedly. */
object GcDeadlock {
  def main(args: Array[String]): Unit = {
    val deadline = System.nanoTime() + 5_000_000_000L
    val loop: Runnable = () => while System.nanoTime() < deadline do sched_yield()
    val threads = Seq.fill(3)(new Thread(loop))
    threads.foreach(_.start())
    var sink = 0L
    while System.nanoTime() < deadline do sink += new Array[Byte](1024).length
    threads.foreach(_.join())
    println("finished")
  }
}
```

Release-fast, multithreading, Immix, macOS 27 on Apple Silicon: 4 of 5 runs hang, on 0.5.12 and on the nightly
above, each ending with:

```
[ScalaNative GC|Warning] 1 thread(s) not reaching safepoint:
[ScalaNative GC|Warning]   Thread id=..., stackBottom=..., state=Managed, alive=yes
[ScalaNative GC|Error] FATAL: Timeout after 4.0s waiting for 1 thread(s)
```

Linked with `SCALANATIVE_GC_TRAP_BASED_YIELDPOINTS=0`, no run hangs.

### What happens

The stuck thread sits on a safepoint poll (`ldr xzr, [x8]`, its yieldpoint trap page), spending most of its time in
the kernel. Its signal mask has SIGBUS blocked (read from inside the process with a SIGUSR1 handler sent from a
watchdog thread: `uc_sigmask` = `0x40200`, SIGBUS and SIGCONT). With SIGBUS blocked, the fault on the protected trap
page cannot be delivered to `SafepointTrapHandler`, so the thread returns to the load and faults again, forever.

SIGBUS is blocked while the trap handler runs, as for any handler, and the handler's return (`__sigreturn`) should
restore the interrupted mask. Tracing every mask change per thread (interposing `sigprocmask`, `sigaction` and
`__sigreturn`) shows that it does not, every time:

1. The thread takes a normal safepoint trap. `__sigreturn` is passed a context whose `uc_sigmask` is `0x40000`
   (only SIGCONT, which the GC blocks for its wakeups).
2. At the same moment, the collecting thread calls `sigprocmask(SIG_UNBLOCK, &signalsBlockedDuringGC, NULL)` in
   `Synchronizer_release`. The two calls overlapped by 9 ticks of `mach_absolute_time`.
3. Straight after, the thread's mask reads `0x40200`: SIGBUS is still blocked. It keeps running normally until the
   next collection arms its trap page, and then faults forever.

Two things combine.

- **`sigprocmask` is process-wide on macOS, and races with signal returns.** POSIX leaves `sigprocmask` unspecified
  in a multithreaded process. glibc treats it as `pthread_sigmask`, but on macOS it changes the mask of every thread,
  and does so without synchronising with another thread's `__sigreturn`. Called even with an empty set, it can write
  back a thread's mask as it was inside a handler. This C program shows it:

  ```c
  #include <pthread.h>
  #include <signal.h>
  #include <stdio.h>
  #include <string.h>
  #include <time.h>

  static volatile int running = 1;
  static void handler(int sig) {}

  static void *changer(void *arg) {
      sigset_t empty; sigemptyset(&empty);
      while (running) {
          sigprocmask(SIG_BLOCK, &empty, NULL); /* what the GC does on each collection */
          sigprocmask(SIG_UNBLOCK, &empty, NULL);
      }
      return NULL;
  }

  int main(void) {
      struct sigaction sa; memset(&sa, 0, sizeof sa); sa.sa_handler = handler; sigaction(SIGUSR1, &sa, NULL);
      pthread_t t; pthread_create(&t, NULL, changer, NULL);
      long returns = 0, leftBlocked = 0;
      time_t end = time(NULL) + 3;
      while (time(NULL) < end) {
          pthread_kill(pthread_self(), SIGUSR1); /* the handler runs and returns before this does */
          returns++;
          sigset_t m; pthread_sigmask(SIG_BLOCK, NULL, &m);
          if (sigismember(&m, SIGUSR1)) {
              leftBlocked++;
              sigset_t u; sigemptyset(&u); sigaddset(&u, SIGUSR1); pthread_sigmask(SIG_UNBLOCK, &u, NULL);
          }
      }
      running = 0; pthread_join(t, NULL);
      printf("%ld handler returns, %ld left SIGUSR1 blocked\n", returns, leftBlocked);
      return 0;
  }
  ```

  ```
  947774 handler returns, 552 left SIGUSR1 blocked
  ```

  With `pthread_sigmask` in `changer`, or with `changer` doing nothing, none are left blocked.

- **The GC makes those calls on every collection, for nothing, in release builds.** `Synchronizer_acquire` and
  `Synchronizer_release` (`gc/immix/Synchronizer.c`, and the same in `gc/commix`) call
  `sigprocmask(SIG_BLOCK/SIG_UNBLOCK, &signalsBlockedDuringGC, NULL)`. But `Synchronizer_init` adds SIGINT and
  SIGTERM to `signalsBlockedDuringGC` only on the conditional-yieldpoint path. With trap-based yieldpoints, the
  default in release builds, the set stays empty, so the calls do nothing except race with the threads returning from
  the safepoint trap handler.

With conditional yieldpoints the set does hold SIGINT and SIGTERM, and the same process-wide behaviour blocks them in
every thread, not only the collecting one. Release also resumes the stopped threads before it unblocks, so they run
with the signals blocked for a moment. A gears check reading each thread's mask after every continuation resume saw
them blocked after about 1 in 5,000 resumes while collections ran (a debug build on 0.5.12), and never with a heap
large enough that no collection ran.

### Proposed fix

`pthread_sigmask` instead of `sigprocmask` in both synchronizers. It confines the mask to the collecting thread, as
the comment above `signalsBlockedDuringGC` intends, and cannot touch another thread's mask. Redirecting the program's
`sigprocmask` calls to `pthread_sigmask` (by interposition) took the repro above from 5 hangs in 6 runs to none in 6.
Filling in `signalsBlockedDuringGC` on both paths would also make the blocking do what it says in release builds.

```diff
--- a/nativelib/src/main/resources/scala-native/gc/immix/Synchronizer.c
+++ b/nativelib/src/main/resources/scala-native/gc/immix/Synchronizer.c
-    sigprocmask(SIG_BLOCK, &signalsBlockedDuringGC, NULL);
+    pthread_sigmask(SIG_BLOCK, &signalsBlockedDuringGC, NULL);
...
-    sigprocmask(SIG_UNBLOCK, &signalsBlockedDuringGC, NULL);
+    pthread_sigmask(SIG_UNBLOCK, &signalsBlockedDuringGC, NULL);
```

and the same in `gc/commix/Synchronizer.c`. Other `sigprocmask` calls in the runtime (`SetupYieldPointTrapHandler`
blocks the wakeup signal with it at start-up, before other threads exist) are worth the same change on macOS.

The `setjmp` in `scalanative_continuation_resume` above is exposed in a similar way. When an exception escapes a
resumed continuation, `longjmp` restores the mask saved at the resume, and on macOS that restore is process-wide
too. Using `_setjmp` as proposed avoids it.
