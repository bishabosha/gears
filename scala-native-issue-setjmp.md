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

## Related: the GC blocks SIGINT and SIGTERM in every thread on macOS

`Synchronizer_acquire` and `Synchronizer_release` (`gc/immix/Synchronizer.c`, and the same in `gc/commix`) block
SIGINT and SIGTERM for the duration of a collection, so that a handler cannot allocate while the GC holds its lock:

```c
// Synchronizer_acquire, after taking the lock
    sigprocmask(SIG_BLOCK, &signalsBlockedDuringGC, NULL);
...
// Synchronizer_release, after Synchronizer_ResumeThreads()
    sigprocmask(SIG_UNBLOCK, &signalsBlockedDuringGC, NULL);
```

POSIX leaves `sigprocmask` unspecified in a multithreaded process. glibc treats it as `pthread_sigmask`, but on macOS
it changes the mask of every thread in the process. This C program shows the difference:

```c
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>

static volatile int phase = 0;

static void *other(void *how) {
    sigset_t m;
    while (phase == 0) ;
    pthread_sigmask(SIG_BLOCK, NULL, &m);
    printf("other thread after main's %s: SIGINT blocked = %d\n", (char *)how, sigismember(&m, SIGINT));
    return NULL;
}

int main(void) {
    sigset_t s; sigemptyset(&s); sigaddset(&s, SIGINT);
    for (int i = 0; i < 2; i++) {
        pthread_t t; phase = 0;
        pthread_create(&t, NULL, other, i == 0 ? "sigprocmask" : "pthread_sigmask");
        usleep(10000);
        if (i == 0) sigprocmask(SIG_BLOCK, &s, NULL); else pthread_sigmask(SIG_BLOCK, &s, NULL);
        phase = 1;
        pthread_join(t, NULL);
        if (i == 0) sigprocmask(SIG_UNBLOCK, &s, NULL); else pthread_sigmask(SIG_UNBLOCK, &s, NULL);
    }
    return 0;
}
```

```
other thread after main's sigprocmask: SIGINT blocked = 1
other thread after main's pthread_sigmask: SIGINT blocked = 0
```

So on macOS each collection blocks SIGINT and SIGTERM in every thread, not only the collecting one. Because release
resumes the stopped threads before it unblocks, those threads also run briefly with the signals blocked. A gears
check that reads each thread's mask after every continuation resume saw them blocked after about 1 in 5,000 resumes
while collections ran (a debug build on 0.5.12), and never with a heap large enough that no collection ran.

The visible effects are small: Ctrl-C and SIGTERM wait for the collection to end. It also interacts with the
`setjmp` in `scalanative_continuation_resume` above. When an exception escapes a resumed continuation, `longjmp`
restores the signal mask saved at the resume, which on macOS also applies to every thread. A mask saved during a
collection's window would then leave SIGINT and SIGTERM blocked in the whole process for good. Using `_setjmp` as
proposed avoids that too.

Proposed fix: `pthread_sigmask` instead of `sigprocmask` in both synchronizers, which confines the mask to the
collecting thread as the comment there intends. Arguably the unblock should also come before
`Synchronizer_ResumeThreads`, or both calls should stay on the collecting thread.

```diff
--- a/nativelib/src/main/resources/scala-native/gc/immix/Synchronizer.c
+++ b/nativelib/src/main/resources/scala-native/gc/immix/Synchronizer.c
-    sigprocmask(SIG_BLOCK, &signalsBlockedDuringGC, NULL);
+    pthread_sigmask(SIG_BLOCK, &signalsBlockedDuringGC, NULL);
...
-    sigprocmask(SIG_UNBLOCK, &signalsBlockedDuringGC, NULL);
+    pthread_sigmask(SIG_UNBLOCK, &signalsBlockedDuringGC, NULL);
```

and the same in `gc/commix/Synchronizer.c`.
