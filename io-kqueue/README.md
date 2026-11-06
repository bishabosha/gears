# Worker and timer ownership

`KqueueScheduler` starts and stops workers and connects `Io.perform` to a timer performance. `KqueueOp` contains
reusable operation descriptions. `TimerWait` contains the completion and cancellation state of one performance.
`Worker` handles cross-thread publication, runnable execution, and parking. `TimerQueue` is an indexed binary heap
accessed exclusively by its worker.

## Timer queue

Only the owner inserts, removes, or changes heap keys. The heap has parallel arrays of task references and primitive
nanosecond deadlines. Comparisons read the deadline array without dereferencing the task or operation. Each task
carries its heap index, so cancellation and rekeying take O(log n), with no scans or heap rebuilds. Heap operations
allocate only when the arrays grow.

Deadline moves update the operation's atomic deadline. Earlier moves request an owner update; later moves leave the
old, earlier heap key in place. Before firing, the owner reads the current deadline again and either completes or
rekeys the task. The performance's timeout remains fixed. Its earlier deadline wins, even if the worker resumes
after both deadlines; an exact tie completes the timer normally.

## Cross-thread commands

Producers CAS-push intrusive nodes onto an inbox stack. The sole consumer atomically detaches and reverses a batch
to obtain FIFO publication order. A runnable needs one node. A timer performance is its own node: an atomic queued
bit coalesces repeated deadline moves and cancellation requests, without allocating command objects.

The consumer saves and clears `next` before clearing the queued bit. Once cleared, a producer may reuse that node
for another update; the consumer never reads its old link again. The consumer then reads the latest cancellation
and deadline state. Updates before that read are observed there; updates racing after the queued bit is cleared
can publish another command. A completed performance is never reset or recycled into a later performance, so a
delayed command cannot complete a reused operation.

Inbox publication uses CAS; the heap needs no concurrent algorithm because it has one owner. This is not a claim
that blocking user code, continuation listeners, memory allocation, or thread parking is lock-free.

## Parking and fairness

The worker publishes its intent to sleep, rechecks the inbox, and only then parks until the next heap deadline.
A producer publishes its command before checking the sleep flag. It unparks the worker only when it changes the
flag from sleeping to awake. A publication before the recheck is visible in the inbox; a publication after the
recheck leaves an unpark permit, including if it precedes the actual park. Spurious wakeups simply repeat the loop.
This handshake follows the [LockSupport parking contract](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/locks/LockSupport.html).

Each turn processes up to 32 commands and 32 expired timers. Neither category can indefinitely exclude the other
while callbacks return. Detaching a batch reverses its linked nodes once; it does not allocate an intermediate
collection.

## Completion and cancellation

A performance has a single CAS slot containing either no subscription, its awaiting listener, or a terminal result.
It does not allocate a listener collection, `Try`, or a success value. Cancellation can resume a caller before its
old subscription is unregistered; the cleanup await may replace that subscription, and identity-based removal
cannot remove the replacement.

Cancellation requests go through the owner. The owner removes the heap entry before publishing completion. The
caller awaits this acknowledgement before releasing the operation for reuse. The normal completion path does not
create an uninterruptible cleanup scope. Workers close after the backend's structured scope has finished.

On the normal timer path, each performance allocates a `TimerWait` and its two atomic cells. There are no separate
waiter, registration wrapper, update closure, queue node, or per-tick batch objects. Gears' continuation/listener
allocations are separate from these scheduler allocations; no throughput or latency benchmark is implied.
