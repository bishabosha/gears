# Deadline timer comparison

Recorded measurements and the unresolved intermittent failure are in [the results report](results/2026-11-14/RESULTS.md).

`ScheduledDeadline` is a higher-level movable timer using only `Scheduler.schedule`, the returned `Cancellable`, and
one passive gears future per wait. Later moves only exchange the deadline; the old callback checks the latest value
and reschedules if needed. Earlier moves cancel and replace the registration. A generation check rejects callbacks
already dispatched when cancelled. The waiting computation stays suspended throughout updates.

The public factory is `AsyncOperations.deadlineTimer(nanoTime)`, alongside `sleep`. Its default returns a
`ScheduledDeadline`; `Io` overrides it with a wrapper around `ops.deadlineTimer`. Both return `DeadlineTimer`, exposing
`deadline`, `moveTo` and `await`. The benchmark selects between these two factories explicitly.

The backend primitive is unchanged. Debug prints were removed from the backend's timer and scheduler paths before
measuring either implementation.

## Run

```sh
sbt --client 'rootJVM/testOnly ScheduledDeadlineBehavior'
sbt --client 'ioKqueue/testOnly gears.async.io.kqueue.ScheduledDeadlineSuite'
sbt --client 'ioKqueueBench/nativeLink'
target/out/native0.5/scala-3.9.0/iokqueuebench/iokqueuebench 200000 9 1001
```

Arguments are moves per measured sample, measured samples, and packets per QUIC connection. The executable uses
Scala Native's `releaseFast` mode with multithreading and the default Immix collector. Run the executable directly
after linking so neither sbt nor compilation competes with measurements.

## What is measured

- **Pending moves:** 1 and 256 resident timers, on pools of 1 and 4 workers. All timers are awaited before timing starts:
  a FIFO sentinel on every worker establishes that their futures have installed their waits. Deadlines start an hour
  ahead. Each move advances or retreats its timer's deadline by one microsecond. All moves stay in the future.
  The timed loop includes array/index bookkeeping and the `moveTo` call; startup, waiting, validation and cancellation
  are outside it. There are two warm-up pairs; implementation order alternates in subsequent measured pairs.
- **QUIC timer workload:** `GearsQuicTimers.runInMemory` uses the existing demo's server ACK-delay/idle state machine,
  with channels carrying packets and acknowledgements. It retains the 16-packet client window, ACK every four packets,
  25 ms maximum ACK delay and 200 ms idle timeout. There are 32 connections, either unpaced or paced at 100 microseconds
  per packet. Both implementations must acknowledge all packets and close every connection. Counters include earlier
  moves, later moves, timer fires and delayed ACKs. One pair is warmed up; up to five measured pairs alternate order.

This branch's kqueue backend has pipes and timers but no UDP support, so this is deliberately **not a network benchmark**.
The networked demo calls the same extracted server state machine and still uses primitive timers by default. The
in-memory clients do not implement the network demo's separate silence watchdog.

`active_ms` ends when every packet has been acknowledged; `total_ms` additionally waits for idle connection closure.
With 1001 packets per connection, each last packet requires a delayed ACK, so active time includes approximately
25 ms of intentional waiting. The 200 ms idle timeout can obscure differences in total time. Use a packet count
divisible by four to remove that final delayed ACK when exploring throughput.

Scheduler registrations select workers round-robin, whereas a primitive timer retains its performing worker. This
placement difference is part of the implementation under test. Heap insertions/rekeys that become the head call
the branch's kqueue wake syscall; cancellation removes entries immediately under the heap lock without waking.

Raw output records every measured sample in CSV rows (`moves` and `quic`), plus median update timings and ratios.
Do not interpret these update-throughput measurements as timer-expiry latency measurements.
