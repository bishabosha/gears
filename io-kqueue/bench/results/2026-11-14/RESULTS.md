# Deadline timer results — 2026-11-14

Branch: `async-io-kqueue-basepoint`, base commit `34591bc1880cf53b90846bac318e3db940bc2498`, with the implementation and benchmark in this change.

These timings were recorded before the subsequent `AsyncOperations.deadlineTimer` factory integration. The benchmark
now selects the same algorithms through that factory and its `Io` override; the tables below have not been rerun for
that API refactor.

Host: macOS arm64 (Darwin 27.0.0), 10 reported logical processors. Scala 3.9.0; Scala Native plugin 0.5.12; `releaseFast`, multithreading enabled, Immix GC, no LTO. No compilation ran concurrently with measurement.

Command, in three separate processes (each completed with exit status 0):

```sh
target/out/native0.5/scala-3.9.0/iokqueuebench/iokqueuebench 200000 9 1001
```

Each move configuration has 27 measured samples, each of 200,000 moves, after two warm-up pairs per process. QUIC configurations have 15 measured samples after one warm-up pair per process. Values below are pooled sample medians; ratios divide those medians. These are observations on this machine, not confidence bounds.

## Pending deadline moves

| Workers | Resident timers | Direction | Primitive ns/move | Scheduled ns/move | Scheduled / primitive |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 1 | earlier | 424.87 | 493.10 | 1.161× |
| 1 | 1 | later | 9.46 | 7.88 | 0.833× |
| 1 | 256 | earlier | 118.55 | 232.60 | 1.962× |
| 1 | 256 | later | 7.77 | 6.53 | 0.840× |
| 4 | 1 | earlier | 651.63 | 1386.94 | 2.128× |
| 4 | 1 | later | 9.43 | 8.94 | 0.947× |
| 4 | 256 | earlier | 197.89 | 357.91 | 1.809× |
| 4 | 256 | later | 7.77 | 6.52 | 0.840× |

Later moves use the same atomic-update strategy in both implementations. The small differences at this scale also include the common loop and validation differences between the implementations; they do not establish a meaningful general speed advantage for either implementation. Earlier moves cost 1.16–2.13× as much through schedule/cancel here. With one resident timer, wake syscalls are frequent; with 256, the primitive reuses/rekeys entries while schedule/cancel removes and allocates replacements. Round-robin scheduler placement is included.

## QUIC server timer state machine

32 connections × 1001 packets; every invocation validated all 32,032 packets and all 32 idle closures. Each measured invocation made 32,064 deadline moves, including 8,032 earlier and 8,032 later moves; the rest were unchanged deadlines. Both implementations produced 64 timer events and 32 delayed ACKs per invocation.

| Workers | Packet interval µs | Primitive active ms | Scheduled active ms | Active ratio | Primitive total ms | Scheduled total ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0 | 51.015 | 57.031 | 1.118× | 227.419 | 232.330 |
| 1 | 100 | 181.037 | 187.233 | 1.034× | 361.126 | 366.925 |
| 4 | 0 | 50.126 | 49.484 | 0.987× | 225.789 | 225.269 |
| 4 | 100 | 155.736 | 156.570 | 1.005× | 331.979 | 333.087 |

Active time ends after every packet is acknowledged. It includes the intentionally delayed final ACK (up to 25 ms). Total time also includes the 200 ms idle timeout. Packets and ACKs travel through channels, using the network demo’s extracted server state machine: this branch does not implement UDP, and these are not UDP throughput results. The network client’s separate silence watchdog is not modeled.

### Variation between processes

Ranges below are the minimum and maximum of the three individual process medians for active time; they are not confidence intervals.

| Workers | Interval µs | Primitive process medians ms | Scheduled process medians ms |
| ---: | ---: | ---: | ---: |
| 1 | 0 | 45.473–51.028 | 50.480–58.094 |
| 1 | 100 | 180.330–185.716 | 185.011–188.460 |
| 4 | 0 | 39.823–51.347 | 44.961–51.283 |
| 4 | 100 | 154.885–156.194 | 156.078–156.668 |

## Correctness and unresolved failure

- `rootJVM/testOnly ScheduledDeadlineBehavior`: 4 passed. Deterministic checks cover deferred later moves, earlier replacements, stale dispatched callbacks, cancellation/reuse and refusal of concurrent waits.
- `ioKqueue/testOnly gears.async.io.kqueue.ScheduledDeadlineSuite`: 7 passed. Both timer modes are checked on kqueue, including the QUIC state machine; the higher-level timer is also moved concurrently from external threads.
- A preliminary smaller smoke run and three subsequent smaller repeat runs completed successfully.
- **The first full-size attempt failed** during four-worker paced QUIC with `ClassCastException: scala.util.Right cannot be cast to scala.scalanative.unsigned.USize`, through `FutureAsync.await` and the primitive pacing timer. Its cause has not been diagnosed or fixed. The three full-size reruns above completed, but do not establish that the intermittent failure is gone. Do not infer that it is necessarily caused by the new timer or necessarily unrelated to it.

The failed attempt is preserved in [failed-run-1.csv](failed-run-1.csv), with the trace excerpt and command in [failed-run-1.txt](failed-run-1.txt). Its samples are excluded from the tables above. Successful raw measurements are [run-1.csv](run-1.csv), [run-2.csv](run-2.csv), and [run-3.csv](run-3.csv).

For the workload and timing boundaries, see the [benchmark README](../../README.md).
