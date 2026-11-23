# The reference backend against the NIO backends

2026-11-19, on the branch `experiment/capabilities`, one macOS machine, the JVM the build runs on.
`ioCapBenchJVM/run --backend <reference|threads|pool> --workers 4 --scenarios ... --repeat 3`, three processes each,
the last repetition of each taken; microseconds an op, medians with ranges.

The reference backend (`io-reference`) is what the contracts are checked against, "with no regard for speed": each op
is tried by the virtual thread performing it and, if it would block, waited for on one selector that one thread
polls. It has no workers, so `--workers` means nothing to it; its futures are on the JDK's own scheduler.
`threads` is `NioBackend`, `pool` is `NioPoolBackend`, each with four workers.

| | reference | `NioBackend` | `NioPoolBackend` |
|---|---:|---:|---:|
| pipe-pingpong | 19.01 | 15.97 | 2.71 (2.66-8.91) |
| pipe-concurrent | 8.75 | 5.59 | 1.69 |
| tcp-pingpong | 30.24 | 32.35 | 19.64 |
| tcp-concurrent | 9.35 | 6.68 | 5.10 |
| tcp-stream | 6.80 (6.40-7.78) | 7.53 (6.84-8.13) | 7.43 (7.43-8.14) |
| tcp-connect | 99.60 | 100.64 | 72.68 |
| tcp-realistic | 9.38 | 6.74 | 5.74 |
| tcp-realistic-bulk | 0.59 | 0.44 | 0.41 |
| flow-mux (`Auto`) | 0.65 | 0.76 (0.66-0.78) | 0.58 |
| flow-mux-futures | 0.67 | 0.85 (0.77-0.85) | 0.70 (0.59-0.72) |
| file-read | 0.40 | 0.44 | 8.03 |
| file-write | 4.54 | 4.34 | 14.84 |

What it shows:

- For a round trip a connection, the pool is ahead of the reference backend by 35 to 45% (tcp-concurrent,
  tcp-realistic, tcp-pingpong) and `NioBackend` by about 28%, except on tcp-pingpong, where it is no faster than the
  reference backend.
- On pipes the pool is five to seven times faster than either.
- On a stream written as fast as it is read, and on a batch of requests answered together, the three are within
  each other's ranges or close: the work is the kernel's and the program's, not the waiting.
- The multiplexed server is as fast on the reference backend, made of plain futures, as on `NioBackend`, and within
  12% of the pool's pinned futures.
- Files are where the pool loses: a read is 20 times slower and a write three times, because the pool does a file's
  op on another thread and the other two do it on the caller's.

`Auto` picked `Futures` on the reference backend, which has neither shards nor queues and whose ops park their
thread, and `PinnedFutures` on both NIO backends.
