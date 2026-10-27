# What the interface lacks for TLS, QUIC, HTTP and WebSockets

A proposal, recorded 2026-10-27 and not yet started. Nothing in the backend interface stops these protocols being
built today: each runs over reliable byte streams or datagrams, which the interface has, and TLS can run in user space
over plain `read` and `write`, as the JDK's `SSLEngine` and rustls do. What follows is what a correct or a fast
implementation would miss, what each backend could offer for it, and what deliberately stays out of the interface.

Where a backend lacks something below, it should fail clearly, not ignore it, as every op misused does. Claims about
Network.framework's options are from its headers, not yet probed.

## Gaps that block a correct implementation

**A socket's local and peer addresses.** There is no way to ask for either.

- `listen` and `datagram` on port 0 cannot tell the program which port they got; the contract suites work round it by
  finding a free port first, which races.
- `accept` hands back a connection but not who connected, which an HTTP server needs for logs, rate limits and proxy
  headers.
- Proposed: `localAddress(handle)` and `peerAddress(handle)` as ops, and `accept` and `acceptAll` writing the peer
  beside each connection. Every backend can: `getsockname` and `getpeername` on kqueue, the channels' addresses on
  NIO, a connection's endpoints on Network.framework.

**Socket options.** None is exposed. The only one set is `SO_REUSEADDR`, from `listen`'s `reuseAddr`, and dual stack
on `::`, which is fixed on since the JDK cannot clear it.

- `TCP_NODELAY`. Nagle's algorithm is on everywhere. TLS, HTTP/2 and WebSockets write small records, and Nagle with
  delayed acknowledgements can stall each by tens of milliseconds.
- Don't-fragment, for QUIC: RFC 9000 §14 requires its UDP datagrams not to be fragmented, with IPv4's DF bit set where
  possible. The JDK has it through `jdk.net.ExtendedSocketOptions` since JDK 19.
- Keepalive and its timings (long-lived HTTP/1.1, HTTP/2 and WebSocket connections); receive and send buffer sizes
  (QUIC's throughput); `SO_REUSEPORT` (a listener per worker); ECN (QUIC's congestion control, recommended rather than
  required).
- Proposed shape: options given as a socket is opened (`connect`, `listen`, `datagram`), since Network.framework takes
  them only on its parameters at creation; and a `setOption` op for the few that can change later, such as no-delay and
  keepalive.

**A QUIC server on a wildcard address.** On a host with several addresses, a server bound to `0.0.0.0` must reply from
the address the client sent to, or the client drops the reply. That needs the destination address with each received
datagram and a chosen source address on each send (`IP_PKTINFO`, `IPV6_RECVPKTINFO`). NIO cannot: the JDK has no
ancillary data, and the portable workaround is a socket bound to each local address. Proposed: an optional packet-info
capability on kqueue and Network.framework, absent on NIO.

**Timer resolution and cost.** `timer(milliseconds: Int)` is too coarse for QUIC's pacing and acknowledgement-delay
timers, and QUIC re-arms or cancels a timer for almost every packet. Today that is cancelling a `Future`, measured at
about 20 µs a timer on kqueue (see "What is open" in [`runtime.md`](runtime.md)). Proposed: a timer in nanoseconds, or to a deadline on a
monotonic clock, re-armed in place as one repeatable op given a new deadline, with no future to cancel. An op as a
passive future (`performAsync`, in the [README](README.md)) would remove the cost too.

## Gaps that make them slow, not impossible

| Proposal | Who needs it | Backends |
| --- | --- | --- |
| Vectored reads and writes (`readv`, `writev`) | a TLS record's header with its payload, HTTP/2 and WebSocket frames, HTTP headers with a body, all without copying | kqueue `readv`/`writev`; NIO's scattering and gathering channels; Network.framework concatenates `dispatch_data`. All three. |
| Batched datagrams, several packets a call, with segmentation offload | QUIC's throughput, dominated by system calls per packet | Linux `sendmmsg`/`recvmmsg`, macOS only private calls; none on the JDK; batched sends on Network.framework. An optional capability. |
| A file sent to a socket without copying (`sendfile`, `transferTo`) | static files over HTTP/1 | kqueue and NIO; not Network.framework. An optional capability. |
| Telling that a datagram was truncated (option B when truncation was settled) | mostly diagnostics, as QUIC fixes its packet size | all three, with a copy on NIO |
| `TCP_NOTSENT_LOWAT` | HTTP/2's prioritisation, keeping the kernel's send queue short | through the options |
| An abortive close (RST, through `SO_LINGER` 0) | a server dropping an abusive client | through the options, or a flag on `close` |
| A listen backlog of the caller's choosing | busy servers; it is 4096 on kqueue and NIO, capped by the kernel | kqueue and NIO; Network.framework has a connection limit instead |

## What stays out of the backend interface

- **TLS itself, ALPN and SNI.** They belong to a TLS layer over `read` and `write`. A native TLS library that drives a
  socket itself would need readiness and the raw descriptor, which only kqueue has; kernel TLS on Linux and
  Network.framework's own TLS would each be a capability of their own. Both are optional extras.
- **The WebSocket upgrade.** The connection carries on after HTTP's 101 response. The catch is in a higher level: a
  buffered reader such as `chunks` that has read past the headers must hand those bytes back, which is a point for
  `higherlevel`, not the backends.
- **HTTP/2 reading and writing at once.** A read and a write may already be pending on one handle together;
  serialising many writers is HTTP/2's own.
- **Happy Eyeballs and connect time limits.** Futures racing `connect` ops do this already, and a cancelled `connect`
  leaves nothing open.

## The order proposed

| Step | Change | What it unblocks |
| --- | --- | --- |
| 1 | Local and peer addresses | correctness for every server |
| 2 | Options as a socket opens, starting with no-delay, keepalive, buffer sizes and don't-fragment | correct and fast TLS, HTTP and WebSockets; QUIC's compliance |
| 3 | Nanosecond timers re-armed in place | QUIC |
| 4 | Vectored reads and writes | throughput for everything that frames its data |
| 5 | Optional capabilities: packet info, batched datagrams, files sent without copying | fast QUIC and HTTP on the backends that can |

Each comes with contract checks on every backend that has it.
