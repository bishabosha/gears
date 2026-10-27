# What the interface lacks for TLS, QUIC, HTTP and WebSockets

A proposal, recorded 2026-10-27 and not yet started. Nothing in the backend interface stops these protocols being
built today: each runs over reliable byte streams or datagrams, which the interface has, and TLS can run in user space
over plain `read` and `write`, as the JDK's `SSLEngine` and rustls do. What follows is what a correct or a fast
implementation would miss, what each backend could offer for it, and what deliberately stays out of the interface.

Where a backend lacks something below, it should fail clearly, not ignore it, as every op misused does. Claims about
Network.framework's options are from its headers, not yet probed.

## Gaps that block a correct implementation

**A socket's local and peer addresses.** Done (2026-10-27): `localAddress` and `peerAddress`, on stream sockets of
either family and on datagram sockets of each, plain methods rather than ops, since every backend answers at once once
the socket is open (`getsockname` and `getpeername` on kqueue, the channels' addresses on NIO, the path's endpoints and
the listener's port on Network.framework). A listener or bound socket on port 0 tells its port, and `peerAddress` of an
accepted connection tells who connected, so `accept` writes nothing more. Contract checks on every backend. The
contract suites still find free ports for the checks that need a fixed one.

**Socket options.** None is exposed. The only ones set are `SO_REUSEADDR`, always on listeners, and dual stack
on `::`, which is fixed on since the JDK cannot clear it.

- `TCP_NODELAY`. Nagle's algorithm is on everywhere. TLS, HTTP/2 and WebSockets write small records, and Nagle with
  delayed acknowledgements can stall each by tens of milliseconds.
- Don't-fragment, for QUIC: RFC 9000 §14 requires its UDP datagrams not to be fragmented, with IPv4's DF bit set where
  possible. The JDK has it through `jdk.net.ExtendedSocketOptions` since JDK 19.
- Keepalive and its timings (long-lived HTTP/1.1, HTTP/2 and WebSocket connections); receive and send buffer sizes
  (QUIC's throughput); `SO_REUSEPORT` (a listener per worker); ECN (QUIC's congestion control, recommended rather than
  required).
- Proposed shape: options given as a socket is opened (`connect`, `listen`, `bindDatagram`, `connectDatagram`), since Network.framework takes
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

## QUIC and TLS from Network.framework itself

Network.framework has QUIC built in (since macOS 12), with its own TLS 1.3, multiplexed streams and QUIC's unreliable
datagrams (RFC 9221), and TLS over TCP; the Network.framework backend, which now uses the framework where it adds what
kqueue cannot (see [`nwframework.md`](nwframework.md)), could offer both as optional capabilities that only it has, so
that a program asking for them does not compile against kqueue or NIO. They map onto the interface's kinds:

- **A QUIC connection** is like a listener of streams: an op opens it (address, ALPN, TLS settings), an accept-like op
  takes each stream the peer opens, and an op opens one of its own.
- **A bidirectional stream** is a `Connection`: `read`, `write`, `shutdownOutput` (its FIN) and `close` as they are; a
  unidirectional one is a `Readable` or a `Writable`.
- **QUIC's datagrams** are a send and a receive on the connection, and **a QUIC listener** accepts connections as a
  TCP listener does.
- **TLS over TCP** is a `Connection` opened with TLS settings, the rest as for any connection.

That leaves two kinds of QUIC for gears, which are not exclusive: the framework's, as a capability of one backend, little
work and macOS's alone; and QUIC in user space over datagrams, on every backend, a large project needing everything in
the lists above (don't-fragment, fine timers re-armed in place, packet info, ECN, batching). What every server needs
either way, addresses and socket options, stays first.

Probed on macOS 27.0, over loopback (`bench/network-framework-quic.c`, a self-signed identity imported into memory):

- **The pieces exist in the C interface since macOS 12**: `nw_parameters_create_quic` with ALPN, TLS identity and
  verification, stream and datagram limits set on its options; a listener whose QUIC connections each arrive as a
  connection group (`nw_listener_set_new_connection_group_handler`); a client's connection group with a multiplex
  descriptor; streams opened with `nw_connection_group_extract_connection` and accepted with the group's new connection
  handler, on either side.
- **A stream is an `nw_connection_t`, read and written as a TCP connection is.** Three client streams (ids 4, 8, 12), a
  unidirectional one (id 2) and a server-opened one (id 5) each carried their bytes and then their end as the
  backend's TCP connections deliver it, a receive complete in the final context: the same events, so the backend's
  stream handling, its send window and its pooled read would serve them as they are.
- **Datagrams are a flow of their own** (options with `nw_quic_set_stream_is_datagram`), whose messages keep their
  boundaries, complete and not final, as a connected UDP socket's do.
- **Opening and accepting are what the interface has**: a stream's opener completes when the stream is ready, and the
  group's handler hands over the peer's streams as a listener hands over connections, so a QUIC connection maps onto
  an op that accepts streams and one that opens them.
- **A stream's type from its metadata is not to be trusted**: the server's own stream, bidirectional by its id (5, a
  server-opened bidirectional stream under RFC 9000) and by what it carried, reported itself a datagram stream on both
  sides, with or without options saying otherwise; its id is what tells.
- **Not probed yet**: a stream reset and its application error, flow control filling a stream's window, the connection
  closing with streams open, and the cost against kqueue over TCP.

## What stays out of the backend interface

- **TLS itself, ALPN and SNI, on kqueue and NIO.** They belong to a TLS layer over `read` and `write`. A native TLS
  library that drives a socket itself would need readiness and the raw descriptor, which only kqueue has; kernel TLS on
  Linux would be a capability of its own, as Network.framework's TLS would (above). Both are optional extras.
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
| 1 | Local and peer addresses (done) | correctness for every server |
| 2 | Options as a socket opens, starting with no-delay, keepalive, buffer sizes and don't-fragment | correct and fast TLS, HTTP and WebSockets; QUIC's compliance |
| 3 | Nanosecond timers re-armed in place | QUIC |
| 4 | Vectored reads and writes | throughput for everything that frames its data |
| 5 | Optional capabilities: packet info, batched datagrams, files sent without copying | fast QUIC and HTTP on the backends that can |

Each comes with contract checks on every backend that has it.
