# Skywalk as a reactor backend, without Dispatch: a design note

`NWFrameworkBackend` (see `TODO.md` beside this file) runs the ops over Network.framework, which is Apple's user-space
networking stack over Skywalk, the kernel's user-space data path, with Dispatch in between. This note records what it
would take to go to Skywalk directly, with no Dispatch and no Network.framework, as a backend driven by the kqueue
loop. It was written on 2026-10-22 from the xnu sources on `apple-oss-distributions/xnu` at `main` and from the
machine itself (Darwin 27.0.0); nothing in it has been built yet.

## What Skywalk gives a process

- **A channel is a descriptor over a mapped arena.** `os_channel_create_extended` opens a channel to a port of a
  nexus; `os_channel_get_fd` gives its descriptor, and the TX and RX rings, their slots and the channel's packet pool
  live in memory the kernel maps into the process (`bsd/skywalk/channel/os_channel.h`). `os_channel_sync` is the
  syscall that hands the kernel what TX has and takes what RX has. The channel descriptor takes `EVFILT_READ` and
  `EVFILT_WRITE` in kqueue (`bsd/skywalk/channel/channel.c`, `filt_chread` and `filt_chwrite`), so `KqueueLoop` can
  wait on it as on a socket. Nothing needs Dispatch.
- **The user-space library ships; its headers do not.** `libsystem_kernel.dylib` exports 139 `os_channel_*`,
  `os_nexus_*` and `os_packet_*` wrappers, the SDK's `libsystem_kernel.tbd` lists them, and their source is xnu's
  `libsyscall/wrappers/skywalk/`. The headers are `PRIVATE` and stripped from the SDK: `bsd/skywalk/channel/os_channel.h`,
  `bsd/skywalk/nexus/os_nexus.h`, `bsd/skywalk/packet/os_packet.h`, and the `*_private.h` beside them with the
  structures the syscalls take. Scala Native binds them with `@extern` against vendored copies, at the tag matching
  the kernel; the public SDK carries only stray mentions (`IFCAP_SKYWALK` in `net/if.h`, `VM_MEMORY_SKYWALK`).
- **A flowswitch port sees layer 3.** The flowswitch (`bsd/skywalk/nexus/flowswitch/`) demuxes packets to flows by
  their 5-tuple, reserves ports, reassembles IP (`kern.skywalk.flowswitch.ip_reass`), offloads checksums, segments
  (`gso_mtu`) and aggregates TCP on receive (`rx_agg_tcp`). What a user port receives has its link header stripped
  (`fsw_dp.c` sets `pkt_l2_len` to 0 on the copy to the user packet). The TCP state machine is not in the kernel: the
  flow tracker in `flow_var.h` follows TCP states for accounting and keep-alives, and the stack itself is
  Network.framework's, in user space. A backend without Network.framework must bring its own TCP and UDP over IP.
- **The machine already has it attached.** `ifconfig -v en0` shows a netif nexus and a flowswitch nexus, and two
  Skywalk netagents, `NetIf` and `FlowSwitch`, described as "Userspace Networking". The macOS default in
  `bsd/skywalk/os_skywalk_private.h` (`SKYWALK_NETWORKING_MAC_OS`) enables the flowswitch and its transport
  netagent for Ethernet and cellular interfaces. `lo0` carries neither nexus, so loopback traffic cannot take this
  path; a bench over it needs two interfaces or a feth pair.

## Getting a flow without entitlements

The Skywalk privileges in `bsd/sys/priv.h` guard registering nexuses (`com.apple.private.skywalk.register-user-pipe`,
`-kernel-pipe`, `-net-if`, `-flow-switch`), opening a low-latency channel, and observing. They are private
entitlements, which a stock machine does not grant to a self-signed binary. Two consequences:

- **No direct flow add.** `NXCFG_CMD_FLOW_ADD` on the system flowswitch requires the controller that owns the nexus,
  and the kernel's controller owns it (`nx_flowswitch.c`, `nexus.c`): a user controller's `__os_nexus_flow_add` gets
  EPERM, entitled or not.
- **No user pipe nexus.** The natural stand-in for Unix domain sockets and pipes needs `register-user-pipe`, so those
  stay on kqueue, as they stay on Dispatch sources in `NWFrameworkBackend`.

The route an ordinary process takes is NECP, which is how Network.framework's stack gets its flows (`bsd/net/necp.h`,
`bsd/net/necp_client.c`):

1. `necp_open`, then `necp_client_action` with `NECP_CLIENT_ACTION_ADD` and the client's parameters, and
   `NECP_CLIENT_ACTION_ADD_FLOW` naming the flowswitch netagent.
2. The kernel adds the flow under its own controller and returns, in the client's results,
   `NECP_CLIENT_RESULT_NEXUS_INSTANCE`, `NECP_CLIENT_RESULT_NEXUS_PORT` and `NECP_CLIENT_RESULT_NEXUS_KEY`.
3. The process opens a channel to that instance and port, with the key on the channel attributes
   (`os_channel_attr_set_key`), which `__channel_open` matches against the flow's binding (`channel.c`, `nxbind`).

The only entitlement `necp_client_action` checks on that path is for `NECP_CLIENT_PARAMETER_FLAG_INTERPOSE`, which a
backend does not need. What is not written down anywhere is the parameter TLVs and the order of the calls: they are
private, versioned per release, and the reference is `necp_client.c` for the running kernel, with Network.framework's
binary to trace when the sources leave a gap. Whether each flow gets a port and channel of its own, or several flows
share a port by `NECP_CLIENT_RESULT_NEXUS_PORT_FLOW_INDEX`, is to be settled the same way.

## Mapping onto the reactor

A readiness backend, so the design is `KqueueBackend`'s rather than `NWFrameworkBackend`'s, and a `ReadinessBackend`
with `whenReady` on channel descriptors falls out of it.

- **Loop.** The channel descriptors join the reactor's kqueue. A turn drains the RX ring of each channel that
  signalled readable, runs the protocol state machines, fills TX rings, and calls `os_channel_sync` once per channel
  touched at the turn's end, which is the batching `Reactor.submit` allows. `asyncio.unsafe.TimerHeap` serves the
  protocol timers. `trySubmit` finishes a read from bytes already in the receive buffer, as kqueue's does.
- **Handles are table indexes**, as in `NWFrameworkBackend`, since a handle is a flow rather than a descriptor.
- **Ops.** `receive` and `send` map onto UDP flows, one packet per slot, with the IP and UDP headers written by the
  backend. `read` and `write` sit on a TCP send and receive buffer in the backend. `accept` needs a listener flow and a
  child flow per completed handshake. `connect` is the SYN exchange; `listening` is the flow assignment completing;
  `close` sends FIN and deletes the flow once the state machine is done. `resolve` and `blocking` stay on the shared
  blocker pool (`asyncio.Blockers`). The pooled `read(handle, pool, into)` can be zero copy: the slot's packet buffer
  goes out as the `Received`, and `release` frees the packet (`os_channel_packet_free`). A read into the caller's
  buffer costs one copy, as kqueue's `recv` does.
- **Memory.** The arena is outside the collector; slots and packets are read and written through pointers, never
  copied into managed memory except on the caller's behalf.
- **Threading.** A channel is driven by one thread at a time, which the one-reactor-per-submission rule gives.

## Order of work

1. **Feasibility, standalone.** A UDP echo over a NECP-assigned flow on `en0`, driven by `kevent` on the channel
   descriptor, in a Scala Native binary with the vendored headers. It proves the NECP handshake and the ring layout
   for the running kernel, and it is the step most likely to fail.
2. **UDP ops.** A backend offering `receive`, `send`, `timer`, `resolve` and `blocking` over Skywalk, with the rest
   of `Ops` on kqueue sockets, so the contract suite runs against it.
3. **TCP.** Either a small stack in Scala, or lwIP vendored in C, built single-threaded on its raw API with no OS
   layer, which fits the per-turn model and needs no Dispatch. A TCP with congestion control, retransmission and SACK is
   the largest item here by a wide margin, and the one that decides whether the backend is worth having.
4. **Bench** against kqueue and `NWFrameworkBackend` on the same scenarios, over two interfaces since `lo0` has no
   flowswitch. The round trip to beat removes the kernel socket layer and Network.framework's Dispatch hop (the
   10 µs over kqueue measured in `TODO.md`); whether a user-space TCP in Scala Native beats the kernel's socket path
   is open until measured.

## Risks

- **Private ABI.** The NECP and Skywalk structures change between releases with no compatibility promise. A backend
  tracks xnu tags, and a kernel update can break it until the headers are refreshed.
- **Not loopback, not Unix sockets, not pipes.** The gears demos that need those cannot run on it alone.
- **A TCP of one's own** is a protocol implementation to maintain, with its own conformance and security surface.
