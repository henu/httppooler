Plan
====

Order of work; each step ends in a review.

1. [x] Decisions, README.md, PROTOCOL.md.
2. [x] internal/noise: NNpsk0 handshake and transport. Vector tests from flynn/noise vectors.txt (vendored),
       interop test against flynn/noise in both roles.
3. [ ] Everything else in one go: wire, conf, pool, peer, cmd, and an in-process integration test with a
       server, two clients and fake upstreams covering routing, FIFO, priority, slots, requeue on connection
       loss, cancel, streaming and queue timeout. Install script (form open): puts in place the binary, a
       system user, a template conf with a fresh secret and every other key commented out, readable by root
       and the service user only, and an enabled unit. The daemon idles on an incomplete conf and never
       watches the file; `systemctl reload` (SIGHUP) re-reads it, applies a valid conf by re-exec and keeps
       the running one when the new one is invalid.
4. [ ] Deploy to two machines.

Later
-----

- Per-peer tokens for revocation.
- `type = tcp` for connection-pinned services, honestly named.
- Routing by a request label, if interchangeable providers ever stop being enough.
- HTTP trailers are carried but nothing uses them; WebSocket and Upgrade requests are answered 501.
