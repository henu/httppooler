Plan
====

Order of work; each step ends in a review.

1. [x] Decisions, README.md, PROTOCOL.md.
2. [x] internal/noise: NNpsk0 handshake and transport. Vector tests from flynn/noise vectors.txt (vendored),
       interop test against flynn/noise in both roles.
3. [x] The daemon itself in one go: wire, conf, pool, peer, cmd, and an in-process integration test with a
       server, two clients and fake upstreams covering routing, FIFO, priority, slots, requeue on connection
       loss, cancel, streaming and queue timeout. The daemon idles on an incomplete conf and never watches
       the file; `systemctl reload` (SIGHUP) re-reads it, applies a valid conf by re-exec and keeps the
       running one when the new one is invalid. Done 2026-09-22: the integration tests run whole peers on an
       in-memory network, so the suite still opens no port, and the binary was smoke-tested on real sockets.
4. [ ] Installation and packaging (form open): unit file, install script, whatever else setup needs. Done
       before the deploy so the deploy tests it.
5. [ ] Deploy to the real machines. Hand-tested 2026-09-22 before the packaging, binaries and confs copied
       by hand onto three: henu.fi is the server, psychotron provides Ollama, vohveli only consumes.
       Routing, queueing behind one slot, streaming and cancel all behaved; what is left for this step is
       the packaging doing it instead of hands.

Later
-----

- Per-peer tokens for revocation.
- `type = tcp` for connection-pinned services, honestly named.
- Routing by a request label, if interchangeable providers ever stop being enough.
- HTTP trailers are carried but nothing uses them; WebSocket and Upgrade requests are answered 501.
- Flow control. Version 0 has no per-job window, so a peer that receives a response body faster than it can
  pass it on buffers the difference. Concurrency is bounded by slots, memory is not.
