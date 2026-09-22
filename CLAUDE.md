HttpPooler
==========

Trusted peers pool their local HTTP services over an encrypted link; each request goes to the first free
provider. One server, any number of clients; any peer may provide and/or consume any service. Written in
Go.

Documents
---------

- PROTOCOL.md is authoritative for everything on the wire, byte by byte. Code follows it; when they differ,
  fix one and say which. Any layout change is a new version constant there and in code.
- README.md is the operator's view: setup, conf template, behaviour. Keep it true to the code and brief.

Building and testing
--------------------

    go build -buildvcs=false ./... && go test ./...
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' ./cmd/httppooler   # release: static

-buildvcs=false because a Mercurial repository above this checkout makes Go's VCS stamping ambiguous and it
refuses to link at all ("multiple VCS detected"). Nothing reads the stamp, and -trimpath already keeps local
detail out of the binary.

go.mod pins the Go version; the installed `go` command fetches that toolchain itself. The binary's only
external dependency is golang.org/x/crypto. flynn/noise is a test-only dependency, the interop oracle.
Tests need no network and no Ollama: fake upstreams run in-process.

Layout
------

- cmd/httppooler: CLI. `genkey` prints 64 hex chars (the install script uses it); `run -conf <path> [-v]`,
  conf defaults to /etc/httppooler/httppooler.conf
- internal/noise: NNpsk0 handshake and transport; vector-tested; knows nothing about HttpPooler
- internal/wire: record stream and message codec
- internal/conf: conf parser
- internal/pool: the server's queues and slots
- internal/peer: connections, providers, consumers
- deploy: systemd unit (Restart=always, ExecReload sends SIGHUP) and install.sh, which puts it on a machine
- build_release_package.sh: packs those two and the binary into the tarball they travel in

Rules
-----

- Payload-agnostic: no code may look inside a body or know any service's protocol.
- No size caps anywhere; bodies stream as chunks. The only bounds are structural or inherited from Noise.
- Wire parsing is strictly sequential and refuses malformed input whole, as PROTOCOL.md says.
- Crypto primitives come from the standard library and x/crypto only. Only the composition is ours, and it
  stays verified against the published vectors and flynn/noise.
- The server's own [provide] and [consume] sections run the same code as a client's, in-process. No special
  fallback paths.
- Conf grammar: `[section]` or `[section "name"]`, `key = value`, `#` to end of line, blank lines. Unknown
  sections or keys, duplicates, missing required keys and bad values refuse the whole conf with the line
  number. `secret` is 64 hex chars, `queue_timeout` a Go duration. Defaults are the values the README
  template shows.
- The daemon never exits because of its conf. At startup an invalid or roleless conf is logged and the
  daemon idles; on SIGHUP a valid conf is applied by re-exec, an invalid one logged and the running conf
  kept. SIGUSR1 logs peers, slots and queues.
- Responses the server synthesizes (502, 503, 501) are text/plain with the reason as the body.

Conventions
-----------

- Markdown: Setext headers only, two levels; wrap at 120; brief. LF line endings everywhere.
- A function that checks several things is a sequence of guards, one `if … { return }` per thing under a
  comment naming it, never one chained expression.
- gofmt, no dependencies beyond those named above.

Later
-----

- Per-peer tokens for revocation.
- `type = tcp` for connection-pinned services, honestly named.
- Routing by a request label, if interchangeable providers ever stop being enough.
- `httppooler status`, a subcommand rather than a second binary, printing peers, slots and queues. SIGUSR1
  cannot back it: its output goes to the journal, and a command that prints nothing is no command. It wants
  a local control socket under /run, with RuntimeDirectory in the unit.
- Flow control, a per-job window. PROTOCOL.md says what version 0 does instead.
