httppooler
==========

Trusted peers pool their local HTTP services over an encrypted link; each request goes to the first free
provider. Typical use: the application talks to a local port, the GPU machine at home does the work. Any
HTTP service works; payloads are never inspected.

- One peer is the `server`: it listens on a public port and holds the queues. Clients dial out to it, so a
  machine behind NAT needs no configuration.
- Any peer may `[provide]` a service (a local upstream doing the work) and `[consume]` one (a local port for
  applications). Every request goes consumer → server queue → first free provider. Providers of one service
  must be interchangeable.
- One secret, generated at install and copied into every client's conf, is the whole security setup: a
  Noise NNpsk0 channel, no TLS or certificates. PROTOCOL.md has every byte.

Setup
-----

The install puts /etc/httppooler/httppooler.conf in place with a fresh secret and every other key commented
out, and starts the service, which idles until told to read a conf with a role. After every edit run
`systemctl reload httppooler`: a valid conf is applied (requests in flight at that moment are dropped), an
invalid one is logged and the running conf kept.

- Server: uncomment `role = server` and add a [consume] section, reload. Copy the secret line to each client.
- Client: uncomment `role = client` and `server`, paste the server's secret, add a [provide] section, reload.

The template, which is the installed file, with the defaults it shows:

    [peer]
    #role = server              # or client
    #listen = 0.0.0.0:7420      # server only: where clients connect
    #server = example.com:7420  # client only: the server to dial
    secret = <64 hex chars>     # generated at install; every client uses the server's
    #name = homebox             # for logs; default hostname
    #queue_timeout = 10m        # server only: max wait for a free provider, then 503

    #[consume "myservice"]      # applications send their requests here
    #listen = 127.0.0.1:8080

    #[provide "myservice"]      # a local upstream joins the pool for that service
    #upstream = 127.0.0.1:9000
    #max_concurrent = 1         # requests run at the same time; match what the upstream really runs in parallel
    #priority = 100             # smallest number wins when several providers are free

Any peer may carry any mix of [provide] and [consume] sections, one of each per service. An unknown section
or key, a key set twice, a key belonging to the other role, a missing `listen` or `upstream`, and a value
that does not parse each refuse the whole conf with the line number.

Behaviour
---------

- Oldest request first; among free providers the smallest priority number wins. A fast machine at 100 gets
  everything while it has a free slot, a fallback at 200 gets the overflow.
- A request whose provider disconnects before the response began is retried on the next provider, so
  upstreams must tolerate a repeated request. Bodies of any size stream through.
- 503 after queue_timeout with no free provider, 502 when the upstream fails, 501 for WebSocket/Upgrade. A
  request the application abandons is cancelled at the provider.
- A consume port stays open whether the peer is connected or not; while it is not, requests get 503 saying
  so rather than a refused connection.
- Request bodies are held in memory until a provider takes the job, since a retry needs them again.

Running
-------

`httppooler run -conf <path>` is the daemon, `-v` adds the detail normally left out; `httppooler genkey`
prints a secret. Logs go to the journal; SIGUSR1 logs peers, slots and queues. `CGO_ENABLED=0 go build
-buildvcs=false ./cmd/httppooler` gives a static binary that needs nothing installed beside it.

`go test ./...` in the repository root runs every test, integration tests included. Nothing is started and
no port is opened: the peers in those tests run on an in-memory network. Only the first run needs the
network, to fetch the Go toolchain go.mod pins and the test-only dependency.
