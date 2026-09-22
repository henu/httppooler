package peer

// The peer that listens. It holds the pool, relays between a submitting peer and a providing one, and
// runs its own [provide] and [consume] sections over a pipe as if they were another client's.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/henu/httppooler/internal/conf"
	"github.com/henu/httppooler/internal/pool"
	"github.com/henu/httppooler/internal/wire"
)

// Server is the whole server role: the listening socket, the pool, and its own sections.
type Server struct {
	cfg      *conf.Conf
	pool     *pool.Pool
	opts     Options
	log      *slog.Logger
	endpoint *Endpoint

	mu    sync.Mutex
	peers map[*Conn]bool
}

// NewServer builds the server role from a conf. Nothing is opened yet.
func NewServer(cfg *conf.Conf, opts Options, log *slog.Logger) *Server {
	return &Server{
		cfg:      cfg,
		pool:     pool.New(cfg.QueueTimeout),
		opts:     opts,
		log:      log,
		endpoint: NewEndpoint(cfg, opts, log),
		peers:    map[*Conn]bool{},
	}
}

// Run listens until ctx ends.
func (s *Server) Run(ctx context.Context) error {
	listener, err := s.opts.listen(ctx, s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Listen, err)
	}
	defer listener.Close()
	s.log.Info("listening", "address", s.cfg.Listen, "queue_timeout", s.cfg.QueueTimeout)

	if err := s.endpoint.Start(ctx); err != nil {
		return err
	}
	defer s.endpoint.Close()

	s.serveSelf(ctx)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		raw, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		go s.accept(ctx, raw)
	}
}

// serveSelf puts the server's own sections on a pipe. Layer 3 and up is the same code a client runs;
// there is no preamble, no handshake and no records, because there is no connection to protect.
func (s *Server) serveSelf(ctx context.Context) {
	if len(s.cfg.Provide) == 0 && len(s.cfg.Consume) == 0 {
		return
	}

	local, remote := net.Pipe()
	go func() {
		defer remote.Close()
		s.handle(ctx, newConn(remote, remote, remote, wire.RoleServer, "", s.log))
	}()
	go func() {
		defer local.Close()
		c := newConn(local, local, local, wire.RoleClient, s.cfg.Name, s.log)
		if err := s.endpoint.Serve(ctx, c); err != nil && ctx.Err() == nil {
			s.log.Error("the server's own sections stopped", "error", err)
		}
	}()
}

// accept takes one connection through layers 0 to 2 and then serves it.
func (s *Server) accept(ctx context.Context, raw net.Conn) {
	// A peer that connects and then says nothing holds nothing open for long.
	raw.SetDeadline(time.Now().Add(idleTimeout))

	session, err := wire.Handshake(raw, s.cfg.Secret, false)
	if err != nil {
		raw.Close()

		// A port scanner is not an event; a peer from another version is.
		var version *wire.VersionError
		switch {
		case errors.Is(err, wire.ErrNotPeer):
		case errors.As(err, &version):
			s.log.Warn("peer speaks another protocol version",
				"address", raw.RemoteAddr(), "theirs", version.Theirs, "ours", version.Ours)
		default:
			s.log.Warn("handshake failed", "address", raw.RemoteAddr(), "error", err)
		}
		return
	}
	raw.SetDeadline(time.Time{})

	defer raw.Close()
	s.handle(ctx, newConn(raw, session.Reader, session.Writer, wire.RoleServer, "", s.log))
}

// handle runs one client session: its HELLO, its providers in the pool, and every job on it.
func (s *Server) handle(ctx context.Context, c *Conn) {
	defer c.Close()

	hello, err := c.hello()
	if err != nil {
		s.log.Warn("no usable HELLO", "address", c.raw.RemoteAddr(), "error", err)
		return
	}
	c.setPeer(hello.Name)

	providers, err := s.register(c, hello)
	if err != nil {
		s.log.Warn("refused a peer", "peer", hello.Name, "error", err)
		return
	}
	defer func() {
		for _, p := range providers {
			s.pool.Remove(p)
		}
	}()

	s.addPeer(c)
	defer s.removePeer(c)
	s.log.Info("peer connected", "peer", hello.Name, "providers", len(providers))

	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go c.ping(ctx)

	err = c.run(ctx, s.relay)
	s.log.Info("peer gone", "peer", hello.Name, "error", err)
}

// register puts a peer's announced upstreams in their pools.
func (s *Server) register(c *Conn, hello *wire.Hello) ([]*pool.Provider, error) {
	seen := map[string]bool{}
	providers := make([]*pool.Provider, 0, len(hello.Providers))

	for _, info := range hello.Providers {
		// One provider per service per peer: a REQUEST names the service, so a second one for the
		// same service would be a job with two places to go and no way to say which.
		if seen[info.Service] {
			return nil, fmt.Errorf("two providers of %q", info.Service)
		}
		seen[info.Service] = true

		p := &pool.Provider{
			Peer:     hello.Name,
			Service:  info.Service,
			Slots:    info.MaxConcurrent,
			Priority: info.Priority,
			Target:   c,
		}
		providers = append(providers, p)
		s.pool.Add(p)
	}
	return providers, nil
}

// errProviderGone is a provider whose connection ended. Before its RESPONSE the job is queued again;
// after it, there is nothing left to do but tell the consumer.
var errProviderGone = errors.New("the provider's connection ended")

// providerFail is a FAIL that arrived before any RESPONSE, which the server answers 502 for.
type providerFail struct {
	reason string
}

func (e *providerFail) Error() string { return e.reason }

// relay is the server's side of a submitted job: queue it, hand it to the first free provider, and pass
// everything between the two peers. The failures it answers itself are the pool's own.
func (s *Server) relay(ctx context.Context, c *Conn, req *wire.Request, in *jobQueue) {
	defer c.endJob(req.Job)
	arrived := time.Now()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// The whole request body is taken in first, so the job can be handed to another provider without
	// asking the application for it again.
	body, trailers, err := collect(ctx, in)
	if err != nil {
		return
	}
	go watchCancel(ctx, in, cancel)

	front := false
	for {
		lease, err := s.pool.Acquire(ctx, req.Service, arrived, front)
		if err != nil {
			// A job whose application left needs no answer; one that waited its whole timeout does.
			if ctx.Err() != nil {
				return
			}
			s.answer(c, req.Job, 503, err.Error())
			return
		}

		target, ok := lease.Provider().Target.(*Conn)
		if !ok {
			lease.Release()
			s.answer(c, req.Job, 502, "the provider has no connection")
			return
		}

		started, err := s.dispatch(ctx, c, req, body, trailers, target)
		lease.Release()

		if err == nil {
			return
		}

		// After a RESPONSE the answer is already on its way; all that is left is to end it.
		if started {
			if errors.Is(err, errProviderGone) {
				c.send(&wire.Fail{Job: req.Job, Reason: errProviderGone.Error()})
			}
			return
		}

		var failed *providerFail
		switch {
		case errors.Is(err, errCancelled):
			return
		case errors.Is(err, errProviderGone):
			// The provider vanished before it answered, so the job goes back to the head of its
			// queue, keeping the deadline it arrived with.
			s.log.Info("requeued", "service", req.Service, "provider", target.Peer())
			front = true
		case errors.As(err, &failed):
			s.answer(c, req.Job, 502, failed.reason)
			return
		default:
			s.answer(c, req.Job, 502, err.Error())
			return
		}
	}
}

// dispatch is one attempt at one provider: the request out, and everything it says back relayed to the
// peer that submitted it. It reports whether a RESPONSE was relayed, because that is the line after
// which the job can no longer be retried or answered any other way.
func (s *Server) dispatch(ctx context.Context, c *Conn, req *wire.Request, body []byte, trailers []wire.Header, target *Conn) (bool, error) {
	job, in, err := target.newJob()
	if err != nil {
		return false, errProviderGone
	}
	defer target.endJob(job)

	out := &wire.Request{Job: job, Service: req.Service, Method: req.Method, Target: req.Target, Headers: req.Headers}
	if err := target.send(out); err != nil {
		return false, errProviderGone
	}
	if err := target.sendBodyEnd(job, body, trailers); err != nil {
		return false, errProviderGone
	}

	started := false
	for {
		m, err := in.next(ctx)
		if err != nil {
			// The submitter is gone, so the provider is told to stop.
			if ctx.Err() != nil {
				target.send(&wire.Cancel{Job: job})
				return started, errCancelled
			}
			return started, errProviderGone
		}

		var onward wire.Message
		switch m := m.(type) {
		case *wire.Response:
			onward = &wire.Response{Job: req.Job, Status: m.Status, Headers: m.Headers}
			started = true
		case *wire.Body:
			onward = &wire.Body{Job: req.Job, Bytes: m.Bytes}
		case *wire.End:
			onward = &wire.End{Job: req.Job, Trailers: m.Trailers}
		case *wire.Fail:
			// A failure before the answer began is the pool's to report as 502; after it, the
			// consumer is the only one who can be told.
			if !started {
				return false, &providerFail{reason: m.Reason}
			}
			onward = &wire.Fail{Job: req.Job, Reason: m.Reason}
		default:
			return started, fmt.Errorf("%s where an answer belongs", m.Kind())
		}

		// A submitter that cannot be written to has gone; the provider is told to stop.
		if err := c.send(onward); err != nil {
			target.send(&wire.Cancel{Job: job})
			return started, errCancelled
		}

		switch onward.(type) {
		case *wire.End, *wire.Fail:
			return started, nil
		}
	}
}

// answer is a response the server makes up rather than relays: text/plain with the reason as the body.
func (s *Server) answer(c *Conn, job uint64, status uint16, why string) {
	s.log.Info("answering from the pool", "peer", c.Peer(), "job", job, "status", status, "reason", why)
	for _, m := range synthMessages(job, status, why) {
		if err := c.send(m); err != nil {
			return
		}
	}
}

// addPeer and removePeer keep the list SIGUSR1 prints.
func (s *Server) addPeer(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers[c] = true
}

func (s *Server) removePeer(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, c)
}

// LogState writes what SIGUSR1 asks for: the peers, their slots and the queues.
func (s *Server) LogState() {
	s.mu.Lock()
	peers := make([]string, 0, len(s.peers))
	for c := range s.peers {
		peers = append(peers, c.Peer())
	}
	s.mu.Unlock()
	sort.Strings(peers)

	s.log.Info("peers", "count", len(peers), "names", peers)
	for _, service := range s.pool.Snapshot().Services {
		for _, p := range service.Providers {
			s.log.Info("provider", "service", service.Name, "peer", p.Peer,
				"busy", p.Busy, "slots", p.Slots, "priority", p.Priority)
		}
		s.log.Info("queue", "service", service.Name, "waiting", service.Waiting,
			"providers", len(service.Providers))
	}
}
