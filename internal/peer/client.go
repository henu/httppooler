package peer

// The peer that dials. It keeps one connection to the server open, and when it drops it dials again.

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/henu/httppooler/internal/conf"
	"github.com/henu/httppooler/internal/wire"
)

const (
	// backoffFirst is how long a client waits before its first reconnect.
	backoffFirst = time.Second
	// backoffLongest is as long as the wait ever gets.
	backoffLongest = 60 * time.Second
)

// Client is the client role: the server to dial and this peer's own sections.
type Client struct {
	cfg      *conf.Conf
	opts     Options
	log      *slog.Logger
	endpoint *Endpoint
}

// NewClient builds the client role from a conf. Nothing is opened yet.
func NewClient(cfg *conf.Conf, opts Options, log *slog.Logger) *Client {
	return &Client{cfg: cfg, opts: opts, log: log, endpoint: NewEndpoint(cfg, opts, log)}
}

// Run keeps a connection to the server until ctx ends. The consume ports are opened once and stay open
// whether there is a connection or not, so an application asking while the peer is disconnected is told
// so rather than refused a socket.
func (c *Client) Run(ctx context.Context) error {
	if err := c.endpoint.Start(ctx); err != nil {
		return err
	}
	defer c.endpoint.Close()

	backoff := backoffFirst
	for ctx.Err() == nil {
		connected, err := c.session(ctx)
		if ctx.Err() != nil {
			return nil
		}

		// A connection that was made and then ended is not a reason to start waiting minutes.
		if connected {
			backoff = backoffFirst
			c.log.Info("connection to the server ended", "error", err)
		} else {
			c.log.Warn("cannot reach the server", "server", c.cfg.Server, "error", err)
		}

		if !sleep(ctx, jitter(backoff)) {
			return nil
		}
		backoff = min(backoff*2, backoffLongest)
	}
	return nil
}

// session dials, does layers 0 to 2, and runs the connection to its end. It reports whether the
// connection was ever made, which is what tells a server that is down from one that dropped us.
func (c *Client) session(ctx context.Context) (bool, error) {
	raw, err := c.opts.dial(ctx, c.cfg.Server)
	if err != nil {
		return false, err
	}
	defer raw.Close()

	// A server that accepts and then says nothing holds nothing open for long.
	raw.SetDeadline(time.Now().Add(idleTimeout))
	session, err := wire.Handshake(raw, c.cfg.Secret, true)
	if err != nil {
		return false, err
	}
	raw.SetDeadline(time.Time{})

	conn := newConn(raw, session.Reader, session.Writer, wire.RoleClient, c.cfg.Server, c.log)
	c.log.Info("connected", "server", c.cfg.Server,
		"provides", len(c.cfg.Provide), "consumes", len(c.cfg.Consume))
	return true, c.endpoint.Serve(ctx, conn)
}

// LogState writes what SIGUSR1 asks for on a client: whether it is connected and what it offers.
func (c *Client) LogState() {
	connected := c.endpoint.connection() != nil
	c.log.Info("client", "server", c.cfg.Server, "connected", connected)
	for _, p := range c.cfg.Provide {
		c.log.Info("provide", "service", p.Service, "upstream", p.Upstream,
			"max_concurrent", p.MaxConcurrent, "priority", p.Priority)
	}
	for _, s := range c.cfg.Consume {
		c.log.Info("consume", "service", s.Service, "listen", s.Listen)
	}
}

// jitter spreads reconnects out, so peers that dropped together do not all come back at once.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

// sleep waits, and says false if the wait was cut short by the daemon stopping.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
