package peer

// An endpoint is a peer's own [provide] and [consume] sections. A client has one and so does the
// server; the only difference is what carries its connection.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/henu/httppooler/internal/conf"
	"github.com/henu/httppooler/internal/wire"
)

// Options are the ways out of the process. The tests replace them with pipes, so the whole test suite
// opens no port and dials nothing.
type Options struct {
	Listen func(ctx context.Context, network, address string) (net.Listener, error)
	Dial   func(ctx context.Context, network, address string) (net.Conn, error)
}

// listen opens a listening socket, or whatever stands in for one.
func (o Options) listen(ctx context.Context, address string) (net.Listener, error) {
	if o.Listen != nil {
		return o.Listen(ctx, "tcp", address)
	}
	config := net.ListenConfig{KeepAlive: pingEvery}
	return config.Listen(ctx, "tcp", address)
}

// dial opens a connection, or whatever stands in for one.
func (o Options) dial(ctx context.Context, address string) (net.Conn, error) {
	if o.Dial != nil {
		return o.Dial(ctx, "tcp", address)
	}
	dialer := net.Dialer{KeepAlive: pingEvery}
	return dialer.DialContext(ctx, "tcp", address)
}

// Endpoint is one peer's local sections: the upstreams it provides and the ports it consumes on.
type Endpoint struct {
	name     string
	provides []conf.Provide
	byName   map[string]conf.Provide
	consumes []conf.Consume
	opts     Options
	log      *slog.Logger
	upstream *http.Client

	listeners []net.Listener
	servers   []*http.Server

	mu      sync.Mutex
	conn    *Conn
	running map[string]int
}

// NewEndpoint reads a conf's own sections. Nothing is opened yet.
func NewEndpoint(cfg *conf.Conf, opts Options, log *slog.Logger) *Endpoint {
	e := &Endpoint{
		name:     cfg.Name,
		provides: cfg.Provide,
		byName:   make(map[string]conf.Provide, len(cfg.Provide)),
		consumes: cfg.Consume,
		opts:     opts,
		log:      log,
		running:  map[string]int{},
	}
	for _, p := range cfg.Provide {
		e.byName[p.Service] = p
	}

	// The upstream client is a proxy's: it follows nothing, decodes nothing, and waits as long as the
	// upstream takes. Bodies are payload, and payload is none of this program's business.
	e.upstream = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			DialContext:         func(ctx context.Context, _, address string) (net.Conn, error) { return opts.dial(ctx, address) },
			DisableCompression:  true,
			MaxIdleConnsPerHost: 64,
		},
	}
	return e
}

// Start opens the consume ports. They stay open for as long as the daemon runs, connected or not: an
// application that asks while the peer has no connection is told 503 rather than refused a socket.
func (e *Endpoint) Start(ctx context.Context) error {
	for _, c := range e.consumes {
		listener, err := e.opts.listen(ctx, c.Listen)
		if err != nil {
			e.Close()
			return fmt.Errorf("consume %q: %w", c.Service, err)
		}

		service := c.Service
		server := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				e.consume(service, w, r)
			}),
		}
		e.listeners = append(e.listeners, listener)
		e.servers = append(e.servers, server)

		e.log.Info("consuming", "service", service, "listen", c.Listen)
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				e.log.Error("consume port closed", "service", service, "error", err)
			}
		}()
	}
	return nil
}

// Close shuts the consume ports.
func (e *Endpoint) Close() error {
	for _, server := range e.servers {
		server.Close()
	}
	for _, listener := range e.listeners {
		listener.Close()
	}
	e.servers = nil
	e.listeners = nil
	return nil
}

// Serve runs one session to its end: HELLO, then every message until the connection dies.
func (e *Endpoint) Serve(ctx context.Context, c *Conn) error {
	if err := c.send(e.hello()); err != nil {
		return err
	}

	e.attach(c)
	defer e.attach(nil)
	defer c.Close()

	return c.run(ctx, e.provide)
}

// hello is what this peer announces: its name and every upstream it provides.
func (e *Endpoint) hello() *wire.Hello {
	m := &wire.Hello{Name: e.name}
	for _, p := range e.provides {
		m.Providers = append(m.Providers, wire.ProviderInfo{
			Service:       p.Service,
			MaxConcurrent: p.MaxConcurrent,
			Priority:      p.Priority,
		})
	}
	return m
}

// attach makes c the connection this peer's consumers submit on, or takes it away again.
func (e *Endpoint) attach(c *Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.conn = c
}

// connection is the session to submit on, or nil while there is none.
func (e *Endpoint) connection() *Conn {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.conn
}

// take claims one of a provider's slots, and refuses when they are all in use. A peer that is sent more
// concurrent jobs than it announced slots for has a peer that is not keeping count.
func (e *Endpoint) take(service string, slots uint16) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running[service] >= int(slots) {
		return false
	}
	e.running[service]++
	return true
}

// give frees a slot taken by take.
func (e *Endpoint) give(service string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.running[service]--
}
