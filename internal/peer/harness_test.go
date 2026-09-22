package peer

// The integration tests run whole peers in one process: a server, clients, fake upstreams and real HTTP
// clients, all of them on the in-memory network. Nothing is started and no port is opened.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/henu/httppooler/internal/conf"
)

// secret is the shared key every peer in the tests uses, as a conf writes it.
const secret = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// harness is one test's world.
type harness struct {
	t    *testing.T
	net  *memNet
	log  *slog.Logger
	http *http.Client
	ctx  context.Context
	stop context.CancelFunc
}

// newHarness builds an empty world and takes it down when the test ends.
func newHarness(t *testing.T) *harness {
	t.Helper()

	output := io.Discard
	if os.Getenv("HTTPPOOLER_TEST_LOG") != "" {
		output = os.Stderr
	}

	ctx, stop := context.WithCancel(context.Background())
	h := &harness{
		t:    t,
		net:  newMemNet(),
		log:  slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: slog.LevelDebug})),
		ctx:  ctx,
		stop: stop,
	}
	h.http = &http.Client{Transport: &http.Transport{DialContext: h.net.Dial, DisableCompression: true}}

	t.Cleanup(func() {
		stop()
		h.http.CloseIdleConnections()
	})
	return h
}

// upstream puts a fake HTTP service on an address, the way a real one would sit on a local port.
func (h *harness) upstream(address string, handler http.HandlerFunc) {
	h.t.Helper()

	listener, err := h.net.Listen(h.ctx, "tcp", address)
	if err != nil {
		h.t.Fatalf("upstream %s: %v", address, err)
	}

	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	h.t.Cleanup(func() { server.Close() })
}

// server starts the peer that listens, and waits until it is listening.
func (h *harness) server(text string) *Server {
	h.t.Helper()

	cfg := h.conf(text)
	s := NewServer(cfg, Options{Listen: h.net.Listen, Dial: h.net.Dial}, h.log)
	go s.Run(h.ctx)
	h.waitListening(cfg.Listen)

	// A server with sections of its own is not ready until they are on their pipe: until then its own
	// consume ports answer that they have no connection, exactly as a client's would.
	if len(cfg.Provide) > 0 || len(cfg.Consume) > 0 {
		waitUntil(h.t.Fatal, func() bool { return s.endpoint.connection() != nil })
	}
	return s
}

// client starts a peer that dials, and hands back the cable its connections run through so a test can
// cut it.
func (h *harness) client(text string) (*Client, *cable) {
	h.t.Helper()

	cfg := h.conf(text)
	wire := &cable{net: h.net}
	c := NewClient(cfg, Options{Listen: h.net.Listen, Dial: wire.dial}, h.log)
	go c.Run(h.ctx)
	return c, wire
}

// conf parses a conf the way the daemon does, so the tests use the same grammar an operator does.
func (h *harness) conf(text string) *conf.Conf {
	h.t.Helper()

	cfg, err := conf.Parse(strings.NewReader(text), "test.conf")
	if err != nil {
		h.t.Fatalf("conf: %v", err)
	}
	return cfg
}

// waitListening waits until something holds an address.
func (h *harness) waitListening(address string) {
	h.t.Helper()
	waitUntil(h.t.Fatal, func() bool {
		h.net.mu.Lock()
		defer h.net.mu.Unlock()
		_, ok := h.net.listeners[address]
		return ok
	})
}

// waitConnected waits until a client has a session with the server. A consume port is open before that
// and answers 503 while it lasts, so a test that means to reach the pool waits here first.
func (h *harness) waitConnected(c *Client) {
	h.t.Helper()
	waitUntil(h.t.Fatal, func() bool { return c.endpoint.connection() != nil })
}

// waitProviders waits until a service's pool holds the number of upstreams a test is about to rely on.
func (h *harness) waitProviders(s *Server, service string, want int) {
	h.t.Helper()
	waitUntil(h.t.Fatal, func() bool { return providers(s, service) == want })
}

// waitQueue waits until a service's queue is the length a test is about to rely on.
func (h *harness) waitQueue(s *Server, service string, want int) {
	h.t.Helper()
	waitUntil(h.t.Fatal, func() bool {
		for _, state := range s.pool.Snapshot().Services {
			if state.Name == service {
				return state.Waiting == want
			}
		}
		return want == 0
	})
}

// providers is how many upstreams a service has right now.
func providers(s *Server, service string) int {
	for _, state := range s.pool.Snapshot().Services {
		if state.Name == service {
			return len(state.Providers)
		}
	}
	return 0
}

// get asks a consume port for a path and reads the whole answer.
func (h *harness) get(url string) (*http.Response, string) {
	h.t.Helper()

	response, err := h.http.Get(url)
	if err != nil {
		h.t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatalf("GET %s: reading the body: %v", url, err)
	}
	return response, string(body)
}

// gate is a fake upstream a test lets through one answer at a time: the handler says which request
// arrived, and waits until the test releases it.
type gate struct {
	arrived chan string
	release chan struct{}
}

func newGate() *gate {
	return &gate{arrived: make(chan string, 64), release: make(chan struct{}, 64)}
}

// handler answers with the text given, once the test lets it.
func (g *gate) handler(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		g.arrived <- r.URL.Path
		select {
		case <-g.release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, name)
	}
}

// waitArrival takes the next request the upstream saw.
func (g *gate) waitArrival(t *testing.T) string {
	t.Helper()
	select {
	case path := <-g.arrived:
		return path
	case <-time.After(5 * time.Second):
		t.Fatal("no request reached the upstream")
		return ""
	}
}

// let releases one answer.
func (g *gate) let() {
	g.release <- struct{}{}
}

// answer is what a fired request came back with.
type answer struct {
	status  int
	body    string
	headers http.Header
	err     error
}

// fire sends a request without waiting for it, which is how a test has several in flight at once.
func (h *harness) fire(ctx context.Context, method, url, body string) <-chan answer {
	done := make(chan answer, 1)
	go func() {
		request, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
		if err != nil {
			done <- answer{err: err}
			return
		}

		response, err := h.http.Do(request)
		if err != nil {
			done <- answer{err: err}
			return
		}
		defer response.Body.Close()

		read, err := io.ReadAll(response.Body)
		done <- answer{status: response.StatusCode, body: string(read), headers: response.Header, err: err}
	}()
	return done
}

// wait takes a fired request's answer, or fails the test as stuck.
func wait(t *testing.T, done <-chan answer) answer {
	t.Helper()
	select {
	case a := <-done:
		if a.err != nil {
			t.Fatalf("request failed: %v", a.err)
		}
		return a
	case <-time.After(10 * time.Second):
		t.Fatal("a request was never answered")
		return answer{}
	}
}

// observed is what a fake upstream saw of a request.
type observed struct {
	method  string
	target  string
	host    string
	headers http.Header
	body    string
}
