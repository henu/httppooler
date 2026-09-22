package peer

// Whole peers, end to end: a server, clients, fake upstreams and real HTTP clients.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/henu/httppooler/internal/wire"
)

// serverConf is a server with one consume port, and whatever else the test adds.
func serverConf(extra string) string {
	return `
[peer]
role = server
listen = pool:7420
secret = ` + secret + `
name = pooler
queue_timeout = 5s
` + extra
}

// clientConf is a client of that server.
func clientConf(name, extra string) string {
	return `
[peer]
role = client
server = pool:7420
secret = ` + secret + `
name = ` + name + `
` + extra
}

// TestRequestReachesTheProvider is the whole path in one test: an application's request goes in at a
// consume port on one peer and comes out at an upstream on another, and the answer comes back.
func TestRequestReachesTheProvider(t *testing.T) {
	h := newHarness(t)

	saw := make(chan observed, 1)
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		saw <- observed{method: r.Method, target: r.URL.RequestURI(), host: r.Host,
			headers: r.Header.Clone(), body: string(body)}

		w.Header().Set("X-Upstream", "kotikone")
		w.Header().Add("X-Twice", "one")
		w.Header().Add("X-Twice", "two")
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "pong")
	})

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
`))
	h.waitProviders(s, "echo", 1)

	request, _ := http.NewRequest("POST", "http://front:8080/api/generate?stream=true", strings.NewReader("ping"))
	request.Header.Set("X-Test", "yes")
	request.Header.Set("Content-Type", "application/json")
	response, err := h.http.Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusCreated || string(body) != "pong" {
		t.Fatalf("answered %d %q, want 201 pong", response.StatusCode, body)
	}
	if got := response.Header.Get("X-Upstream"); got != "kotikone" {
		t.Errorf("X-Upstream is %q", got)
	}
	if got := response.Header.Values("X-Twice"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Errorf("X-Twice came back as %v, want both values in order", got)
	}

	up := <-saw
	if up.method != "POST" || up.target != "/api/generate?stream=true" || up.body != "ping" {
		t.Fatalf("the upstream saw %+v", up)
	}
	if up.host != "up:9000" {
		t.Errorf("the upstream saw Host %q, want its own address", up.host)
	}
	if up.headers.Get("X-Test") != "yes" || up.headers.Get("Content-Type") != "application/json" {
		t.Errorf("the upstream saw headers %v", up.headers)
	}
	if up.headers.Get("Content-Length") != "4" {
		t.Errorf("the upstream saw Content-Length %q, want 4", up.headers.Get("Content-Length"))
	}
}

// TestServerServesItself is the server's own [provide] and [consume] sections, with no client anywhere:
// the same code, over a pipe.
func TestServerServesItself(t *testing.T) {
	h := newHarness(t)
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "from the server's own upstream")
	})

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080

[provide "echo"]
upstream = up:9000
`))
	h.waitProviders(s, "echo", 1)

	response, body := h.get("http://front:8080/")
	if response.StatusCode != 200 || body != "from the server's own upstream" {
		t.Fatalf("answered %d %q", response.StatusCode, body)
	}
}

// TestRoutesByService keeps two services apart on one connection.
func TestRoutesByService(t *testing.T) {
	h := newHarness(t)
	h.upstream("alpha:9000", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "alpha") })
	h.upstream("beta:9000", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "beta") })

	s := h.server(serverConf(`
[consume "alpha"]
listen = front-alpha:8080

[consume "beta"]
listen = front-beta:8080
`))
	h.client(clientConf("kotikone", `
[provide "alpha"]
upstream = alpha:9000

[provide "beta"]
upstream = beta:9000
`))
	h.waitProviders(s, "alpha", 1)
	h.waitProviders(s, "beta", 1)

	if _, body := h.get("http://front-alpha:8080/"); body != "alpha" {
		t.Errorf("the alpha port answered %q", body)
	}
	if _, body := h.get("http://front-beta:8080/"); body != "beta" {
		t.Errorf("the beta port answered %q", body)
	}
}

// TestPriorityWins sends work to the smallest priority number while it has a free slot, and to the next
// one only when it has none.
func TestPriorityWins(t *testing.T) {
	h := newHarness(t)
	fast, fallback := newGate(), newGate()
	h.upstream("fast:9000", fast.handler("fast"))
	h.upstream("slow:9000", fallback.handler("fallback"))

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = fast:9000
max_concurrent = 1
priority = 100
`))
	h.client(clientConf("varakone", `
[provide "echo"]
upstream = slow:9000
max_concurrent = 1
priority = 200
`))
	h.waitProviders(s, "echo", 2)

	first := h.fire(h.ctx, "GET", "http://front:8080/first", "")
	fast.waitArrival(t)

	// The fast peer is busy, so the next job goes to the one behind it.
	second := h.fire(h.ctx, "GET", "http://front:8080/second", "")
	fallback.waitArrival(t)

	fast.let()
	fallback.let()
	if got := wait(t, first).body; got != "fast" {
		t.Errorf("the first job was served by %q, want the smallest priority", got)
	}
	if got := wait(t, second).body; got != "fallback" {
		t.Errorf("the second job was served by %q, want the overflow", got)
	}
}

// TestSlotsAreRespected never gives a provider more jobs at once than it announced.
func TestSlotsAreRespected(t *testing.T) {
	h := newHarness(t)

	gate := newGate()
	h.upstream("up:9000", gate.handler("done"))

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
max_concurrent = 2
`))
	h.waitProviders(s, "echo", 1)

	const jobs = 6
	answers := make([]<-chan answer, jobs)
	for i := range answers {
		answers[i] = h.fire(h.ctx, "GET", fmt.Sprintf("http://front:8080/%d", i), "")
	}

	// Two jobs reach the upstream; the rest wait for a slot.
	gate.waitArrival(t)
	gate.waitArrival(t)
	h.waitQueue(s, "echo", jobs-2)

	select {
	case path := <-gate.arrived:
		t.Fatalf("a third job reached an upstream with two slots: %s", path)
	case <-time.After(100 * time.Millisecond):
	}

	for i := 0; i < jobs; i++ {
		gate.let()
	}
	for i, done := range answers {
		if got := wait(t, done); got.status != 200 || got.body != "done" {
			t.Fatalf("job %d answered %d %q", i, got.status, got.body)
		}
	}
}

// TestOldestFirst is the queue rule seen from outside: jobs reach the upstream in the order they were
// submitted.
func TestOldestFirst(t *testing.T) {
	h := newHarness(t)

	gate := newGate()
	h.upstream("up:9000", gate.handler("done"))

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
max_concurrent = 1
`))
	h.waitProviders(s, "echo", 1)

	paths := []string{"/one", "/two", "/three", "/four"}
	for i, path := range paths {
		h.fire(h.ctx, "GET", "http://front:8080"+path, "")
		if i == 0 {
			// The first job takes the only slot; the rest queue behind it, one at a time so that
			// the order they arrive in is the order the test means.
			if got := gate.waitArrival(t); got != path {
				t.Fatalf("the first job was %s", got)
			}
			continue
		}
		h.waitQueue(s, "echo", i)
	}

	for range paths {
		gate.let()
	}
	for _, want := range paths[1:] {
		if got := gate.waitArrival(t); got != want {
			t.Fatalf("the upstream saw %s where %s was next in the queue", got, want)
		}
	}
}

// TestQueueTimeoutIs503 is a service nobody provides: the job waits its whole timeout and is answered
// by the server itself.
func TestQueueTimeoutIs503(t *testing.T) {
	h := newHarness(t)
	h.server(`
[peer]
role = server
listen = pool:7420
secret = ` + secret + `
queue_timeout = 200ms

[consume "nobody"]
listen = front:8080
`)

	start := time.Now()
	response, body := h.get("http://front:8080/")
	waited := time.Since(start)

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("answered %d, want 503", response.StatusCode)
	}
	if !strings.Contains(body, "no provider") || !strings.Contains(body, "nobody") {
		t.Errorf("the 503 body is %q, want the reason", body)
	}
	if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Errorf("the 503 is %q, want text/plain", got)
	}
	if waited < 200*time.Millisecond {
		t.Errorf("gave up after %v, want the whole queue timeout", waited)
	}
}

// TestRequeueWhenTheProviderVanishes is a connection that drops with a job on it: the job goes back to
// the front of its queue and another provider runs it, without the application seeing anything.
func TestRequeueWhenTheProviderVanishes(t *testing.T) {
	h := newHarness(t)

	gate := newGate()
	h.upstream("gone:9000", gate.handler("never"))
	h.upstream("left:9000", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "the one that stayed") })

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	_, cut := h.client(clientConf("vanishing", `
[provide "echo"]
upstream = gone:9000
max_concurrent = 1
priority = 100
`))
	h.client(clientConf("staying", `
[provide "echo"]
upstream = left:9000
max_concurrent = 1
priority = 200
`))
	h.waitProviders(s, "echo", 2)

	done := h.fire(h.ctx, "GET", "http://front:8080/", "")
	gate.waitArrival(t)

	// The peer running the job drops off the network for good.
	cut.cut()

	if got := wait(t, done); got.status != 200 || got.body != "the one that stayed" {
		t.Fatalf("answered %d %q, want the job to have been run again elsewhere", got.status, got.body)
	}
}

// TestCancelReachesTheUpstream is an application that goes away: the CANCEL travels the whole way and
// the upstream's request is aborted.
func TestCancelReachesTheUpstream(t *testing.T) {
	h := newHarness(t)

	arrived := make(chan struct{})
	aborted := make(chan struct{})
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-r.Context().Done()
		close(aborted)
	})

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
`))
	h.waitProviders(s, "echo", 1)

	ctx, cancel := context.WithCancel(h.ctx)
	h.fire(ctx, "GET", "http://front:8080/", "")
	<-arrived
	cancel()

	select {
	case <-aborted:
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream was never told the application had gone")
	}
}

// TestStreamingArrivesInPieces is what makes the pool usable for a model that answers a token at a
// time: each piece reaches the application as the upstream writes it, not when the answer ends.
func TestStreamingArrivesInPieces(t *testing.T) {
	h := newHarness(t)

	second := make(chan struct{})
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		control := http.NewResponseController(w)
		io.WriteString(w, "one")
		control.Flush()
		<-second
		io.WriteString(w, "two")
		control.Flush()
	})

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
`))
	h.waitProviders(s, "echo", 1)

	response, err := h.http.Get("http://front:8080/")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()

	// The first piece is readable while the upstream is still holding the second one.
	first := make([]byte, 3)
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatalf("reading the first piece: %v", err)
	}
	if string(first) != "one" {
		t.Fatalf("the first piece is %q", first)
	}

	close(second)
	rest, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading the rest: %v", err)
	}
	if string(rest) != "two" {
		t.Fatalf("the rest is %q", rest)
	}
}

// TestBigBodiesRoundTrip sends more than any one message or record can hold, both ways.
func TestBigBodiesRoundTrip(t *testing.T) {
	h := newHarness(t)

	const size = 1 << 20
	sent := make([]byte, size)
	for i := range sent {
		sent[i] = byte(i * 7)
	}
	answerBody := make([]byte, size)
	for i := range answerBody {
		answerBody[i] = byte(i * 13)
	}

	got := make(chan [32]byte, 1)
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("upstream reading the body: %v", err)
			return
		}
		got <- sha256.Sum256(body)
		w.Write(answerBody)
	})

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
`))
	h.waitProviders(s, "echo", 1)

	response, err := h.http.Post("http://front:8080/", "application/octet-stream", strings.NewReader(string(sent)))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()
	back, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}

	if <-got != sha256.Sum256(sent) {
		t.Error("the upstream got a different request body from the one that was sent")
	}
	if sha256.Sum256(back) != sha256.Sum256(answerBody) {
		t.Errorf("the answer came back as %d bytes, want %d unchanged", len(back), size)
	}
}

// TestUpstreamFailureIs502 is an upstream that is not there: the provider says why and the server turns
// it into the answer the application sees.
func TestUpstreamFailureIs502(t *testing.T) {
	h := newHarness(t)

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = nothing:9000
`))
	h.waitProviders(s, "echo", 1)

	response, body := h.get("http://front:8080/")
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("answered %d, want 502", response.StatusCode)
	}
	if !strings.Contains(body, "connection refused") {
		t.Errorf("the 502 body is %q, want the reason the upstream failed", body)
	}
}

// TestUpgradeIs501 answers a request that wants to become a WebSocket where it arrives.
func TestUpgradeIs501(t *testing.T) {
	h := newHarness(t)
	h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))

	request, _ := http.NewRequest("GET", "http://front:8080/socket", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	response, err := h.http.Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusNotImplemented {
		t.Fatalf("answered %d, want 501", response.StatusCode)
	}
	if !strings.Contains(string(body), "upgrade") {
		t.Errorf("the 501 body is %q", body)
	}
}

// TestDisconnectedConsumerIs503 is a client whose server is not there: its consume port stays open and
// says so, rather than refusing the connection.
func TestDisconnectedConsumerIs503(t *testing.T) {
	h := newHarness(t)
	h.client(clientConf("lonely", `
[consume "echo"]
listen = front:8080
`))
	h.waitListening("front:8080")

	response, body := h.get("http://front:8080/")
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("answered %d, want 503", response.StatusCode)
	}
	if !strings.Contains(body, "no connection to the server") {
		t.Errorf("the 503 body is %q", body)
	}
}

// TestHopByHopHeadersStay is the rule that headers belonging to one connection do not travel to the
// next, including the ones a Connection header names.
func TestHopByHopHeadersStay(t *testing.T) {
	h := newHarness(t)

	saw := make(chan observed, 1)
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		saw <- observed{headers: r.Header.Clone(), host: r.Host}
		io.WriteString(w, "ok")
	})

	s := h.server(serverConf(`
[consume "echo"]
listen = front:8080
`))
	h.client(clientConf("kotikone", `
[provide "echo"]
upstream = up:9000
`))
	h.waitProviders(s, "echo", 1)

	request, _ := http.NewRequest("GET", "http://front:8080/", nil)
	request.Header.Set("Connection", "X-Per-Hop")
	request.Header.Set("X-Per-Hop", "do not pass")
	request.Header.Set("X-End-To-End", "pass")
	response, err := h.http.Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	response.Body.Close()

	up := <-saw
	for _, name := range []string{"Connection", "X-Per-Hop", "Keep-Alive", "Te", "Transfer-Encoding", "Upgrade"} {
		if got := up.headers.Get(name); got != "" {
			t.Errorf("the upstream saw %s: %q", name, got)
		}
	}
	if got := up.headers.Get("X-End-To-End"); got != "pass" {
		t.Errorf("the upstream saw X-End-To-End %q, want it passed through", got)
	}
}

// TestPingIsAnswered is the liveness exchange, and with it the one place the read loop would otherwise
// write: the PONG comes from a goroutine of its own, so a reading connection never waits on the socket
// it is reading.
func TestPingIsAnswered(t *testing.T) {
	h := newHarness(t)

	here, there := net.Pipe()
	defer here.Close()
	defer there.Close()

	server := newConn(here, here, here, wire.RoleServer, "server", h.log)
	client := newConn(there, there, there, wire.RoleClient, "client", h.log)

	// A client says HELLO before anything else, as it does on a real connection.
	go client.send(&wire.Hello{Name: "client"})
	if _, err := server.hello(); err != nil {
		t.Fatalf("hello: %v", err)
	}
	go client.run(h.ctx, func(context.Context, *Conn, *wire.Request, *jobQueue) {})

	go server.send(&wire.Ping{})
	if err := here.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}

	m, err := server.dec.Next()
	if err != nil {
		t.Fatalf("no answer to the PING: %v", err)
	}
	if _, ok := m.(*wire.Pong); !ok {
		t.Fatalf("the peer answered a PING with %s", m.Kind())
	}
}

// TestConsumerOnlyClient is the three-machine shape: one client only consumes, another only provides, and
// the server relays between two connections rather than between a connection and its own sections. The
// consuming peer announces no providers at all, which is a HELLO with an empty list.
func TestConsumerOnlyClient(t *testing.T) {
	h := newHarness(t)

	saw := make(chan observed, 1)
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		saw <- observed{method: r.Method, target: r.URL.RequestURI(), body: string(body)}
		io.WriteString(w, "answered at the far end")
	})

	// The server holds the pool and nothing else: no [consume], no [provide].
	s := h.server(serverConf(""))
	h.client(clientConf("kotikone", `
[provide "ollama"]
upstream = up:9000
`))
	third, _ := h.client(clientConf("kolmas", `
[consume "ollama"]
listen = front:8080
`))
	h.waitProviders(s, "ollama", 1)
	h.waitConnected(third)

	response, body := h.get("http://front:8080/api/tags")
	if response.StatusCode != 200 || body != "answered at the far end" {
		t.Fatalf("answered %d %q", response.StatusCode, body)
	}
	if up := <-saw; up.target != "/api/tags" {
		t.Fatalf("the upstream saw %+v", up)
	}
}

// TestConsumerOnlyClientGets503 is the same shape with the providing peer gone: the pool answers, and the
// answer travels back over the consuming peer's own connection.
func TestConsumerOnlyClientGets503(t *testing.T) {
	h := newHarness(t)

	h.server(`
[peer]
role = server
listen = pool:7420
secret = ` + secret + `
queue_timeout = 200ms
`)
	third, _ := h.client(clientConf("kolmas", `
[consume "ollama"]
listen = front:8080
`))
	h.waitConnected(third)

	response, body := h.get("http://front:8080/api/tags")
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("answered %d, want 503", response.StatusCode)
	}
	if !strings.Contains(body, "no provider") {
		t.Errorf("the 503 body is %q", body)
	}
}

// TestPeerConsumesWhatItProvides is one peer with both sections for one service: its request goes out to
// the server and comes straight back to it. Both halves live on the one connection, told apart only by the
// job id's parity, so this is the case where that rule earns its keep.
func TestPeerConsumesWhatItProvides(t *testing.T) {
	h := newHarness(t)
	h.upstream("up:9000", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "round trip")
	})

	s := h.server(serverConf(""))
	c, _ := h.client(clientConf("psychotron", `
[provide "ollama"]
upstream = up:9000

[consume "ollama"]
listen = front:8080
`))
	h.waitProviders(s, "ollama", 1)
	h.waitConnected(c)

	response, body := h.get("http://front:8080/api/tags")
	if response.StatusCode != 200 || body != "round trip" {
		t.Fatalf("answered %d %q", response.StatusCode, body)
	}
}
