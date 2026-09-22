// Package peer is everything above the wire: the connections peers keep, the providers that run jobs
// against a local upstream, the consumers that take HTTP in, and the server that queues between them.
//
// A client's [provide] and [consume] sections and the server's own run the same code. The server's sit
// on a pipe instead of a socket and skip the preamble, the handshake and the records; from layer 3 up
// nothing else is different, and there is no second path through the program for them.
package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/henu/httppooler/internal/wire"
)

const (
	// pingEvery is how often the server asks whether a client is still there.
	pingEvery = 30 * time.Second
	// idleTimeout is how long a peer waits to hear anything at all before it closes the connection.
	idleTimeout = 90 * time.Second
	// bodyChunk is how much of a body one BODY message carries.
	bodyChunk = 32 * 1024
)

// serveFunc runs one job the peer at the other end started. It owns the job until it sends the job's
// last message, and it is the only reader of in.
type serveFunc func(ctx context.Context, c *Conn, req *wire.Request, in *jobQueue)

// Conn is one live session with one peer: the codec, the jobs on it, and the liveness that ends it.
//
// Both ends of a job travel on the same connection, told apart by the job id's parity: the server's
// jobs are even and a client's odd. A message about a job the connection no longer knows is ignored,
// which is how a cancelled job's tail is let through.
type Conn struct {
	raw  net.Conn
	enc  *wire.Encoder
	dec  *wire.Decoder
	role wire.Role
	peer string
	log  *slog.Logger

	writeMu sync.Mutex

	// pongs carries answers to PING from the read loop to a goroutine that writes them. It is the
	// one thing the read loop would otherwise wait for, and the read loop waiting is what makes two
	// peers relaying to each other able to wedge.
	pongs chan struct{}

	mu      sync.Mutex
	nextJob uint64
	jobs    map[uint64]*jobQueue
	err     error
}

// newConn wraps a transport. r and w are the plaintext streams: the record streams of an encrypted
// connection, or raw itself for the pipe the server's own sections sit on.
func newConn(raw net.Conn, r io.Reader, w io.Writer, role wire.Role, peer string, log *slog.Logger) *Conn {
	first := uint64(1)
	if role == wire.RoleServer {
		first = 2
	}
	return &Conn{
		raw:     raw,
		enc:     wire.NewEncoder(w, role),
		dec:     wire.NewDecoder(r, role),
		role:    role,
		peer:    peer,
		log:     log,
		pongs:   make(chan struct{}, 1),
		nextJob: first,
		jobs:    map[uint64]*jobQueue{},
	}
}

// Peer is the name of the peer at the other end, for logs.
func (c *Conn) Peer() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peer
}

// setPeer names the peer at the other end, which the server learns from its HELLO.
func (c *Conn) setPeer(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peer = name
}

// hello reads a client's first message, which is its HELLO. The decoder refuses anything else there, so
// this is where a peer that starts talking about jobs straight away is turned away.
func (c *Conn) hello() (*wire.Hello, error) {
	if err := c.raw.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
		return nil, err
	}

	m, err := c.dec.Next()
	if err != nil {
		return nil, err
	}

	hello, ok := m.(*wire.Hello)
	if !ok {
		return nil, fmt.Errorf("peer: first message is %s, want HELLO", m.Kind())
	}
	return hello, nil
}

// send writes one message. Messages of concurrent jobs interleave freely, but each one goes out whole,
// so this is where writers queue up behind each other.
func (c *Conn) send(m wire.Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// A peer that has stopped reading is as dead as one that has stopped writing.
	if err := c.raw.SetWriteDeadline(time.Now().Add(idleTimeout)); err != nil {
		return err
	}
	return c.enc.Write(m)
}

// sendBodyEnd splits a body into BODY messages and ends it with END. It is how every side of every hop
// writes a body it already has whole.
func (c *Conn) sendBodyEnd(job uint64, body []byte, trailers []wire.Header) error {
	for len(body) > 0 {
		chunk := body
		if len(chunk) > bodyChunk {
			chunk = chunk[:bodyChunk]
		}
		if err := c.send(&wire.Body{Job: job, Bytes: chunk}); err != nil {
			return err
		}
		body = body[len(chunk):]
	}
	return c.send(&wire.End{Job: job, Trailers: trailers})
}

// newJob takes the next job id of this side's parity and opens its queue. The caller ends the job with
// endJob whatever happens to it.
func (c *Conn) newJob() (uint64, *jobQueue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return 0, nil, c.err
	}

	id := c.nextJob
	c.nextJob += 2
	q := newJobQueue()
	c.jobs[id] = q
	return id, q, nil
}

// openJob opens the queue of a job the peer started.
func (c *Conn) openJob(id uint64) (*jobQueue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return nil, c.err
	}

	// A job id is never reused on a connection, so a second REQUEST for one is a peer out of step.
	if _, ok := c.jobs[id]; ok {
		return nil, fmt.Errorf("job %d is already running", id)
	}

	q := newJobQueue()
	c.jobs[id] = q
	return q, nil
}

// endJob forgets a job. Messages that arrive for it afterwards are ignored, which is what the consumer
// side of a cancelled job does with the tail of its answer.
func (c *Conn) endJob(id uint64) {
	c.mu.Lock()
	q := c.jobs[id]
	delete(c.jobs, id)
	c.mu.Unlock()

	if q != nil {
		q.close(errJobEnded)
	}
}

// errJobEnded closes the queue of a job whose owner has finished with it.
var errJobEnded = errors.New("peer: job ended")

// mine says whether a job id belongs to this side's numbering, which is what makes it the consumer side
// of that job.
func (c *Conn) mine(id uint64) bool {
	even := id%2 == 0
	return even == (c.role == wire.RoleServer)
}

// fail ends the connection and every job on it with the same reason.
func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	jobs := make([]*jobQueue, 0, len(c.jobs))
	for id, q := range c.jobs {
		jobs = append(jobs, q)
		delete(c.jobs, id)
	}
	c.mu.Unlock()

	for _, q := range jobs {
		q.close(err)
	}
	c.raw.Close()
}

// Close ends the connection the way a shutdown does.
func (c *Conn) Close() error {
	c.fail(net.ErrClosed)
	return nil
}

// run is the read loop: every message either keeps the connection alive, starts a job, or belongs to
// one that is already running. It returns when the connection ends, and the connection ends whenever
// anything at all is wrong with what came in.
func (c *Conn) run(ctx context.Context, serve serveFunc) error {
	// A read is only woken by the socket, so the daemon stopping has to close it. The watcher lives
	// exactly as long as this loop does.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		<-ctx.Done()
		c.fail(ctx.Err())
	}()
	go c.pong(ctx)

	for {
		// Any received message counts as life; nothing at all for this long does not.
		if err := c.raw.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
			c.fail(err)
			return err
		}

		m, err := c.dec.Next()
		if err != nil {
			c.fail(err)
			return err
		}

		if err := c.dispatch(ctx, m, serve); err != nil {
			c.fail(err)
			return err
		}
	}
}

// dispatch hands one message to whatever it is about.
func (c *Conn) dispatch(ctx context.Context, m wire.Message, serve serveFunc) error {
	switch m := m.(type) {
	case *wire.Ping:
		// One pending PONG answers the question a PING asks, so a second one while the first is
		// still unwritten would say nothing more.
		select {
		case c.pongs <- struct{}{}:
		default:
		}
		return nil
	case *wire.Pong:
		return nil
	case *wire.Hello:
		return errors.New("peer: HELLO in the middle of a session")
	case *wire.Request:
		return c.start(ctx, m, serve)
	}

	jobbed, ok := m.(wire.Jobbed)
	if !ok {
		return fmt.Errorf("peer: %s belongs to no job", m.Kind())
	}

	c.mu.Lock()
	q := c.jobs[jobbed.JobID()]
	c.mu.Unlock()

	// A job nobody is running any more is one that ended early; its last messages are let through.
	if q == nil {
		c.log.Debug("message for a job that ended", "peer", c.Peer(), "job", jobbed.JobID(), "kind", m.Kind().String())
		return nil
	}

	q.push(m)
	return nil
}

// start opens a job the peer asked for and puts a goroutine on it.
func (c *Conn) start(ctx context.Context, req *wire.Request, serve serveFunc) error {
	// The two sides number their own jobs; a peer using this side's parity is out of step.
	if c.mine(req.Job) {
		return fmt.Errorf("peer: REQUEST for job %d, which is this side's to number", req.Job)
	}

	q, err := c.openJob(req.Job)
	if err != nil {
		return err
	}

	go serve(ctx, c, req, q)
	return nil
}

// pong writes the answers to the PINGs the read loop took in. It is a goroutine of its own so that the
// read loop never blocks on anything: a read loop that waits is a read loop that is not draining, and a
// peer that is not draining is one the other end's writers pile up behind.
func (c *Conn) pong(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.pongs:
			if err := c.send(&wire.Pong{}); err != nil {
				return
			}
		}
	}
}

// ping keeps asking the peer whether it is still there, until the connection ends.
func (c *Conn) ping(ctx context.Context) {
	ticker := time.NewTicker(pingEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.send(&wire.Ping{}); err != nil {
				return
			}
		}
	}
}
