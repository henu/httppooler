package peer

// The provider side of a job: run it against the local upstream and send back what the upstream says,
// as it says it.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/henu/httppooler/internal/conf"
	"github.com/henu/httppooler/internal/wire"
)

// provide runs one job the peer at the other end sent. It is the serve function of every endpoint's
// connection, on a client and on the server alike.
func (e *Endpoint) provide(ctx context.Context, c *Conn, req *wire.Request, in *jobQueue) {
	defer c.endJob(req.Job)

	// The server only dispatches to upstreams a peer announced, so a service this peer does not have
	// is a job nobody can run.
	p, ok := e.byName[req.Service]
	if !ok {
		c.send(&wire.Fail{Job: req.Job, Reason: fmt.Sprintf("this peer provides no %q", req.Service)})
		return
	}

	// A provider that is sent more concurrent jobs than the slots it announced has a peer that is not
	// keeping count, and the connection is no longer one both sides agree about.
	if !e.take(req.Service, p.MaxConcurrent) {
		c.fail(fmt.Errorf("peer: more than %d jobs of %q at once", p.MaxConcurrent, req.Service))
		return
	}
	defer e.give(req.Service)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	body, _, err := collect(ctx, in)
	if err != nil {
		c.send(&wire.Fail{Job: req.Job, Reason: reason(ctx, err)})
		return
	}

	// The body is in; from here the job is the upstream's, and the queue is watched only for a CANCEL.
	go watchCancel(ctx, in, cancel)

	if err := e.runUpstream(ctx, c, req, p, body); err != nil {
		e.log.Info("job failed", "peer", c.Peer(), "service", req.Service, "error", err)
		c.send(&wire.Fail{Job: req.Job, Reason: reason(ctx, err)})
	}
}

// runUpstream is the HTTP request against the local upstream, from the request line to the last trailer.
func (e *Endpoint) runUpstream(ctx context.Context, c *Conn, req *wire.Request, p conf.Provide, body []byte) error {
	// The target is the request-target as the application wrote it, and an origin server's is a path.
	if !strings.HasPrefix(req.Target, "/") {
		return fmt.Errorf("request-target %q is not a path", req.Target)
	}

	request, err := http.NewRequestWithContext(ctx, req.Method, "http://"+p.Upstream+req.Target, bytes.NewReader(body))
	if err != nil {
		return err
	}

	// Host is the upstream's, as any reverse proxy sets it. Content-Length is Go's to write from the
	// body it has. A User-Agent the application did not send is not one this program invents.
	request.Host = p.Upstream
	request.Header = toHTTP(req.Headers, "Content-Length")
	request.ContentLength = int64(len(body))
	if _, ok := request.Header["User-Agent"]; !ok {
		request.Header["User-Agent"] = nil
	}

	response, err := e.upstream.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// The status travels as a u16 in the range a status line can hold.
	if response.StatusCode < 100 || response.StatusCode > 599 {
		return fmt.Errorf("upstream answered with status %d", response.StatusCode)
	}

	// RESPONSE goes out as soon as the headers arrive, and each piece of body as it arrives. That is
	// all streaming needs.
	if err := c.send(&wire.Response{
		Job:     req.Job,
		Status:  uint16(response.StatusCode),
		Headers: fromHTTP(response.Header),
	}); err != nil {
		return err
	}

	buf := make([]byte, bodyChunk)
	for {
		n, err := response.Body.Read(buf)
		if n > 0 {
			if err := c.send(&wire.Body{Job: req.Job, Bytes: buf[:n]}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return c.send(&wire.End{Job: req.Job, Trailers: fromHTTP(response.Trailer)})
}
