package peer

// The consumer side of a job: take an application's HTTP request in on a local port, put it in the
// pool, and write back what comes out, as it comes out.

import (
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"

	"github.com/henu/httppooler/internal/wire"
)

// consume is the handler of one [consume] section's port.
func (e *Endpoint) consume(service string, w http.ResponseWriter, r *http.Request) {
	// The pool carries requests and responses. A connection that wants to become something else is
	// answered where it arrives.
	if isUpgrade(r) {
		synth(w, http.StatusNotImplemented, "upgrade requests are not pooled")
		return
	}

	// The whole request body is read before the job is submitted, so the server can hand the job to
	// another provider if the first one vanishes.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		synth(w, http.StatusBadGateway, "reading the request body: "+err.Error())
		return
	}

	c := e.connection()
	if c == nil {
		synth(w, http.StatusServiceUnavailable, "this peer has no connection to the server")
		return
	}

	job, in, err := c.newJob()
	if err != nil {
		synth(w, http.StatusServiceUnavailable, "the connection to the server ended: "+err.Error())
		return
	}
	defer c.endJob(job)

	if err := c.send(&wire.Request{
		Job:     job,
		Service: service,
		Method:  r.Method,
		Target:  target(r),
		Headers: requestHeaders(r, len(body)),
	}); err != nil {
		synth(w, http.StatusServiceUnavailable, "sending the request: "+err.Error())
		return
	}
	if err := c.sendBodyEnd(job, body, fromHTTP(r.Trailer)); err != nil {
		synth(w, http.StatusServiceUnavailable, "sending the request body: "+err.Error())
		return
	}

	e.relayAnswer(c, job, in, w, r)
}

// relayAnswer writes back what the job's provider side says, in the order it says it. Once the status
// line is out there is no way left to report a failure, so a job that dies after it takes the
// application's connection down with it.
func (e *Endpoint) relayAnswer(c *Conn, job uint64, in *jobQueue, w http.ResponseWriter, r *http.Request) {
	control := http.NewResponseController(w)
	started := false

	for {
		m, err := in.next(r.Context())
		if err != nil {
			// The application went away: the job is cancelled at its provider and forgotten here.
			if r.Context().Err() != nil {
				c.send(&wire.Cancel{Job: job})
				return
			}
			e.answerFailure(w, started, "the connection to the server ended: "+err.Error())
			return
		}

		switch m := m.(type) {
		case *wire.Response:
			writeHead(w, m)
			control.Flush()
			started = true
		case *wire.Body:
			if _, err := w.Write(m.Bytes); err != nil {
				c.send(&wire.Cancel{Job: job})
				return
			}
			control.Flush()
		case *wire.End:
			writeTrailers(w, m.Trailers)
			return
		case *wire.Fail:
			e.answerFailure(w, started, m.Reason)
			return
		default:
			e.answerFailure(w, started, fmt.Sprintf("%s where an answer belongs", m.Kind()))
			return
		}
	}
}

// answerFailure is how a job that did not finish reaches the application: 502 while there is still a
// status line to write, and a broken connection once there is not.
func (e *Endpoint) answerFailure(w http.ResponseWriter, started bool, why string) {
	if started {
		// The application has the status and part of the body; the only honest end is no end.
		panic(http.ErrAbortHandler)
	}
	synth(w, http.StatusBadGateway, why)
}

// writeHead copies a RESPONSE onto the application's connection.
func writeHead(w http.ResponseWriter, m *wire.Response) {
	head := w.Header()
	for _, header := range m.Headers {
		name := textproto.CanonicalMIMEHeaderKey(header.Name)
		if hopByHop[name] {
			continue
		}
		head.Add(name, header.Value)
	}
	w.WriteHeader(int(m.Status))
}

// writeTrailers writes the trailers an END carried. Nothing uses them yet; they are carried because
// they are part of the answer.
func writeTrailers(w http.ResponseWriter, trailers []wire.Header) {
	head := w.Header()
	for _, trailer := range trailers {
		head.Add(http.TrailerPrefix+textproto.CanonicalMIMEHeaderKey(trailer.Name), trailer.Value)
	}
}

// requestHeaders is what travels with a REQUEST: everything the application sent but the hop-by-hop
// headers, with Content-Length rewritten to the body that is actually being sent.
func requestHeaders(r *http.Request, length int) []wire.Header {
	headers := fromHTTP(r.Header, "Content-Length")

	// Transfer-Encoding does not travel, so a body that arrived chunked travels with its length. An
	// empty body keeps its Content-Length only if the application wrote one.
	if length > 0 || r.Header.Get("Content-Length") != "" {
		headers = append(headers, wire.Header{Name: "Content-Length", Value: strconv.Itoa(length)})
	}
	return headers
}

// target is the request-target as the application wrote it.
func target(r *http.Request) string {
	if r.RequestURI != "" {
		return r.RequestURI
	}
	return r.URL.RequestURI()
}

// synth is a response this program makes up rather than relays: text/plain with the reason as the body,
// the same from whichever peer answers.
func synth(w http.ResponseWriter, status int, why string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(why)+1))
	w.WriteHeader(status)
	io.WriteString(w, why+"\n")
}

// synthMessages is the same answer built as wire messages, for the server, which has no
// http.ResponseWriter to write it on.
func synthMessages(job uint64, status uint16, why string) []wire.Message {
	body := why + "\n"
	return []wire.Message{
		&wire.Response{Job: job, Status: status, Headers: []wire.Header{
			{Name: "Content-Type", Value: "text/plain; charset=utf-8"},
			{Name: "Content-Length", Value: strconv.Itoa(len(body))},
		}},
		&wire.Body{Job: job, Bytes: []byte(body)},
		&wire.End{Job: job},
	}
}
