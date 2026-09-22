package peer

// Headers on their way between HTTP and the wire. Hop-by-hop headers belong to one connection and are
// dropped; everything else travels unchanged.

import (
	"net/http"
	"net/textproto"
	"sort"
	"strings"

	"github.com/henu/httppooler/internal/wire"
)

// hopByHop is the list PROTOCOL.md names. Host is dropped as well: the provider sets it to its own
// upstream, as any reverse proxy does.
var hopByHop = map[string]bool{
	"Connection":         true,
	"Keep-Alive":         true,
	"Transfer-Encoding":  true,
	"Te":                 true,
	"Trailer":            true,
	"Upgrade":            true,
	"Proxy-Connection":   true,
	"Host":               true,
	"Proxy-Authenticate": true,
}

// fromHTTP turns Go's header map into wire headers. Values of one name keep their order; the names
// themselves are sorted, because a Go server has already lost the order they arrived in.
func fromHTTP(h http.Header, drop ...string) []wire.Header {
	skip := connectionHeaders(h)
	for _, name := range drop {
		skip[textproto.CanonicalMIMEHeaderKey(name)] = true
	}

	names := make([]string, 0, len(h))
	for name := range h {
		if hopByHop[name] || skip[name] {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]wire.Header, 0, len(names))
	for _, name := range names {
		for _, value := range h[name] {
			out = append(out, wire.Header{Name: name, Value: value})
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// toHTTP turns wire headers back into Go's header map, in the order they came.
func toHTTP(hs []wire.Header, drop ...string) http.Header {
	skip := map[string]bool{}
	for _, name := range drop {
		skip[textproto.CanonicalMIMEHeaderKey(name)] = true
	}

	h := make(http.Header, len(hs))
	for _, header := range hs {
		name := textproto.CanonicalMIMEHeaderKey(header.Name)
		if hopByHop[name] || skip[name] {
			continue
		}
		h[name] = append(h[name], header.Value)
	}
	return h
}

// connectionHeaders are the headers a Connection header lists, which are hop-by-hop by having been
// listed there.
func connectionHeaders(h http.Header) map[string]bool {
	named := map[string]bool{}
	for _, value := range h["Connection"] {
		for _, token := range strings.Split(value, ",") {
			token = strings.TrimSpace(token)
			if token != "" {
				named[textproto.CanonicalMIMEHeaderKey(token)] = true
			}
		}
	}
	return named
}

// isUpgrade says whether a request asks to turn the connection into something else, WebSocket above
// all. The pool carries requests and responses, so those are answered 501 where they arrive.
func isUpgrade(r *http.Request) bool {
	if r.Header.Get("Upgrade") != "" {
		return true
	}
	for _, value := range r.Header["Connection"] {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}
