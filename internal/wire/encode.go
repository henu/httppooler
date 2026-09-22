package wire

// Writing messages. The encoder refuses to put anything on the wire that a reader would refuse to take
// off it, so every range PROTOCOL.md states is checked on both sides.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

// Encoder writes messages to one direction of a connection. It is not safe for concurrent use: the
// bytes of one message go out as a unit, so callers hold one encoder's lock.
type Encoder struct {
	w         io.Writer
	role      Role
	sentHello bool
	buf       []byte
}

// NewEncoder writes to w as the given side of the connection.
func NewEncoder(w io.Writer, role Role) *Encoder {
	return &Encoder{w: w, role: role}
}

// Write encodes m and writes it in one call, so a reader sees whole messages whenever the writer under
// it preserves boundaries.
func (e *Encoder) Write(m Message) error {
	// The client says HELLO exactly once and says it first; the server never says it at all.
	if err := e.helloOrder(m); err != nil {
		return err
	}

	e.buf = e.buf[:0]
	buf, err := Encode(e.buf, m)
	if err != nil {
		return err
	}
	e.buf = buf

	if _, err := e.w.Write(buf); err != nil {
		return err
	}
	if m.Kind() == KindHello {
		e.sentHello = true
	}
	return nil
}

// helloOrder is the one direction rule the wire itself keeps.
func (e *Encoder) helloOrder(m Message) error {
	hello := m.Kind() == KindHello

	// HELLO goes client to server only.
	if hello && e.role == RoleServer {
		return errors.New("wire: the server does not send HELLO")
	}

	// A client's first message is its HELLO, and it has only one.
	if e.role == RoleClient && hello == e.sentHello {
		if hello {
			return errors.New("wire: HELLO sent twice")
		}
		return fmt.Errorf("wire: %s sent before HELLO", m.Kind())
	}

	return nil
}

// Encode appends the encoding of m to dst and returns the grown slice.
func Encode(dst []byte, m Message) ([]byte, error) {
	b := builder{buf: append(dst, byte(m.Kind()))}

	switch m := m.(type) {
	case *Hello:
		b.name8("peer name", m.Name)
		b.count8("providers", len(m.Providers))
		for _, p := range m.Providers {
			b.name8("service", p.Service)
			b.slots(p.MaxConcurrent)
			b.u16(p.Priority)
		}
	case *Ping, *Pong:
	case *Request:
		b.job(m.Job)
		b.name8("service", m.Service)
		b.str8("method", m.Method)
		b.str16("target", m.Target)
		b.headers(m.Headers)
	case *Response:
		b.job(m.Job)
		b.status(m.Status)
		b.headers(m.Headers)
	case *Body:
		b.job(m.Job)
		b.chunk(m.Bytes)
	case *End:
		b.job(m.Job)
		b.headers(m.Trailers)
	case *Cancel:
		b.job(m.Job)
	case *Fail:
		b.job(m.Job)
		b.text16("reason", m.Reason)
	default:
		return dst, fmt.Errorf("wire: cannot encode %T", m)
	}

	if b.err != nil {
		return dst, b.err
	}
	return b.buf, nil
}

// builder appends fields and remembers the first thing that was wrong.
type builder struct {
	buf []byte
	err error
}

func (b *builder) u8(v uint8)   { b.buf = append(b.buf, v) }
func (b *builder) u16(v uint16) { b.buf = binary.LittleEndian.AppendUint16(b.buf, v) }
func (b *builder) u32(v uint32) { b.buf = binary.LittleEndian.AppendUint32(b.buf, v) }
func (b *builder) u64(v uint64) { b.buf = binary.LittleEndian.AppendUint64(b.buf, v) }

// fail keeps the first refusal; the rest of the message is written and thrown away.
func (b *builder) fail(format string, args ...any) {
	if b.err == nil {
		b.err = fmt.Errorf("wire: "+format, args...)
	}
}

// str8 is a u8 byte count and those bytes.
func (b *builder) str8(what, s string) {
	if len(s) > 255 {
		b.fail("%s is %d bytes, at most 255 fit in a str8", what, len(s))
		return
	}
	b.u8(uint8(len(s)))
	b.buf = append(b.buf, s...)
}

// str16 is a u16 byte count and those bytes.
func (b *builder) str16(what, s string) {
	if len(s) > 65535 {
		b.fail("%s is %d bytes, at most 65535 fit in a str16", what, len(s))
		return
	}
	b.u16(uint16(len(s)))
	b.buf = append(b.buf, s...)
}

// name8 is a str8 that names something: peer, service. Those are text and must be valid UTF-8.
func (b *builder) name8(what, s string) {
	if !utf8.ValidString(s) {
		b.fail("%s is not valid UTF-8", what)
		return
	}
	b.str8(what, s)
}

// text16 is a str16 that carries text meant to be read by a person.
func (b *builder) text16(what, s string) {
	if !utf8.ValidString(s) {
		b.fail("%s is not valid UTF-8", what)
		return
	}
	b.str16(what, s)
}

// job is the id: never 0, and its parity says which side started the job.
func (b *builder) job(id uint64) {
	if id == 0 {
		b.fail("job id 0 is not a job")
		return
	}
	b.u64(id)
}

// slots is max_concurrent: an upstream that runs nothing at a time is not an upstream.
func (b *builder) slots(n uint16) {
	if n < 1 {
		b.fail("max_concurrent is 0, want at least 1")
		return
	}
	b.u16(n)
}

// status is the HTTP status, which is three digits and starts at 100.
func (b *builder) status(s uint16) {
	if s < minStatus || s > maxStatus {
		b.fail("status %d is outside %d..%d", s, minStatus, maxStatus)
		return
	}
	b.u16(s)
}

// count8 announces items counted by a u8.
func (b *builder) count8(what string, n int) {
	if n > 255 {
		b.fail("%d %s, at most 255 are counted by a u8", n, what)
		return
	}
	b.u8(uint8(n))
}

// chunk is a piece of a body: a u32 byte count and those bytes, never empty.
func (b *builder) chunk(p []byte) {
	if len(p) < 1 {
		b.fail("body chunk is empty")
		return
	}
	if uint64(len(p)) > math.MaxUint32 {
		b.fail("body chunk is %d bytes, at most %d are counted by a u32", len(p), uint64(math.MaxUint32))
		return
	}
	b.u32(uint32(len(p)))
	b.buf = append(b.buf, p...)
}

// headers writes a u16 count and that many name and value pairs, in order.
func (b *builder) headers(hs []Header) {
	if len(hs) > 65535 {
		b.fail("%d headers, at most 65535 are counted by a u16", len(hs))
		return
	}
	b.u16(uint16(len(hs)))
	for _, h := range hs {
		b.str8("header name", h.Name)
		b.str16("header value", h.Value)
	}
}
