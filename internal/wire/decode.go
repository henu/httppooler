package wire

// Reading messages. Everything is read in the order PROTOCOL.md lists it, every byte of every item is
// consumed, and the first thing that is wrong refuses the connection: a decoder that has refused
// anything refuses everything after it.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"
)

// Decoder reads messages from one direction of a connection. It is not safe for concurrent use.
type Decoder struct {
	r        *bufio.Reader
	role     Role
	gotHello bool
	err      error
	scratch  [8]byte
}

// NewDecoder reads from r as the given side of the connection.
func NewDecoder(r io.Reader, role Role) *Decoder {
	return &Decoder{r: bufio.NewReader(r), role: role}
}

// Next reads the next message. io.EOF means the stream ended cleanly between messages; an end inside a
// message is io.ErrUnexpectedEOF, and everything else is malformed input.
func (d *Decoder) Next() (Message, error) {
	if d.err != nil {
		return nil, d.err
	}

	first, err := d.r.ReadByte()
	if err != nil {
		if err != io.EOF {
			d.err = err
		}
		return nil, err
	}

	m, err := d.message(Kind(first))
	if err != nil {
		d.err = err
		return nil, err
	}
	return m, nil
}

// message reads the fields of one kind, the kind byte already consumed. Every field is read in the
// order PROTOCOL.md lists it, one statement per field, because that order is the format.
func (d *Decoder) message(kind Kind) (Message, error) {
	// A client's first message is its HELLO, so the server takes nothing before it.
	if d.role == RoleServer && !d.gotHello && kind != KindHello {
		return nil, fmt.Errorf("wire: %s received before HELLO", kind)
	}

	var m Message
	switch kind {
	case KindHello:
		m = d.hello()
	case KindPing:
		m = &Ping{}
	case KindPong:
		m = &Pong{}
	case KindRequest:
		r := &Request{}
		r.Job = d.job()
		r.Service = d.name8("service")
		r.Method = d.str8()
		r.Target = d.str16()
		r.Headers = d.headers()
		m = r
	case KindResponse:
		r := &Response{}
		r.Job = d.job()
		r.Status = d.status()
		r.Headers = d.headers()
		m = r
	case KindBody:
		b := &Body{}
		b.Job = d.job()
		b.Bytes = d.chunk()
		m = b
	case KindEnd:
		e := &End{}
		e.Job = d.job()
		e.Trailers = d.headers()
		m = e
	case KindCancel:
		c := &Cancel{}
		c.Job = d.job()
		m = c
	case KindFail:
		f := &Fail{}
		f.Job = d.job()
		f.Reason = d.text16("reason")
		m = f
	default:
		return nil, fmt.Errorf("wire: unknown message kind 0x%02x", uint8(kind))
	}

	if d.err != nil {
		return nil, d.err
	}
	return m, nil
}

// hello reads a HELLO and keeps the rule that there is one of them and it comes first.
func (d *Decoder) hello() Message {
	// HELLO goes client to server only.
	if d.role == RoleClient {
		d.fail("the server sent HELLO")
		return nil
	}

	// A client's first message is its HELLO, and it has only one.
	if d.gotHello {
		d.fail("HELLO sent twice")
		return nil
	}

	m := &Hello{}
	m.Name = d.name8("peer name")
	count := int(d.u8())
	if d.err != nil {
		return nil
	}
	if count == 0 {
		d.gotHello = true
		return m
	}

	m.Providers = make([]ProviderInfo, 0, count)
	for i := 0; i < count; i++ {
		var p ProviderInfo
		p.Service = d.name8("service")
		p.MaxConcurrent = d.slots()
		p.Priority = d.u16()
		if d.err != nil {
			return nil
		}
		m.Providers = append(m.Providers, p)
	}

	d.gotHello = true
	return m
}

// fail keeps the first refusal; everything after it reads nothing.
func (d *Decoder) fail(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf("wire: "+format, args...)
	}
}

// read consumes exactly n bytes into buf. An end of stream here is inside a message, so it is
// unexpected: the count that announced these bytes has already been read.
func (d *Decoder) read(buf []byte) bool {
	if d.err != nil {
		return false
	}
	if _, err := io.ReadFull(d.r, buf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		d.err = err
		return false
	}
	return true
}

func (d *Decoder) u8() uint8 {
	if !d.read(d.scratch[:1]) {
		return 0
	}
	return d.scratch[0]
}

func (d *Decoder) u16() uint16 {
	if !d.read(d.scratch[:2]) {
		return 0
	}
	return binary.LittleEndian.Uint16(d.scratch[:2])
}

func (d *Decoder) u32() uint32 {
	if !d.read(d.scratch[:4]) {
		return 0
	}
	return binary.LittleEndian.Uint32(d.scratch[:4])
}

func (d *Decoder) u64() uint64 {
	if !d.read(d.scratch[:8]) {
		return 0
	}
	return binary.LittleEndian.Uint64(d.scratch[:8])
}

// bytes reads a byte count's worth of bytes.
func (d *Decoder) bytes(n int) []byte {
	if d.err != nil || n == 0 {
		return nil
	}
	buf := make([]byte, n)
	if !d.read(buf) {
		return nil
	}
	return buf
}

// str8 is a u8 byte count and those bytes.
func (d *Decoder) str8() string {
	return string(d.bytes(int(d.u8())))
}

// str16 is a u16 byte count and those bytes.
func (d *Decoder) str16() string {
	return string(d.bytes(int(d.u16())))
}

// name8 is a str8 that names something, and names are UTF-8.
func (d *Decoder) name8(what string) string {
	s := d.str8()
	if d.err == nil && !utf8.ValidString(s) {
		d.fail("%s is not valid UTF-8", what)
		return ""
	}
	return s
}

// text16 is a str16 that carries text meant to be read by a person.
func (d *Decoder) text16(what string) string {
	s := d.str16()
	if d.err == nil && !utf8.ValidString(s) {
		d.fail("%s is not valid UTF-8", what)
		return ""
	}
	return s
}

// job is the id: never 0, and its parity says which side started the job.
func (d *Decoder) job() uint64 {
	id := d.u64()
	if d.err == nil && id == 0 {
		d.fail("job id 0 is not a job")
		return 0
	}
	return id
}

// slots is max_concurrent: an upstream that runs nothing at a time is not an upstream.
func (d *Decoder) slots() uint16 {
	n := d.u16()
	if d.err == nil && n < 1 {
		d.fail("max_concurrent is 0, want at least 1")
		return 0
	}
	return n
}

// status is the HTTP status, which is three digits and starts at 100.
func (d *Decoder) status() uint16 {
	s := d.u16()
	if d.err == nil && (s < minStatus || s > maxStatus) {
		d.fail("status %d is outside %d..%d", s, minStatus, maxStatus)
		return 0
	}
	return s
}

// chunk is a piece of a body: a u32 byte count and those bytes, never empty.
func (d *Decoder) chunk() []byte {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if n < 1 {
		d.fail("body chunk is empty")
		return nil
	}
	return d.bytes(int(n))
}

// headers reads a u16 count and that many name and value pairs, in order.
func (d *Decoder) headers() []Header {
	count := int(d.u16())
	if d.err != nil || count == 0 {
		return nil
	}
	hs := make([]Header, 0, count)
	for i := 0; i < count; i++ {
		var h Header
		h.Name = d.str8()
		h.Value = d.str16()
		if d.err != nil {
			return nil
		}
		hs = append(hs, h)
	}
	return hs
}
