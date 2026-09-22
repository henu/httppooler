package wire

// Layer 3: the messages parsed out of the plaintext stream. Every message begins with a kind byte and
// continues with the fields of that kind only, so a reader that meets an unknown kind cannot skip it and
// does not try: it refuses the connection.

import "fmt"

// Kind is the first byte of every message.
type Kind uint8

const (
	KindHello    Kind = 0x01
	KindPing     Kind = 0x02
	KindPong     Kind = 0x03
	KindRequest  Kind = 0x10
	KindResponse Kind = 0x11
	KindBody     Kind = 0x12
	KindEnd      Kind = 0x13
	KindCancel   Kind = 0x14
	KindFail     Kind = 0x15
)

// String names a kind for logs.
func (k Kind) String() string {
	switch k {
	case KindHello:
		return "HELLO"
	case KindPing:
		return "PING"
	case KindPong:
		return "PONG"
	case KindRequest:
		return "REQUEST"
	case KindResponse:
		return "RESPONSE"
	case KindBody:
		return "BODY"
	case KindEnd:
		return "END"
	case KindCancel:
		return "CANCEL"
	case KindFail:
		return "FAIL"
	}
	return fmt.Sprintf("kind 0x%02x", uint8(k))
}

// Role is which end of the connection a codec sits on. It decides who may say HELLO.
type Role int

const (
	// RoleServer is the peer that listens: it reads HELLO and never writes one.
	RoleServer Role = iota
	// RoleClient is the peer that dials: it writes HELLO and never reads one.
	RoleClient
)

// Header is one HTTP header, name and value as they were written.
type Header struct {
	Name  string
	Value string
}

// Message is one of the nine kinds below.
type Message interface {
	Kind() Kind
}

// Jobbed is every message that belongs to a job, which is all of them but HELLO, PING and PONG. The job
// id's parity tells each side whether it started the job, so no message carries a direction flag.
type Jobbed interface {
	Message
	JobID() uint64
}

// ProviderInfo is one upstream a client announces in its HELLO.
type ProviderInfo struct {
	Service       string
	MaxConcurrent uint16
	Priority      uint16
}

// Hello is a client's first message and its only one of this kind.
type Hello struct {
	Name      string
	Providers []ProviderInfo
}

// Ping and Pong keep a connection honest; any received message counts as life.
type Ping struct{}
type Pong struct{}

// Request opens a job: the pool to queue in on the way to the server, the provide section to use on the
// way from it.
type Request struct {
	Job     uint64
	Service string
	Method  string
	Target  string
	Headers []Header
}

// Response is the upstream's status and headers, sent as soon as they arrive.
type Response struct {
	Job     uint64
	Status  uint16
	Headers []Header
}

// Body is a piece of a body, in order. The request body travels consumer to provider and the response
// body the other way.
type Body struct {
	Job   uint64
	Bytes []byte
}

// End closes a body in one direction, with trailers if the upstream sent any.
type End struct {
	Job      uint64
	Trailers []Header
}

// Cancel says the application went away.
type Cancel struct {
	Job uint64
}

// Fail ends a job abnormally, with a reason for the log and for the 502 body.
type Fail struct {
	Job    uint64
	Reason string
}

func (*Hello) Kind() Kind    { return KindHello }
func (*Ping) Kind() Kind     { return KindPing }
func (*Pong) Kind() Kind     { return KindPong }
func (*Request) Kind() Kind  { return KindRequest }
func (*Response) Kind() Kind { return KindResponse }
func (*Body) Kind() Kind     { return KindBody }
func (*End) Kind() Kind      { return KindEnd }
func (*Cancel) Kind() Kind   { return KindCancel }
func (*Fail) Kind() Kind     { return KindFail }

func (m *Request) JobID() uint64  { return m.Job }
func (m *Response) JobID() uint64 { return m.Job }
func (m *Body) JobID() uint64     { return m.Job }
func (m *End) JobID() uint64      { return m.Job }
func (m *Cancel) JobID() uint64   { return m.Job }
func (m *Fail) JobID() uint64     { return m.Job }

// Status is the range a status line can hold.
const (
	minStatus = 100
	maxStatus = 599
)
