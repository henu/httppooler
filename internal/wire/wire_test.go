package wire

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"

	"github.com/henu/httppooler/internal/noise"
	"reflect"
	"strings"
	"testing"
)

// messages is one of every kind, with the fields filled so that nothing is left at its zero value by
// accident.
func messages() []Message {
	return []Message{
		&Hello{Name: "kotikone", Providers: []ProviderInfo{
			{Service: "ollama", MaxConcurrent: 1, Priority: 100},
			{Service: "whisper", MaxConcurrent: 4, Priority: 200},
		}},
		&Ping{},
		&Pong{},
		&Request{Job: 1, Service: "ollama", Method: "POST", Target: "/api/generate?stream=true", Headers: []Header{
			{Name: "Content-Type", Value: "application/json"},
			{Name: "Accept", Value: "*/*"},
		}},
		&Response{Job: 1, Status: 200, Headers: []Header{{Name: "Content-Type", Value: "application/json"}}},
		&Body{Job: 1, Bytes: []byte("one piece of a body")},
		&End{Job: 1, Trailers: []Header{{Name: "X-Elapsed", Value: "1.5s"}}},
		&Cancel{Job: 3},
		&Fail{Job: 3, Reason: "upstream refused the connection"},
	}
}

// TestRoundTrip writes every kind and reads it back. The HELLO goes first because a client's first
// message is its HELLO and the server takes nothing before it.
func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, RoleClient)
	for _, m := range messages() {
		if err := enc.Write(m); err != nil {
			t.Fatalf("write %s: %v", m.Kind(), err)
		}
	}

	dec := NewDecoder(&buf, RoleServer)
	for _, want := range messages() {
		got, err := dec.Next()
		if err != nil {
			t.Fatalf("read %s: %v", want.Kind(), err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s came back as %#v, want %#v", want.Kind(), got, want)
		}
	}

	// The stream ends exactly on the last byte of the last message.
	if _, err := dec.Next(); err != io.EOF {
		t.Fatalf("after the last message: %v, want EOF", err)
	}
}

// TestEmptyRepeatedFields keeps the counted fields honest when there is nothing to count.
func TestEmptyRepeatedFields(t *testing.T) {
	empty := []Message{
		&Hello{Name: "solo"},
		&Request{Job: 1, Service: "s", Method: "GET", Target: "/"},
		&Response{Job: 1, Status: 204},
		&End{Job: 1},
	}

	var buf bytes.Buffer
	enc := NewEncoder(&buf, RoleClient)
	for _, m := range empty {
		if err := enc.Write(m); err != nil {
			t.Fatalf("write %s: %v", m.Kind(), err)
		}
	}

	dec := NewDecoder(&buf, RoleServer)
	for _, want := range empty {
		got, err := dec.Next()
		if err != nil {
			t.Fatalf("read %s: %v", want.Kind(), err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s came back as %#v, want %#v", want.Kind(), got, want)
		}
	}
}

// TestTruncated cuts every valid message at every length short of whole. A reader that stops early must
// say the stream ended inside a message, and must never hand out a message it did not fully read.
func TestTruncated(t *testing.T) {
	for _, m := range messages() {
		encoded, err := Encode(nil, m)
		if err != nil {
			t.Fatalf("encode %s: %v", m.Kind(), err)
		}
		for cut := 0; cut < len(encoded); cut++ {
			dec := NewDecoder(bytes.NewReader(encoded[:cut]), RoleServer)
			if m.Kind() != KindHello {
				dec.gotHello = true
			}
			got, err := dec.Next()
			if err == nil {
				t.Fatalf("%s cut to %d of %d bytes decoded as %#v", m.Kind(), cut, len(encoded), got)
			}
			if cut == 0 && err != io.EOF {
				t.Fatalf("%s cut to nothing: %v, want EOF", m.Kind(), err)
			}
			if cut > 0 && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("%s cut to %d bytes: %v, want unexpected EOF", m.Kind(), cut, err)
			}
		}
	}
}

// TestMalformed is every range PROTOCOL.md states, one byte string per refusal.
func TestMalformed(t *testing.T) {
	cases := []struct {
		name  string
		role  Role
		hello bool
		bytes []byte
		want  string
	}{
		{name: "unknown kind", hello: true, bytes: []byte{0x7f}, want: "unknown message kind"},
		{name: "kind zero", hello: true, bytes: []byte{0x00}, want: "unknown message kind"},
		{name: "job zero", hello: true, bytes: append([]byte{byte(KindCancel)}, make([]byte, 8)...), want: "job id 0"},
		{name: "empty body chunk", hello: true, bytes: concat(
			[]byte{byte(KindBody)}, u64(1), u32(0)), want: "body chunk is empty"},
		{name: "status below range", hello: true, bytes: concat(
			[]byte{byte(KindResponse)}, u64(1), u16(99), u16(0)), want: "outside 100..599"},
		{name: "status above range", hello: true, bytes: concat(
			[]byte{byte(KindResponse)}, u64(1), u16(600), u16(0)), want: "outside 100..599"},
		{name: "no slots", bytes: concat(
			[]byte{byte(KindHello)}, str8("peer"), []byte{1}, str8("ollama"), u16(0), u16(100)),
			want: "max_concurrent is 0"},
		{name: "name not utf-8", bytes: concat(
			[]byte{byte(KindHello)}, []byte{2, 0xff, 0xfe}), want: "not valid UTF-8"},
		{name: "before hello", bytes: concat(
			[]byte{byte(KindCancel)}, u64(1)), want: "received before HELLO"},
		{name: "hello to a client", role: RoleClient, bytes: concat(
			[]byte{byte(KindHello)}, str8("peer"), []byte{0}), want: "server sent HELLO"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec := NewDecoder(bytes.NewReader(c.bytes), c.role)
			dec.gotHello = c.hello
			if _, err := dec.Next(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("decoded to %v, want a refusal mentioning %q", err, c.want)
			}
			// A decoder that has refused anything refuses everything after it.
			if _, err := dec.Next(); err == nil {
				t.Fatal("the decoder spoke again after refusing")
			}
		})
	}
}

// TestHelloOnce is the rule that a client says HELLO first and says it once, kept at both ends.
func TestHelloOnce(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, RoleClient)

	if err := enc.Write(&Ping{}); err == nil {
		t.Fatal("wrote PING before HELLO")
	}
	if err := enc.Write(&Hello{Name: "peer"}); err != nil {
		t.Fatalf("write HELLO: %v", err)
	}
	if err := enc.Write(&Hello{Name: "peer"}); err == nil {
		t.Fatal("wrote HELLO twice")
	}

	server := NewEncoder(&buf, RoleServer)
	if err := server.Write(&Hello{Name: "server"}); err == nil {
		t.Fatal("the server wrote HELLO")
	}

	// A second HELLO is refused on the reading side as well.
	second, err := Encode(nil, &Hello{Name: "peer"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec := NewDecoder(bytes.NewReader(append(buf.Bytes(), second...)), RoleServer)
	if _, err := dec.Next(); err != nil {
		t.Fatalf("read HELLO: %v", err)
	}
	if _, err := dec.Next(); err == nil {
		t.Fatal("read a second HELLO")
	}
}

// TestEncodeRefusesWhatDecodeWould keeps the two ends agreeing: nothing the encoder emits is something
// the decoder would refuse.
func TestEncodeRefusesWhatDecodeWould(t *testing.T) {
	cases := []struct {
		name string
		m    Message
	}{
		{"job zero", &Cancel{Job: 0}},
		{"empty body chunk", &Body{Job: 1}},
		{"status below range", &Response{Job: 1, Status: 99}},
		{"status above range", &Response{Job: 1, Status: 600}},
		{"no slots", &Hello{Name: "p", Providers: []ProviderInfo{{Service: "s"}}}},
		{"name not utf-8", &Hello{Name: "\xff\xfe"}},
		{"name too long for a str8", &Hello{Name: strings.Repeat("x", 256)}},
		{"target too long for a str16", &Request{Job: 1, Service: "s", Method: "GET", Target: strings.Repeat("x", 65536)}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Encode(nil, c.m); err == nil {
				t.Fatalf("encoded %#v", c.m)
			}
		})
	}
}

// TestPreamble is layer 0: the bytes, a stranger, and a peer from another version.
func TestPreamble(t *testing.T) {
	version, err := ReadPreamble(bytes.NewReader(Preamble()))
	if err != nil {
		t.Fatalf("own preamble: %v", err)
	}
	if version != VersionNewest {
		t.Fatalf("own preamble says version %d, want %d", version, VersionNewest)
	}

	if _, err := ReadPreamble(strings.NewReader("GET / HTTP/1.1")); !errors.Is(err, ErrNotPeer) {
		t.Fatalf("an HTTP request gave %v, want ErrNotPeer", err)
	}

	future := append(Preamble()[:4:4], VersionNewest+1)
	err = readVersion(bytes.NewReader(future))
	var mismatch *VersionError
	if !errors.As(err, &mismatch) {
		t.Fatalf("a newer peer gave %v, want a VersionError", err)
	}
	if mismatch.Theirs != VersionNewest+1 || mismatch.Ours != VersionNewest {
		t.Fatalf("version error says theirs %d ours %d", mismatch.Theirs, mismatch.Ours)
	}
}

// TestHandshakeAndRecords runs layers 0 to 3 over a pipe: both sides set up the encrypted stream, and
// every message goes through it unchanged.
func TestHandshakeAndRecords(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("secret: %v", err)
	}

	type result struct {
		session *Session
		err     error
	}
	done := make(chan result, 1)
	go func() {
		session, err := Handshake(server, psk, false)
		done <- result{session, err}
	}()

	clientSession, err := Handshake(client, psk, true)
	if err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	got := <-done
	if got.err != nil {
		t.Fatalf("server handshake: %v", got.err)
	}

	sent := messages()
	go func() {
		enc := NewEncoder(clientSession.Writer, RoleClient)
		for _, m := range sent {
			if err := enc.Write(m); err != nil {
				t.Errorf("write %s: %v", m.Kind(), err)
				return
			}
		}
	}()

	dec := NewDecoder(got.session.Reader, RoleServer)
	for _, want := range sent {
		m, err := dec.Next()
		if err != nil {
			t.Fatalf("read %s: %v", want.Kind(), err)
		}
		if !reflect.DeepEqual(m, want) {
			t.Fatalf("%s came back as %#v, want %#v", want.Kind(), m, want)
		}
	}
}

// TestWrongSecret is the whole security setup in one test: a client with another secret gets no stream.
func TestWrongSecret(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	theirs := make([]byte, 32)
	ours := make([]byte, 32)
	if _, err := rand.Read(theirs); err != nil {
		t.Fatalf("secret: %v", err)
	}
	if _, err := rand.Read(ours); err != nil {
		t.Fatalf("secret: %v", err)
	}

	// The responder refuses on its first read and writes nothing back, so the initiator learns of it
	// the way it does in the daemon: the connection closes under it.
	done := make(chan error, 1)
	go func() {
		_, err := Handshake(server, ours, false)
		if err != nil {
			server.Close()
		}
		done <- err
	}()

	if _, err := Handshake(client, theirs, true); err == nil {
		t.Fatal("the client finished a handshake under the wrong secret")
	}
	if err := <-done; err == nil {
		t.Fatal("the server accepted the wrong secret")
	}
}

// TestRecordBoundariesCarryNoMeaning sends several messages in one record and one message split across
// records, and expects the same messages out of both.
func TestRecordBoundariesCarryNoMeaning(t *testing.T) {
	sent := messages()
	var stream []byte
	for _, m := range sent {
		encoded, err := Encode(nil, m)
		if err != nil {
			t.Fatalf("encode %s: %v", m.Kind(), err)
		}
		stream = append(stream, encoded...)
	}

	for _, piece := range []int{1, 3, 7, len(stream)} {
		var buf bytes.Buffer
		send, receive := transportPair(t)
		w := NewRecordWriter(&buf, send)
		for start := 0; start < len(stream); start += piece {
			end := min(start+piece, len(stream))
			if _, err := w.Write(stream[start:end]); err != nil {
				t.Fatalf("write: %v", err)
			}
		}

		dec := NewDecoder(NewRecordReader(&buf, receive), RoleServer)
		for _, want := range sent {
			m, err := dec.Next()
			if err != nil {
				t.Fatalf("pieces of %d: read %s: %v", piece, want.Kind(), err)
			}
			if !reflect.DeepEqual(m, want) {
				t.Fatalf("pieces of %d: %s came back as %#v", piece, want.Kind(), m)
			}
		}
	}
}

// TestShortRecord is the one structural bound of layer 2: a record carries a tag and at least one byte
// under it.
func TestShortRecord(t *testing.T) {
	_, receive := transportPair(t)
	short := concat(u16(uint16(minRecordSize-1)), make([]byte, minRecordSize-1))
	if _, err := io.ReadAll(NewRecordReader(bytes.NewReader(short), receive)); err == nil {
		t.Fatal("a record too short to hold anything was accepted")
	}
}

// TestTamperedRecord changes one byte of the ciphertext and expects the stream to end there.
func TestTamperedRecord(t *testing.T) {
	var buf bytes.Buffer
	send, receive := transportPair(t)
	if _, err := NewRecordWriter(&buf, send).Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	record := buf.Bytes()
	record[len(record)-1] ^= 0x01
	if _, err := io.ReadAll(NewRecordReader(bytes.NewReader(record), receive)); err == nil {
		t.Fatal("a tampered record was accepted")
	}
}

// transportPair is one direction of a finished handshake: the sender's state and the receiver's.
func transportPair(t *testing.T) (send, receive *noise.CipherState) {
	t.Helper()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("secret: %v", err)
	}

	done := make(chan *Session, 1)
	go func() {
		session, err := Handshake(server, psk, false)
		if err != nil {
			t.Errorf("server handshake: %v", err)
		}
		done <- session
	}()

	clientSession, err := Handshake(client, psk, true)
	if err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	serverSession := <-done
	if serverSession == nil {
		t.Fatal("no server session")
	}
	return clientSession.Writer.cipher, serverSession.Reader.cipher
}

func u16(v uint16) []byte { return []byte{byte(v), byte(v >> 8)} }
func u32(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }
func u64(v uint64) []byte {
	out := make([]byte, 8)
	for i := range out {
		out[i] = byte(v >> (8 * i))
	}
	return out
}
func str8(s string) []byte { return append([]byte{byte(len(s))}, s...) }

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
