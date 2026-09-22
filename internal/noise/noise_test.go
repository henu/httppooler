package noise

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// TestWrongPSK and TestWrongPrologue are the two ways peers can disagree about what they share. Both
// fail on the responder's first read, before it has written anything back.
func TestWrongPSK(t *testing.T) {
	initiator := handshake(t, Config{Initiator: true, PSK: secret(t)})
	responder := handshake(t, Config{PSK: secret(t)})

	message, err := initiator.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := responder.ReadMessage(message); err == nil {
		t.Fatal("responder accepted a message encrypted under another secret")
	}
}

func TestWrongPrologue(t *testing.T) {
	psk := secret(t)
	initiator := handshake(t, Config{Initiator: true, PSK: psk, Prologue: []byte("HPOL\x00")})
	responder := handshake(t, Config{PSK: psk, Prologue: []byte("HPOL\x01")})

	message, err := initiator.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := responder.ReadMessage(message); err == nil {
		t.Fatal("responder accepted a message with another prologue")
	}
}

// TestTamperedHandshakeMessage changes one byte at a time, in the ephemeral key and in the ciphertext,
// and expects the message to be refused whole either way.
func TestTamperedHandshakeMessage(t *testing.T) {
	psk := secret(t)

	for _, offset := range []int{0, PublicKeySize - 1, PublicKeySize, HandshakeMessageSize - 1} {
		initiator := handshake(t, Config{Initiator: true, PSK: psk})
		responder := handshake(t, Config{PSK: psk})

		message, err := initiator.WriteMessage(nil)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		message[offset] ^= 0x01

		if _, err := responder.ReadMessage(message); err == nil {
			t.Fatalf("responder accepted a message with byte %d flipped", offset)
		}
	}
}

// TestShortHandshakeMessage: anything shorter than an ephemeral key and a tag cannot be a message.
func TestShortHandshakeMessage(t *testing.T) {
	responder := handshake(t, Config{PSK: secret(t)})

	if _, err := responder.ReadMessage(make([]byte, HandshakeMessageSize-1)); err == nil {
		t.Fatal("responder accepted a message one byte short")
	}
}

// TestBadPSKSize: the secret is the whole security setup, so a wrong-sized one is refused at once.
func TestBadPSKSize(t *testing.T) {
	for _, size := range []int{0, KeySize - 1, KeySize + 1} {
		if _, err := NewHandshake(Config{PSK: make([]byte, size)}); err == nil {
			t.Fatalf("accepted a %d-byte pre-shared key", size)
		}
	}
}

// TestOutOfTurn: each side writes its own message and reads the other's, and neither can have the keys
// before both messages have gone by.
func TestOutOfTurn(t *testing.T) {
	psk := secret(t)
	initiator := handshake(t, Config{Initiator: true, PSK: psk})
	responder := handshake(t, Config{PSK: psk})

	if _, err := initiator.ReadMessage(make([]byte, HandshakeMessageSize)); err == nil {
		t.Fatal("initiator read the message it is supposed to write")
	}
	if _, err := responder.WriteMessage(nil); err == nil {
		t.Fatal("responder wrote the message it is supposed to read")
	}
	if _, _, err := initiator.Split(); err == nil {
		t.Fatal("initiator split before the handshake was complete")
	}
}

// TestFailedHandshakeStaysFailed: once a handshake has refused something it is over, and the caller
// cannot retry its way past it.
func TestFailedHandshakeStaysFailed(t *testing.T) {
	psk := secret(t)
	initiator := handshake(t, Config{Initiator: true, PSK: psk})
	responder := handshake(t, Config{PSK: secret(t)})

	message, err := initiator.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := responder.ReadMessage(message); err == nil {
		t.Fatal("responder accepted a message encrypted under another secret")
	}

	if _, err := responder.ReadMessage(message); err == nil {
		t.Fatal("a failed handshake read a second message")
	}
	if _, err := responder.WriteMessage(nil); err == nil {
		t.Fatal("a failed handshake wrote a message")
	}
	if _, _, err := responder.Split(); err == nil {
		t.Fatal("a failed handshake handed out keys")
	}
}

// TestHandshakeIsDoneAfterSplit: there is no third message to write or read.
func TestHandshakeIsDoneAfterSplit(t *testing.T) {
	initiator, responder := complete(t)

	if _, err := initiator.WriteMessage(nil); err == nil {
		t.Fatal("initiator wrote a third handshake message")
	}
	if _, err := responder.ReadMessage(make([]byte, HandshakeMessageSize)); err == nil {
		t.Fatal("responder read a third handshake message")
	}
}

// TestTamperedRecord: a record that does not authenticate is refused and leaves the counter alone, so
// the record that was actually sent still reads.
func TestTamperedRecord(t *testing.T) {
	initiator, responder := complete(t)
	send, _, err := initiator.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	_, receive, err := responder.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	plaintext := []byte("a request body chunk")
	record, err := send.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	tampered := bytes.Clone(record)
	tampered[0] ^= 0x01
	if _, err := receive.Decrypt(nil, tampered); err == nil {
		t.Fatal("a tampered record decrypted")
	}
	if receive.Nonce() != 0 {
		t.Fatalf("counter moved to %d on a refused record", receive.Nonce())
	}

	got, err := receive.Decrypt(nil, record)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("read %q, want %q", got, plaintext)
	}
}

// TestRecordsAreOrdered: the counter is the replay and reordering defence, so records that arrive out
// of order do not decrypt.
func TestRecordsAreOrdered(t *testing.T) {
	initiator, responder := complete(t)
	send, _, err := initiator.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	_, receive, err := responder.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	first, err := send.Encrypt(nil, []byte("first"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	second, err := send.Encrypt(nil, []byte("second"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	if _, err := receive.Decrypt(nil, second); err == nil {
		t.Fatal("the second record decrypted before the first")
	}
	if _, err := receive.Decrypt(nil, first); err != nil {
		t.Fatalf("first record: %v", err)
	}
	if _, err := receive.Decrypt(nil, first); err == nil {
		t.Fatal("the first record decrypted twice")
	}
}

// TestOversizedPlaintext: a Noise message with its tag fits in 65535 bytes, and that is the only size
// this package has an opinion about.
func TestOversizedPlaintext(t *testing.T) {
	initiator, _ := complete(t)
	send, _, err := initiator.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	if _, err := send.Encrypt(nil, make([]byte, MaxPlaintextSize+1)); err == nil {
		t.Fatal("encrypted a plaintext that does not fit in a message")
	}
	if _, err := send.Encrypt(nil, make([]byte, MaxPlaintextSize)); err != nil {
		t.Fatalf("the largest plaintext that fits was refused: %v", err)
	}
}

// handshake starts one side, failing the test if the config is refused.
func handshake(t *testing.T, cfg Config) *Handshake {
	t.Helper()

	hs, err := NewHandshake(cfg)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	return hs
}

// complete runs a whole handshake and returns both sides, ready to split.
func complete(t *testing.T) (initiator, responder *Handshake) {
	t.Helper()

	psk := secret(t)
	initiator = handshake(t, Config{Initiator: true, PSK: psk, Prologue: []byte("HPOL\x00")})
	responder = handshake(t, Config{PSK: psk, Prologue: []byte("HPOL\x00")})

	message, err := initiator.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write message 0: %v", err)
	}
	if _, err := responder.ReadMessage(message); err != nil {
		t.Fatalf("read message 0: %v", err)
	}

	message, err = responder.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write message 1: %v", err)
	}
	if _, err := initiator.ReadMessage(message); err != nil {
		t.Fatalf("read message 1: %v", err)
	}

	return initiator, responder
}

// secret is a fresh pre-shared key; two of them are never the same.
func secret(t *testing.T) []byte {
	t.Helper()

	psk := make([]byte, KeySize)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("random: %v", err)
	}
	return psk
}
