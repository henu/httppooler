package noise

import (
	"bytes"
	"crypto/rand"
	"testing"

	flynn "github.com/flynn/noise"
)

// TestInteropInitiator and TestInteropResponder run this package against another implementation of the
// same suite, once in each role: if both agree on every byte, the composition here is the specification
// and not just self-consistent.
func TestInteropInitiator(t *testing.T) {
	psk, prologue := interopInputs(t)

	ours, err := NewHandshake(Config{Initiator: true, PSK: psk, Prologue: prologue})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	theirs, err := flynn.NewHandshakeState(flynnConfig(false, psk, prologue))
	if err != nil {
		t.Fatalf("flynn handshake: %v", err)
	}

	// Message 0 is ours, message 1 is theirs; the payloads are what httppooler sends, nothing.
	message, err := ours.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write message 0: %v", err)
	}
	if _, _, _, err := theirs.ReadMessage(nil, message); err != nil {
		t.Fatalf("flynn read message 0: %v", err)
	}

	message, theirSend, theirReceive, err := theirs.WriteMessage(nil, nil)
	if err != nil {
		t.Fatalf("flynn write message 1: %v", err)
	}
	if _, err := ours.ReadMessage(message); err != nil {
		t.Fatalf("read message 1: %v", err)
	}

	ourSend, ourReceive, err := ours.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	// theirSend is the responder's first cipher state, which the specification gives to the initiator's
	// direction; as the responder they send with the second.
	interopTransport(t, ourSend, ourReceive, theirReceive, theirSend)
}

func TestInteropResponder(t *testing.T) {
	psk, prologue := interopInputs(t)

	ours, err := NewHandshake(Config{PSK: psk, Prologue: prologue})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	theirs, err := flynn.NewHandshakeState(flynnConfig(true, psk, prologue))
	if err != nil {
		t.Fatalf("flynn handshake: %v", err)
	}

	message, _, _, err := theirs.WriteMessage(nil, nil)
	if err != nil {
		t.Fatalf("flynn write message 0: %v", err)
	}
	if _, err := ours.ReadMessage(message); err != nil {
		t.Fatalf("read message 0: %v", err)
	}

	message, err = ours.WriteMessage(nil)
	if err != nil {
		t.Fatalf("write message 1: %v", err)
	}
	_, theirSend, theirReceive, err := theirs.ReadMessage(nil, message)
	if err != nil {
		t.Fatalf("flynn read message 1: %v", err)
	}

	ourSend, ourReceive, err := ours.Split()
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	interopTransport(t, ourSend, ourReceive, theirSend, theirReceive)
}

// interopTransport sends messages both ways and checks that each side reads what the other wrote. The
// messages differ in length so a counter that drifted would show up as a failure to authenticate.
func interopTransport(t *testing.T, ourSend, ourReceive *CipherState, theirSend, theirReceive *flynn.CipherState) {
	t.Helper()

	for i := range 8 {
		ours := bytes.Repeat([]byte{byte(i)}, i*100)
		ciphertext, err := ourSend.Encrypt(nil, ours)
		if err != nil {
			t.Fatalf("message %d: encrypt: %v", i, err)
		}
		got, err := theirReceive.Decrypt(nil, nil, ciphertext)
		if err != nil {
			t.Fatalf("message %d: flynn decrypt: %v", i, err)
		}
		if !bytes.Equal(got, ours) {
			t.Fatalf("message %d: flynn read %x, want %x", i, got, ours)
		}

		theirs := bytes.Repeat([]byte{byte(i)}, i*37+1)
		ciphertext, err = theirSend.Encrypt(nil, nil, theirs)
		if err != nil {
			t.Fatalf("message %d: flynn encrypt: %v", i, err)
		}
		got, err = ourReceive.Decrypt(nil, ciphertext)
		if err != nil {
			t.Fatalf("message %d: decrypt: %v", i, err)
		}
		if !bytes.Equal(got, theirs) {
			t.Fatalf("message %d: read %x, want %x", i, got, theirs)
		}
	}
}

// flynnConfig is the same suite, pattern and psk placement as this package, spelled the other library's
// way: NN with the psk as the first token is what NNpsk0 means.
func flynnConfig(initiator bool, psk, prologue []byte) flynn.Config {
	return flynn.Config{
		CipherSuite:           flynn.NewCipherSuite(flynn.DH25519, flynn.CipherChaChaPoly, flynn.HashSHA256),
		Pattern:               flynn.HandshakeNN,
		Initiator:             initiator,
		Prologue:              prologue,
		PresharedKey:          psk,
		PresharedKeyPlacement: 0,
	}
}

// interopInputs is a fresh secret and a prologue, both sides sharing them as httppooler's peers do.
func interopInputs(t *testing.T) (psk, prologue []byte) {
	t.Helper()

	psk = make([]byte, KeySize)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("random: %v", err)
	}

	return psk, []byte("HPOL\x00")
}
