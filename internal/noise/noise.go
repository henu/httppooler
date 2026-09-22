// Package noise implements Noise_NNpsk0_25519_ChaChaPoly_SHA256 exactly as the Noise specification
// (revision 34) composes it: the two-message handshake and the pair of cipher states that carries the
// transport afterwards.
//
// The package knows nothing about any application. The caller supplies the prologue and the pre-shared
// key, gets handshake messages as byte slices and frames them however its own protocol says. The
// primitives are the standard library's and x/crypto's; only the composition is here, and it is tested
// against the published vectors and against another implementation in both roles.
package noise

import (
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// protocolName is the suite this package implements; it is the first thing hashed into the handshake.
const protocolName = "Noise_NNpsk0_25519_ChaChaPoly_SHA256"

const (
	// KeySize is the length of the pre-shared key and of every symmetric key derived from it.
	KeySize = 32
	// PublicKeySize is the length of an X25519 public key.
	PublicKeySize = 32
	// TagSize is the length of a ChaCha20-Poly1305 authentication tag.
	TagSize = 16
	// HandshakeMessageSize is the length of either handshake message when the payload is empty, which
	// is the only way this handshake is used in practice.
	HandshakeMessageSize = PublicKeySize + TagSize
)

// The three steps of the handshake: the initiator's message, the responder's answer, and done.
const (
	stepInitiatorMessage = iota
	stepResponderMessage
	stepComplete
)

// Config is one side of a handshake.
type Config struct {
	// Initiator is true on the side that sends the first message.
	Initiator bool
	// PSK is the pre-shared key, exactly KeySize bytes. Both sides must have the same one.
	PSK []byte
	// Prologue is data both sides must already agree on, mixed into the handshake hash before
	// anything else. It may be empty, and a mismatch makes the handshake fail.
	Prologue []byte
	// Random is where the ephemeral keypair comes from. nil means crypto/rand; tests pass fixed bytes.
	Random io.Reader
}

// Handshake is one side of the handshake. It is used from one goroutine at a time and is finished once
// Split has returned; a Handshake that has refused anything refuses everything after it.
type Handshake struct {
	sym       symmetric
	initiator bool
	psk       []byte
	random    io.Reader
	e         *ecdh.PrivateKey
	re        *ecdh.PublicKey
	step      int
	failed    bool
}

// NewHandshake starts a handshake. Nothing is sent or derived yet; the first WriteMessage or
// ReadMessage does that.
func NewHandshake(cfg Config) (*Handshake, error) {
	// The pre-shared key is the whole security setup, so it is exactly the size the suite says.
	if len(cfg.PSK) != KeySize {
		return nil, fmt.Errorf("noise: pre-shared key is %d bytes, want %d", len(cfg.PSK), KeySize)
	}

	random := cfg.Random
	if random == nil {
		random = rand.Reader
	}

	return &Handshake{
		sym:       newSymmetric(cfg.Prologue),
		initiator: cfg.Initiator,
		psk:       cfg.PSK,
		random:    random,
		step:      stepInitiatorMessage,
	}, nil
}

// WriteMessage returns this side's next handshake message, carrying payload. The payload of the first
// message is not secret against an attacker who has the pre-shared key; httppooler sends none.
func (h *Handshake) WriteMessage(payload []byte) ([]byte, error) {
	// A handshake that has already refused something never speaks again.
	if h.failed {
		return nil, errors.New("noise: handshake has failed")
	}

	// There is nothing left to write once both messages are out.
	if h.step == stepComplete {
		return nil, errors.New("noise: handshake is already complete")
	}

	// The initiator writes the first message and the responder the second, never the other way round.
	if h.initiator != (h.step == stepInitiatorMessage) {
		return nil, errors.New("noise: not this side's turn to write")
	}

	message, err := h.writeMessage(payload)
	if err != nil {
		h.failed = true
		return nil, err
	}

	h.step++
	return message, nil
}

// ReadMessage takes the other side's next handshake message and returns the payload it carried. A
// wrong pre-shared key, a different prologue or a tampered message all fail here.
func (h *Handshake) ReadMessage(message []byte) ([]byte, error) {
	// A handshake that has already refused something never speaks again.
	if h.failed {
		return nil, errors.New("noise: handshake has failed")
	}

	// There is nothing left to read once both messages are in.
	if h.step == stepComplete {
		return nil, errors.New("noise: handshake is already complete")
	}

	// The responder reads the first message and the initiator the second, never the other way round.
	if h.initiator == (h.step == stepInitiatorMessage) {
		return nil, errors.New("noise: not this side's turn to read")
	}

	// A message is an ephemeral public key and at least the tag of its encrypted payload.
	if len(message) < HandshakeMessageSize {
		return nil, fmt.Errorf("noise: handshake message is %d bytes, want at least %d", len(message), HandshakeMessageSize)
	}

	payload, err := h.readMessage(message)
	if err != nil {
		h.failed = true
		return nil, err
	}

	h.step++
	return payload, nil
}

// Split ends the handshake and returns the two transport cipher states: the first encrypts what this
// side sends, the second decrypts what it receives. Both counters start at zero.
func (h *Handshake) Split() (send, receive *CipherState, err error) {
	// A handshake that has already refused something never speaks again.
	if h.failed {
		return nil, nil, errors.New("noise: handshake has failed")
	}

	// Splitting before both messages have gone by would hand out keys nobody has authenticated.
	if h.step != stepComplete {
		return nil, nil, errors.New("noise: handshake is not complete")
	}

	initiatorToResponder, responderToInitiator, err := h.sym.split()
	if err != nil {
		h.failed = true
		return nil, nil, err
	}

	if h.initiator {
		return initiatorToResponder, responderToInitiator, nil
	}
	return responderToInitiator, initiatorToResponder, nil
}

// writeMessage is WriteMessage once the guards have passed: the tokens of this side's message, in the
// order the pattern lists them.
func (h *Handshake) writeMessage(payload []byte) ([]byte, error) {
	// psk: the first token of the first message, before anything is encrypted.
	if h.step == stepInitiatorMessage {
		if err := h.sym.mixKeyAndHash(h.psk); err != nil {
			return nil, err
		}
	}

	// e: a fresh ephemeral, sent in the clear. In psk mode its public key is mixed into the chaining
	// key as well as the hash (specification section 9).
	e, err := generateKeypair(h.random)
	if err != nil {
		return nil, err
	}
	h.e = e
	public := e.PublicKey().Bytes()
	h.sym.mixHash(public)
	if err := h.sym.mixKey(public); err != nil {
		return nil, err
	}

	// ee: only the responder's message has it, and by then it knows the initiator's ephemeral.
	if h.step == stepResponderMessage {
		if err := h.mixDH(); err != nil {
			return nil, err
		}
	}

	ciphertext, err := h.sym.encryptAndHash(payload)
	if err != nil {
		return nil, err
	}

	return append(public, ciphertext...), nil
}

// readMessage is ReadMessage once the guards have passed, mirroring writeMessage token for token.
func (h *Handshake) readMessage(message []byte) ([]byte, error) {
	// psk: the first token of the first message, before anything is decrypted.
	if h.step == stepInitiatorMessage {
		if err := h.sym.mixKeyAndHash(h.psk); err != nil {
			return nil, err
		}
	}

	// e: the peer's ephemeral, hashed and mixed into the chaining key exactly as the sender did.
	re, err := ecdh.X25519().NewPublicKey(message[:PublicKeySize])
	if err != nil {
		return nil, fmt.Errorf("noise: bad ephemeral public key: %w", err)
	}
	h.re = re
	h.sym.mixHash(message[:PublicKeySize])
	if err := h.sym.mixKey(message[:PublicKeySize]); err != nil {
		return nil, err
	}

	// ee: only the responder's message has it; the initiator reads that one.
	if h.step == stepResponderMessage {
		if err := h.mixDH(); err != nil {
			return nil, err
		}
	}

	return h.sym.decryptAndHash(message[PublicKeySize:])
}

// mixDH is the ee token: the shared secret of the two ephemerals goes into the chaining key.
func (h *Handshake) mixDH() error {
	shared, err := h.e.ECDH(h.re)
	if err != nil {
		return fmt.Errorf("noise: X25519: %w", err)
	}
	return h.sym.mixKey(shared)
}

// generateKeypair reads a private key straight from random, the way the test vectors expect. The
// standard library's GenerateKey sometimes draws an extra byte first, which fixed vectors cannot match.
func generateKeypair(random io.Reader) (*ecdh.PrivateKey, error) {
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(random, key); err != nil {
		return nil, fmt.Errorf("noise: reading random bytes: %w", err)
	}
	return ecdh.X25519().NewPrivateKey(key)
}
