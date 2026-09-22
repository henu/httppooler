package noise

// The specification's symmetric state (h, ck and the key that comes with it) and cipher state (a key
// and its message counter). noise.go drives these; the pattern is not their business.

import (
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	// MaxMessageSize is the largest Noise message, the bound the specification itself sets.
	MaxMessageSize = 65535
	// MaxPlaintextSize is what is left of a message once the tag is in it.
	MaxPlaintextSize = MaxMessageSize - TagSize
	// nonceReserved is the counter value the specification reserves. A stream that reaches it is
	// finished: nonces are never reused and never wrap.
	nonceReserved = ^uint64(0)
)

// symmetric is the handshake hash h, the chaining key ck, and the cipher state keyed by the mixes so
// far. In this pattern the psk token keys it before anything is encrypted, so cipher is never nil once
// the first message is under way.
type symmetric struct {
	h      [sha256.Size]byte
	ck     [sha256.Size]byte
	cipher *CipherState
}

// newSymmetric is InitializeSymmetric followed by MixHash(prologue). The protocol name is longer than
// the hash output, so h starts as its hash rather than the padded name.
func newSymmetric(prologue []byte) symmetric {
	s := symmetric{h: sha256.Sum256([]byte(protocolName))}
	s.ck = s.h
	s.mixHash(prologue)
	return s
}

// mixHash is h = SHA256(h ‖ data).
func (s *symmetric) mixHash(data []byte) {
	sum := sha256.New()
	sum.Write(s.h[:])
	sum.Write(data)
	copy(s.h[:], sum.Sum(nil))
}

// mixKey derives a new chaining key and a new cipher key from ikm.
func (s *symmetric) mixKey(ikm []byte) error {
	out, err := hkdfExpand(s.ck[:], ikm, 2)
	if err != nil {
		return err
	}

	cipher, err := newCipherState(out[1])
	if err != nil {
		return err
	}

	copy(s.ck[:], out[0])
	s.cipher = cipher
	return nil
}

// mixKeyAndHash is the psk token: three outputs, the middle one going into the hash.
func (s *symmetric) mixKeyAndHash(ikm []byte) error {
	out, err := hkdfExpand(s.ck[:], ikm, 3)
	if err != nil {
		return err
	}

	cipher, err := newCipherState(out[2])
	if err != nil {
		return err
	}

	copy(s.ck[:], out[0])
	s.mixHash(out[1])
	s.cipher = cipher
	return nil
}

// encryptAndHash encrypts a handshake payload with h as associated data and hashes the ciphertext.
func (s *symmetric) encryptAndHash(plaintext []byte) ([]byte, error) {
	// The state is keyed by the psk token before any payload is written, so an unkeyed state here
	// would mean the pattern was driven out of order.
	if s.cipher == nil {
		return nil, errors.New("noise: encrypting before the state is keyed")
	}

	ciphertext, err := s.cipher.seal(nil, s.h[:], plaintext)
	if err != nil {
		return nil, err
	}

	s.mixHash(ciphertext)
	return ciphertext, nil
}

// decryptAndHash is encryptAndHash backwards: h as it was before this ciphertext is the associated
// data, and the ciphertext is hashed once it has been authenticated.
func (s *symmetric) decryptAndHash(ciphertext []byte) ([]byte, error) {
	// The state is keyed by the psk token before any payload is read, so an unkeyed state here would
	// mean the pattern was driven out of order.
	if s.cipher == nil {
		return nil, errors.New("noise: decrypting before the state is keyed")
	}

	plaintext, err := s.cipher.open(nil, s.h[:], ciphertext)
	if err != nil {
		return nil, err
	}

	s.mixHash(ciphertext)
	return plaintext, nil
}

// split ends the handshake: two keys from the chaining key alone, the first for initiator to responder
// and the second for responder to initiator.
func (s *symmetric) split() (*CipherState, *CipherState, error) {
	out, err := hkdfExpand(s.ck[:], nil, 2)
	if err != nil {
		return nil, nil, err
	}

	initiatorToResponder, err := newCipherState(out[0])
	if err != nil {
		return nil, nil, err
	}

	responderToInitiator, err := newCipherState(out[1])
	if err != nil {
		return nil, nil, err
	}

	return initiatorToResponder, responderToInitiator, nil
}

// hkdfExpand is the specification's HKDF: temp = HMAC(ck, ikm), then out1 = HMAC(temp, 0x01) and each
// further output HMAC(temp, previous ‖ i). Extract and Expand with an empty info are exactly that, and
// outputs is 2 or 3 as the caller's token says.
func hkdfExpand(ck, ikm []byte, outputs int) ([][]byte, error) {
	temp, err := hkdf.Extract(sha256.New, ikm, ck)
	if err != nil {
		return nil, fmt.Errorf("noise: HKDF extract: %w", err)
	}

	stream, err := hkdf.Expand(sha256.New, temp, "", outputs*KeySize)
	if err != nil {
		return nil, fmt.Errorf("noise: HKDF expand: %w", err)
	}

	out := make([][]byte, outputs)
	for i := range out {
		out[i] = stream[i*KeySize : (i+1)*KeySize]
	}
	return out, nil
}

// CipherState carries one direction of messages: a key that never changes and a counter that is never
// reused. The counter is the whole replay and reordering defence, so the messages of one direction must
// be decrypted in the order they were encrypted, each exactly once.
type CipherState struct {
	aead  cipher.AEAD
	nonce uint64
}

// newCipherState keys a direction. The key is always KeySize bytes; anything else is a bug above.
func newCipherState(key []byte) (*CipherState, error) {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, fmt.Errorf("noise: ChaCha20-Poly1305: %w", err)
	}
	return &CipherState{aead: aead}, nil
}

// Encrypt appends the next message of this direction to out and returns it: the ciphertext and its
// tag, with no associated data, as the transport uses it.
func (c *CipherState) Encrypt(out, plaintext []byte) ([]byte, error) {
	return c.seal(out, nil, plaintext)
}

// Decrypt appends the plaintext of the next message of this direction to out and returns it. A message
// that does not authenticate leaves the counter where it was; the stream is over either way, since
// nothing that follows would decrypt.
func (c *CipherState) Decrypt(out, ciphertext []byte) ([]byte, error) {
	return c.open(out, nil, ciphertext)
}

// Nonce is how many messages this direction has carried, for logs.
func (c *CipherState) Nonce() uint64 {
	return c.nonce
}

// seal is Encrypt with the associated data the handshake needs.
func (c *CipherState) seal(out, ad, plaintext []byte) ([]byte, error) {
	// The counter never wraps: the specification reserves its last value.
	if c.nonce == nonceReserved {
		return nil, errors.New("noise: message counter is exhausted")
	}

	// A Noise message, tag included, fits in 65535 bytes.
	if len(plaintext) > MaxPlaintextSize {
		return nil, fmt.Errorf("noise: plaintext is %d bytes, at most %d fit in a message", len(plaintext), MaxPlaintextSize)
	}

	sealed := c.aead.Seal(out, c.nonceBytes(), plaintext, ad)
	c.nonce++
	return sealed, nil
}

// open is Decrypt with the associated data the handshake needs.
func (c *CipherState) open(out, ad, ciphertext []byte) ([]byte, error) {
	// The counter never wraps: the specification reserves its last value.
	if c.nonce == nonceReserved {
		return nil, errors.New("noise: message counter is exhausted")
	}

	// A message the sender could not have produced is refused before the tag is even checked.
	if len(ciphertext) < TagSize || len(ciphertext) > MaxMessageSize {
		return nil, fmt.Errorf("noise: message is %d bytes, want %d to %d", len(ciphertext), TagSize, MaxMessageSize)
	}

	plaintext, err := c.aead.Open(out, c.nonceBytes(), ciphertext, ad)
	if err != nil {
		return nil, fmt.Errorf("noise: message %d does not authenticate", c.nonce)
	}

	c.nonce++
	return plaintext, nil
}

// nonceBytes is the counter as the suite writes it: four zero bytes and then u64 little-endian.
func (c *CipherState) nonceBytes() []byte {
	var nonce [chacha20poly1305.NonceSize]byte
	binary.LittleEndian.PutUint64(nonce[4:], c.nonce)
	return nonce[:]
}
