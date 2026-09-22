// Package wire is everything httppooler puts on a connection: the five-byte preamble, the encrypted
// record stream that carries the rest, and the messages parsed out of it.
//
// PROTOCOL.md is authoritative for every byte here. Parsing is strictly sequential and refuses malformed
// input whole: a reader consumes every byte of every item, never skips, and never recovers.
package wire

import (
	"errors"
	"fmt"
	"io"
)

// The preamble is four magic bytes and a version.
const (
	// Version0Initial is the first version, named for what it introduced. The next would be
	// Version1<Feature>; any layout change, a new message kind included, is a new version.
	Version0Initial uint8 = 0
	// VersionNewest is the version this build speaks.
	VersionNewest = Version0Initial
	// PreambleSize is the size of the preamble, which is also the size of the Noise prologue.
	PreambleSize = 5
)

// magic starts every connection, so a port scanner is told apart from a peer that disagrees about the
// version.
var magic = [4]byte{'H', 'P', 'O', 'L'}

// ErrNotPeer is what ReadPreamble returns when the first bytes are not httppooler's. The caller closes
// without a word: on a public port this is ordinary noise.
var ErrNotPeer = errors.New("wire: not an httppooler peer")

// Preamble is the five bytes this build sends immediately after connecting, without waiting for the
// other side. The same bytes are the Noise prologue.
func Preamble() []byte {
	return []byte{magic[0], magic[1], magic[2], magic[3], VersionNewest}
}

// ReadPreamble reads the peer's five bytes and returns its version. Wrong magic gives ErrNotPeer; the
// version is returned as it came, and the caller decides whether it can speak it.
func ReadPreamble(r io.Reader) (uint8, error) {
	var buf [PreambleSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, fmt.Errorf("wire: reading preamble: %w", err)
	}

	// Anything that does not begin with the magic is not a peer at all.
	if buf[0] != magic[0] || buf[1] != magic[1] || buf[2] != magic[2] || buf[3] != magic[3] {
		return 0, ErrNotPeer
	}

	return buf[4], nil
}
