package wire

// Layer 2: after the handshake each direction is a stream of records, u16 length and then the
// ciphertext. The plaintexts concatenated are one byte stream per direction; record boundaries carry no
// meaning, so a message may span records and one record may carry several messages.

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/henu/httppooler/internal/noise"
)

const (
	// maxRecordSize is the largest ciphertext a record can hold, the bound the u16 length sets.
	maxRecordSize = noise.MaxMessageSize
	// minRecordSize is the tag plus the one plaintext byte an empty record is not allowed to omit.
	minRecordSize = noise.TagSize + 1
	// maxRecordPlaintext is what is left of a record once the tag is in it.
	maxRecordPlaintext = noise.MaxPlaintextSize
)

// RecordWriter encrypts what is written to it into records. One record per Write while the slice fits,
// so a caller that writes whole messages gets whole messages. It is not safe for concurrent use: the
// cipher counter must advance in the order the bytes go out, so callers hold one writer's lock.
type RecordWriter struct {
	w      io.Writer
	cipher *noise.CipherState
	buf    []byte
}

// NewRecordWriter wraps w with the cipher state for this direction.
func NewRecordWriter(w io.Writer, cipher *noise.CipherState) *RecordWriter {
	return &RecordWriter{w: w, cipher: cipher}
}

// Write encrypts p into as few records as it fits in and writes them. A short write of the underlying
// writer ends the stream: a record that went out in part cannot be resynchronised.
func (rw *RecordWriter) Write(p []byte) (int, error) {
	// An empty plaintext has no record to carry it, and nothing to say either.
	if len(p) == 0 {
		return 0, nil
	}

	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > maxRecordPlaintext {
			chunk = chunk[:maxRecordPlaintext]
		}

		rw.buf = rw.buf[:0]
		rw.buf = append(rw.buf, 0, 0)
		record, err := rw.cipher.Encrypt(rw.buf, chunk)
		if err != nil {
			return written, fmt.Errorf("wire: encrypting record: %w", err)
		}
		rw.buf = record
		binary.LittleEndian.PutUint16(record[:2], uint16(len(record)-2))

		if _, err := rw.w.Write(record); err != nil {
			return written, err
		}
		written += len(chunk)
		p = p[len(chunk):]
	}
	return written, nil
}

// RecordReader is the other end: the plaintexts of the records, one byte stream. It is not safe for
// concurrent use, for the same reason as the writer.
type RecordReader struct {
	r      io.Reader
	cipher *noise.CipherState
	header [2]byte
	record []byte
	plain  []byte
}

// NewRecordReader wraps r with the cipher state for this direction.
func NewRecordReader(r io.Reader, cipher *noise.CipherState) *RecordReader {
	return &RecordReader{r: r, cipher: cipher}
}

// Read hands out the plaintext of the records in order, reading one more record when what is left of
// the last one runs out. A record that does not authenticate ends the stream for good; nothing after it
// would decrypt either, since the counter is the replay defence.
func (rr *RecordReader) Read(p []byte) (int, error) {
	if len(rr.plain) == 0 {
		if err := rr.next(); err != nil {
			return 0, err
		}
	}

	n := copy(p, rr.plain)
	rr.plain = rr.plain[n:]
	return n, nil
}

// next reads and decrypts one record.
func (rr *RecordReader) next() error {
	if _, err := io.ReadFull(rr.r, rr.header[:]); err != nil {
		return err
	}
	size := int(binary.LittleEndian.Uint16(rr.header[:]))

	// A record carries a tag and at least one byte under it; shorter is malformed, longer than the
	// length field can count is impossible.
	if size < minRecordSize {
		return fmt.Errorf("wire: record is %d bytes, want at least %d", size, minRecordSize)
	}

	if cap(rr.record) < size {
		rr.record = make([]byte, size)
	}
	rr.record = rr.record[:size]
	if _, err := io.ReadFull(rr.r, rr.record); err != nil {
		return err
	}

	plain, err := rr.cipher.Decrypt(rr.record[:0], rr.record)
	if err != nil {
		return fmt.Errorf("wire: %w", err)
	}
	rr.plain = plain
	return nil
}

// VersionError is a peer that speaks a different version of the protocol. Version 0 requires equality;
// a later version may choose to accept older peers.
type VersionError struct {
	Ours   uint8
	Theirs uint8
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("wire: peer speaks protocol version %d, this build speaks %d", e.Theirs, e.Ours)
}

// Session is layers 0 to 2 established: the plaintext stream in each direction.
type Session struct {
	Reader *RecordReader
	Writer *RecordWriter
}

// Handshake does the preamble and the Noise handshake on rw and returns the record streams that carry
// everything after. The initiator sends its preamble and handshake message at once without waiting; the
// responder reads first and answers both together, so the whole setup is one round trip.
//
// The errors worth telling apart are ErrNotPeer, which the caller answers by closing silently, and
// *VersionError, which it logs. Anything else is a wrong secret, a tampered message or a dead
// connection, and none of those can be told from each other on purpose.
func Handshake(rw io.ReadWriter, psk []byte, initiator bool) (*Session, error) {
	hs, err := noise.NewHandshake(noise.Config{Initiator: initiator, PSK: psk, Prologue: Preamble()})
	if err != nil {
		return nil, err
	}

	if initiator {
		message, err := hs.WriteMessage(nil)
		if err != nil {
			return nil, err
		}
		if err := writeFramed(rw, Preamble(), message); err != nil {
			return nil, err
		}
		if err := readVersion(rw); err != nil {
			return nil, err
		}
		answer, err := readFramed(rw)
		if err != nil {
			return nil, err
		}
		if _, err := hs.ReadMessage(answer); err != nil {
			return nil, err
		}
	} else {
		if err := readVersion(rw); err != nil {
			return nil, err
		}
		message, err := readFramed(rw)
		if err != nil {
			return nil, err
		}
		if _, err := hs.ReadMessage(message); err != nil {
			return nil, err
		}
		answer, err := hs.WriteMessage(nil)
		if err != nil {
			return nil, err
		}
		if err := writeFramed(rw, Preamble(), answer); err != nil {
			return nil, err
		}
	}

	send, receive, err := hs.Split()
	if err != nil {
		return nil, err
	}
	return &Session{Reader: NewRecordReader(rw, receive), Writer: NewRecordWriter(rw, send)}, nil
}

// readVersion reads the peer's preamble and refuses a version this build does not speak.
func readVersion(r io.Reader) error {
	version, err := ReadPreamble(r)
	if err != nil {
		return err
	}
	if version != VersionNewest {
		return &VersionError{Ours: VersionNewest, Theirs: version}
	}
	return nil
}

// writeFramed writes the prefix as it is and the handshake message behind its u16 length, in one write.
func writeFramed(w io.Writer, prefix, message []byte) error {
	// A handshake message is framed by a u16 and so cannot be longer than one counts.
	if len(message) > maxRecordSize {
		return fmt.Errorf("wire: handshake message is %d bytes, at most %d fit in a frame", len(message), maxRecordSize)
	}

	out := make([]byte, 0, len(prefix)+2+len(message))
	out = append(out, prefix...)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(message)))
	out = append(out, message...)
	if _, err := w.Write(out); err != nil {
		return err
	}
	return nil
}

// readFramed reads one u16-framed handshake message.
func readFramed(r io.Reader) ([]byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("wire: reading handshake message: %w", err)
	}
	size := int(binary.LittleEndian.Uint16(header[:]))

	// A handshake message is an ephemeral public key and the tag of its payload at the very least.
	if size < noise.HandshakeMessageSize {
		return nil, fmt.Errorf("wire: handshake message is %d bytes, want at least %d", size, noise.HandshakeMessageSize)
	}

	message := make([]byte, size)
	if _, err := io.ReadFull(r, message); err != nil {
		return nil, fmt.Errorf("wire: reading handshake message: %w", err)
	}
	return message, nil
}
