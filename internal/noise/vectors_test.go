package noise

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
)

// vector is one block of testdata/vectors.txt: the two fixed ephemerals, the inputs both sides share,
// and four messages — the two handshake messages and then one transport message each way.
type vector struct {
	name            string
	initiatorRandom []byte
	responderRandom []byte
	prologue        []byte
	psk             []byte
	payloads        [4][]byte
	ciphertexts     [4][]byte
}

// TestVectors runs both sides against the published vectors for this suite. Every byte a side writes
// must equal the vector, and every byte it reads must come back as the payload the vector names.
func TestVectors(t *testing.T) {
	vectors := readVectors(t, "testdata/vectors.txt")
	if len(vectors) != 4 {
		t.Fatalf("read %d vectors, want the 4 in the file", len(vectors))
	}

	for i, v := range vectors {
		t.Run(fmt.Sprintf("%d_%s", i, v.name), func(t *testing.T) {
			initiator, err := NewHandshake(Config{
				Initiator: true,
				PSK:       v.psk,
				Prologue:  v.prologue,
				Random:    bytes.NewReader(v.initiatorRandom),
			})
			if err != nil {
				t.Fatalf("initiator: %v", err)
			}

			responder, err := NewHandshake(Config{
				PSK:      v.psk,
				Prologue: v.prologue,
				Random:   bytes.NewReader(v.responderRandom),
			})
			if err != nil {
				t.Fatalf("responder: %v", err)
			}

			// Message 0, initiator to responder, and message 1 back.
			exchange(t, "message 0", initiator, responder, v.payloads[0], v.ciphertexts[0])
			exchange(t, "message 1", responder, initiator, v.payloads[1], v.ciphertexts[1])

			initiatorSend, initiatorReceive, err := initiator.Split()
			if err != nil {
				t.Fatalf("initiator split: %v", err)
			}
			responderSend, responderReceive, err := responder.Split()
			if err != nil {
				t.Fatalf("responder split: %v", err)
			}

			// The first transport message of each direction.
			carry(t, "message 2", initiatorSend, responderReceive, v.payloads[2], v.ciphertexts[2])
			carry(t, "message 3", responderSend, initiatorReceive, v.payloads[3], v.ciphertexts[3])
		})
	}
}

// exchange writes one handshake message, checks it against the vector, and reads it on the other side.
func exchange(t *testing.T, what string, writer, reader *Handshake, payload, want []byte) {
	t.Helper()

	message, err := writer.WriteMessage(payload)
	if err != nil {
		t.Fatalf("%s: write: %v", what, err)
	}
	if !bytes.Equal(message, want) {
		t.Fatalf("%s: wrote %x, vector says %x", what, message, want)
	}

	got, err := reader.ReadMessage(message)
	if err != nil {
		t.Fatalf("%s: read: %v", what, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("%s: read payload %x, want %x", what, got, payload)
	}
}

// carry does the same for a transport message.
func carry(t *testing.T, what string, send, receive *CipherState, payload, want []byte) {
	t.Helper()

	ciphertext, err := send.Encrypt(nil, payload)
	if err != nil {
		t.Fatalf("%s: encrypt: %v", what, err)
	}
	if !bytes.Equal(ciphertext, want) {
		t.Fatalf("%s: encrypted to %x, vector says %x", what, ciphertext, want)
	}

	got, err := receive.Decrypt(nil, ciphertext)
	if err != nil {
		t.Fatalf("%s: decrypt: %v", what, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("%s: decrypted to %x, want %x", what, got, payload)
	}
}

// readVectors parses the vendored file: key=value lines with hex values, blocks ended by a blank line,
// '#' to end of line. Anything it does not recognise fails the test rather than being skipped.
func readVectors(t *testing.T, path string) []vector {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer file.Close()

	var vectors []vector
	var current *vector
	lines := bufio.NewScanner(file)
	for number := 1; lines.Scan(); number++ {
		line := strings.TrimSpace(lines.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			current = nil
			continue
		}

		key, value, found := strings.Cut(line, "=")
		if !found {
			t.Fatalf("%s:%d: no '=' in %q", path, number, line)
		}
		if current == nil {
			vectors = append(vectors, vector{})
			current = &vectors[len(vectors)-1]
		}

		if key == "handshake" {
			current.name = value
			continue
		}

		raw, err := hex.DecodeString(value)
		if err != nil {
			t.Fatalf("%s:%d: %v", path, number, err)
		}

		switch key {
		case "gen_init_ephemeral":
			current.initiatorRandom = raw
		case "gen_resp_ephemeral":
			current.responderRandom = raw
		case "prologue":
			current.prologue = raw
		case "preshared_key":
			current.psk = raw
		default:
			index, field, ok := messageKey(key)
			if !ok {
				t.Fatalf("%s:%d: unknown key %q", path, number, key)
			}
			if field == "payload" {
				current.payloads[index] = raw
			} else {
				current.ciphertexts[index] = raw
			}
		}
	}
	if err := lines.Err(); err != nil {
		t.Fatalf("read: %v", err)
	}

	return vectors
}

// messageKey splits msg_<index>_payload and msg_<index>_ciphertext, the only per-message keys the four
// vendored blocks use.
func messageKey(key string) (index int, field string, ok bool) {
	rest, found := strings.CutPrefix(key, "msg_")
	if !found {
		return 0, "", false
	}

	number, field, found := strings.Cut(rest, "_")
	if !found {
		return 0, "", false
	}
	if field != "payload" && field != "ciphertext" {
		return 0, "", false
	}
	if len(number) != 1 || number[0] < '0' || number[0] > '3' {
		return 0, "", false
	}

	return int(number[0] - '0'), field, true
}
