// Package conf reads httppooler.conf.
//
// The grammar is four things: [section] or [section "name"], key = value, # to the end of the line, and
// blank lines. Anything else refuses the whole conf with the line it was on, and so do unknown sections
// and keys, duplicates, missing required keys and values that do not parse. A conf is taken whole or not
// at all: the daemon keeps running on the one it already has.
package conf

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Role is what a peer does on the connection. A conf without one is roleless: it parses, and the daemon
// idles on it until someone finishes the file.
type Role int

const (
	RoleNone Role = iota
	RoleServer
	RoleClient
)

// String names a role for logs and for the conf grammar.
func (r Role) String() string {
	switch r {
	case RoleServer:
		return "server"
	case RoleClient:
		return "client"
	}
	return "none"
}

// Defaults, which are the values the README template shows.
const (
	DefaultListen       = "0.0.0.0:7420"
	DefaultQueueTimeout = 10 * time.Minute
	DefaultConcurrent   = 1
	DefaultPriority     = 100
	// SecretSize is the shared secret in bytes; the conf writes it as twice as many hex characters.
	SecretSize = 32
)

// Conf is a whole configuration file, defaults filled in.
type Conf struct {
	Role         Role
	Listen       string
	Server       string
	Secret       []byte
	Name         string
	QueueTimeout time.Duration
	Consume      []Consume
	Provide      []Provide
}

// Consume is a local port applications send requests to.
type Consume struct {
	Service string
	Listen  string
}

// Provide is a local upstream that joins a service's pool.
type Provide struct {
	Service       string
	Upstream      string
	MaxConcurrent uint16
	Priority      uint16
}

// Error is a refusal with the line it happened on.
type Error struct {
	File string
	Line int
	Msg  string
}

func (e *Error) Error() string {
	if e.File == "" {
		return fmt.Sprintf("line %d: %s", e.Line, e.Msg)
	}
	return fmt.Sprintf("%s:%d: %s", e.File, e.Line, e.Msg)
}

// Load reads and parses a conf file.
func Load(path string) (*Conf, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(strings.NewReader(string(data)), path)
}

// Parse reads a conf from r. file only names the source in error messages.
func Parse(r io.Reader, file string) (*Conf, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	sections, err := split(string(data), file)
	if err != nil {
		return nil, err
	}
	return build(sections, file)
}
