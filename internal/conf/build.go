package conf

// What the keys mean: defaults, the keys each section has, the keys each role has, and the values that
// parse. Everything here refuses with the line the key was written on.

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
	"unicode/utf8"
)

// build turns parsed sections into a conf with its defaults filled in.
func build(sections []*section, file string) (*Conf, error) {
	c := &Conf{
		Listen:       DefaultListen,
		Name:         hostname(),
		QueueTimeout: DefaultQueueTimeout,
	}

	var peer *section
	consumed := map[string]int{}
	provided := map[string]int{}

	for _, s := range sections {
		var err error
		switch s.kind {
		case "peer":
			// One [peer] section: a peer is one peer.
			if peer != nil {
				return nil, &Error{File: file, Line: s.line,
					Msg: fmt.Sprintf("[peer] is already on line %d", peer.line)}
			}
			peer = s
			err = c.peer(s, file)
		case "consume":
			err = c.consume(s, file, consumed)
		case "provide":
			err = c.provide(s, file, provided)
		}
		if err != nil {
			return nil, err
		}
	}

	if err := c.checkRole(peer, file); err != nil {
		return nil, err
	}
	return c, nil
}

// peer reads the [peer] section.
func (c *Conf) peer(s *section, file string) error {
	if err := s.known(file, "role", "listen", "server", "secret", "name", "queue_timeout"); err != nil {
		return err
	}

	for _, key := range s.order {
		v := s.keys[key]
		var err error
		switch key {
		case "role":
			c.Role, err = role(v, file)
		case "listen":
			c.Listen, err = address(v, file, "listen", false)
		case "server":
			c.Server, err = address(v, file, "server", true)
		case "secret":
			c.Secret, err = secret(v, file)
		case "name":
			c.Name, err = text(v, file, "name")
		case "queue_timeout":
			c.QueueTimeout, err = duration(v, file)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// consume reads one [consume "service"] section.
func (c *Conf) consume(s *section, file string, seen map[string]int) error {
	if err := s.known(file, "listen"); err != nil {
		return err
	}
	if err := s.service(file, seen); err != nil {
		return err
	}

	// A consume section is the port applications use, so there is nothing to default it to.
	v, ok := s.keys["listen"]
	if !ok {
		return &Error{File: file, Line: s.line, Msg: fmt.Sprintf("%s has no listen", s.label())}
	}

	listen, err := address(v, file, "listen", false)
	if err != nil {
		return err
	}
	c.Consume = append(c.Consume, Consume{Service: s.name, Listen: listen})
	return nil
}

// provide reads one [provide "service"] section.
func (c *Conf) provide(s *section, file string, seen map[string]int) error {
	if err := s.known(file, "upstream", "max_concurrent", "priority"); err != nil {
		return err
	}
	if err := s.service(file, seen); err != nil {
		return err
	}

	// A provide section is an upstream, and an upstream is an address.
	v, ok := s.keys["upstream"]
	if !ok {
		return &Error{File: file, Line: s.line, Msg: fmt.Sprintf("%s has no upstream", s.label())}
	}

	p := Provide{Service: s.name, MaxConcurrent: DefaultConcurrent, Priority: DefaultPriority}
	var err error
	if p.Upstream, err = address(v, file, "upstream", true); err != nil {
		return err
	}
	if v, ok := s.keys["max_concurrent"]; ok {
		if p.MaxConcurrent, err = number(v, file, "max_concurrent", 1); err != nil {
			return err
		}
	}
	if v, ok := s.keys["priority"]; ok {
		if p.Priority, err = number(v, file, "priority", 0); err != nil {
			return err
		}
	}

	c.Provide = append(c.Provide, p)
	return nil
}

// checkRole refuses a conf whose role and keys disagree. A conf with no role at all is not refused: the
// daemon logs it and idles, which is what a freshly installed conf does.
func (c *Conf) checkRole(peer *section, file string) error {
	if c.Role == RoleNone {
		return nil
	}

	line := peer.line
	refuse := func(key, msg string) error {
		at := line
		if v, ok := peer.keys[key]; ok {
			at = v.line
		}
		return &Error{File: file, Line: at, Msg: msg}
	}

	// The secret is the whole security setup, and both roles need the same one.
	if len(c.Secret) == 0 {
		return refuse("secret", "no secret; run httppooler genkey and put it in secret")
	}

	// A client dials the server, so it needs to know where the server is.
	if c.Role == RoleClient && c.Server == "" {
		return refuse("server", "a client has no server to dial")
	}

	// listen and queue_timeout belong to the peer that holds the pool.
	if c.Role == RoleClient {
		for _, key := range []string{"listen", "queue_timeout"} {
			if _, ok := peer.keys[key]; ok {
				return refuse(key, fmt.Sprintf("%q is a server key, and this peer is a client", key))
			}
		}
	}

	// server is where a client dials, and the server does not dial itself.
	if c.Role == RoleServer {
		if _, ok := peer.keys["server"]; ok {
			return refuse("server", `"server" is a client key, and this peer is the server`)
		}
	}

	return nil
}

// known refuses a key this section does not have, naming the first one written.
func (s *section) known(file string, allowed ...string) error {
	for _, key := range s.order {
		found := false
		for _, a := range allowed {
			if key == a {
				found = true
				break
			}
		}
		if !found {
			return &Error{File: file, Line: s.keys[key].line,
				Msg: fmt.Sprintf("unknown key %q in %s", key, s.label())}
		}
	}
	return nil
}

// service checks the name in the section header and refuses a second section about the same service.
func (s *section) service(file string, seen map[string]int) error {
	// A service name travels as a str8 and names things in logs, so it is short text.
	if err := name(s.name, s.line, file, "service name"); err != nil {
		return err
	}

	// Two sections of one kind for one service are two answers to the same question.
	if line, ok := seen[s.name]; ok {
		return &Error{File: file, Line: s.line,
			Msg: fmt.Sprintf("%s is already on line %d", s.label(), line)}
	}

	seen[s.name] = s.line
	return nil
}

// role is server or client, spelled as the README spells them.
func role(v value, file string) (Role, error) {
	switch v.text {
	case "server":
		return RoleServer, nil
	case "client":
		return RoleClient, nil
	}
	return RoleNone, &Error{File: file, Line: v.line,
		Msg: fmt.Sprintf("role %q is neither server nor client", v.text)}
}

// address is host:port with the port written as a number. Dialled addresses need a host as well; a
// listen address without one listens on everything.
func address(v value, file, what string, needHost bool) (string, error) {
	refuse := func(msg string) (string, error) {
		return "", &Error{File: file, Line: v.line, Msg: fmt.Sprintf("%s: %s", what, msg)}
	}

	host, port, err := net.SplitHostPort(v.text)
	if err != nil {
		return refuse(fmt.Sprintf("%q is not host:port", v.text))
	}

	// A named port is one more thing that can be wrong on the machine; the port is a number.
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return refuse(fmt.Sprintf("%q is not a port number", port))
	}

	// There is nothing to dial without a host.
	if needHost && host == "" {
		return refuse(fmt.Sprintf("%q has no host", v.text))
	}

	return v.text, nil
}

// secret is the shared key, written as twice as many hex characters as it has bytes.
func secret(v value, file string) ([]byte, error) {
	refuse := func(msg string) ([]byte, error) {
		return nil, &Error{File: file, Line: v.line, Msg: "secret: " + msg}
	}

	if len(v.text) != SecretSize*2 {
		return refuse(fmt.Sprintf("%d characters, want %d", len(v.text), SecretSize*2))
	}

	key, err := hex.DecodeString(v.text)
	if err != nil {
		return refuse("not hexadecimal")
	}
	return key, nil
}

// duration is a Go duration, and a timeout of no time at all is not a timeout.
func duration(v value, file string) (time.Duration, error) {
	d, err := time.ParseDuration(v.text)
	if err != nil {
		return 0, &Error{File: file, Line: v.line,
			Msg: fmt.Sprintf("queue_timeout %q is not a duration, as 10m or 30s", v.text)}
	}
	if d <= 0 {
		return 0, &Error{File: file, Line: v.line, Msg: fmt.Sprintf("queue_timeout %q is not a wait", v.text)}
	}
	return d, nil
}

// number is a u16 key with the smallest value it is allowed to take.
func number(v value, file, what string, low uint64) (uint16, error) {
	n, err := strconv.ParseUint(v.text, 10, 16)
	if err != nil {
		return 0, &Error{File: file, Line: v.line,
			Msg: fmt.Sprintf("%s %q is not a number from %d to 65535", what, v.text, low)}
	}
	if n < low {
		return 0, &Error{File: file, Line: v.line,
			Msg: fmt.Sprintf("%s is %d, want at least %d", what, n, low)}
	}
	return uint16(n), nil
}

// text is a key whose value is short text, which is what the wire carries names as.
func text(v value, file, what string) (string, error) {
	if err := name(v.text, v.line, file, what); err != nil {
		return "", err
	}
	return v.text, nil
}

// name is the shape every name has: text, not empty, and short enough for the str8 it travels in.
func name(s string, line int, file, what string) error {
	if s == "" {
		return &Error{File: file, Line: line, Msg: fmt.Sprintf("%s is empty", what)}
	}
	if len(s) > 255 {
		return &Error{File: file, Line: line,
			Msg: fmt.Sprintf("%s is %d bytes, at most 255 fit on the wire", what, len(s))}
	}
	if !utf8.ValidString(s) {
		return &Error{File: file, Line: line, Msg: fmt.Sprintf("%s is not valid UTF-8", what)}
	}
	return nil
}

// hostname is what a peer is called when the conf does not say.
func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" && len(h) <= 255 && utf8.ValidString(h) {
		return h
	}
	return "httppooler"
}
