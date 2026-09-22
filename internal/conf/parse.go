package conf

// The grammar, and nothing about what the keys mean: this file turns text into sections and refuses
// anything that is not a section header, a key and a value, a comment or a blank line.

import (
	"fmt"
	"strings"
)

// section is one [section] or [section "name"] with the keys written under it, in the order they were
// written so that the first thing wrong is the one reported.
type section struct {
	kind  string
	name  string
	line  int
	keys  map[string]value
	order []string
}

// value is what a key was set to and where.
type value struct {
	text string
	line int
}

// label names a section the way the file writes it, for error messages.
func (s *section) label() string {
	if s.name == "" {
		return "[" + s.kind + "]"
	}
	return fmt.Sprintf("[%s %q]", s.kind, s.name)
}

// The sections that exist, and whether each is named.
var sectionKinds = map[string]bool{
	"peer":    false,
	"consume": true,
	"provide": true,
}

// split reads the text into sections. Every refusal carries the line it was on.
func split(text, file string) ([]*section, error) {
	var sections []*section
	var current *section

	for i, raw := range strings.Split(text, "\n") {
		line := i + 1
		text := strings.TrimSpace(uncomment(raw))
		if text == "" {
			continue
		}

		if strings.HasPrefix(text, "[") {
			parsed, err := header(text, line, file)
			if err != nil {
				return nil, err
			}
			sections = append(sections, parsed)
			current = parsed
			continue
		}

		key, val, err := setting(text, line, file)
		if err != nil {
			return nil, err
		}

		// A key needs a section to belong to; there is no unnamed one at the top of the file.
		if current == nil {
			return nil, &Error{File: file, Line: line, Msg: fmt.Sprintf("key %q before any section", key)}
		}

		// One key is set once. Two settings are two intentions and there is no telling which is meant.
		if old, ok := current.keys[key]; ok {
			return nil, &Error{File: file, Line: line,
				Msg: fmt.Sprintf("%q in %s is already set on line %d", key, current.label(), old.line)}
		}

		current.keys[key] = value{text: val, line: line}
		current.order = append(current.order, key)
	}

	return sections, nil
}

// uncomment cuts a line at its first #, which starts a comment wherever it appears.
func uncomment(line string) string {
	if i := strings.IndexByte(line, '#'); i >= 0 {
		return line[:i]
	}
	return line
}

// header reads [section] or [section "name"].
func header(text string, line int, file string) (*section, error) {
	refuse := func(msg string) (*section, error) {
		return nil, &Error{File: file, Line: line, Msg: msg}
	}

	// A section header is bracketed, and the brackets are the whole line.
	if !strings.HasSuffix(text, "]") {
		return refuse("section header does not end in ]")
	}
	inner := strings.TrimSpace(text[1 : len(text)-1])

	kind := inner
	name := ""
	if cut := strings.IndexAny(inner, " \t"); cut >= 0 {
		kind = inner[:cut]
		quoted := strings.TrimSpace(inner[cut:])

		// A section's name is in double quotes and holds none of its own.
		if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
			return refuse("section name is not in double quotes")
		}
		name = quoted[1 : len(quoted)-1]
		if strings.Contains(name, `"`) {
			return refuse("section name holds a double quote")
		}
		if name == "" {
			return refuse("section name is empty")
		}
	}

	named, known := sectionKinds[kind]

	// Only the sections that exist exist; a typo is not a section nobody reads.
	if !known {
		return refuse(fmt.Sprintf("unknown section [%s]", kind))
	}

	// [consume] and [provide] name the service they are about; [peer] is about the peer itself.
	if named && name == "" {
		return refuse(fmt.Sprintf("[%s] needs a service name, as [%s \"myservice\"]", kind, kind))
	}
	if !named && name != "" {
		return refuse(fmt.Sprintf("[%s] takes no name", kind))
	}

	return &section{kind: kind, name: name, line: line, keys: map[string]value{}}, nil
}

// setting reads one key = value line.
func setting(text string, line int, file string) (string, string, error) {
	refuse := func(msg string) (string, string, error) {
		return "", "", &Error{File: file, Line: line, Msg: msg}
	}

	key, val, found := strings.Cut(text, "=")

	// Everything that is not a section header is a key and a value.
	if !found {
		return refuse("not a section header and not key = value")
	}

	key = strings.TrimSpace(key)
	val = strings.TrimSpace(val)

	// A key is a bare word: letters, digits and underscores.
	if key == "" || !isWord(key) {
		return refuse(fmt.Sprintf("%q is not a key", key))
	}

	// A key with nothing after the = says nothing; leaving it out is how a key goes unset.
	if val == "" {
		return refuse(fmt.Sprintf("%q has no value", key))
	}

	return key, val, nil
}

// isWord is the shape of a key.
func isWord(s string) bool {
	for _, r := range s {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		digit := r >= '0' && r <= '9'
		if !letter && !digit && r != '_' {
			return false
		}
	}
	return true
}
