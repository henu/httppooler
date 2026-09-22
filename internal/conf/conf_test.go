package conf

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func parse(t *testing.T, text string) (*Conf, error) {
	t.Helper()
	return Parse(strings.NewReader(text), "httppooler.conf")
}

func good(t *testing.T, text string) *Conf {
	t.Helper()
	c, err := parse(t, text)
	if err != nil {
		t.Fatalf("refused a conf that is fine: %v", err)
	}
	return c
}

// TestInstalledTemplate is the file the install puts in place, read out of the install script itself so
// that the two cannot drift: a template that stopped parsing would give every fresh install a daemon that
// refuses its own conf. It parses, it has no role, and the daemon idles on it.
func TestInstalledTemplate(t *testing.T) {
	c := good(t, installTemplate(t))

	if c.Role != RoleNone {
		t.Fatalf("role is %s, want none", c.Role)
	}
	if len(c.Secret) != SecretSize {
		t.Fatalf("secret is %d bytes, want %d", len(c.Secret), SecretSize)
	}
	if len(c.Consume) != 0 || len(c.Provide) != 0 {
		t.Fatal("the template has sections in it")
	}

	// Every key the template leaves commented is a key the daemon has a default for, and these are the
	// defaults the README shows beside them.
	if c.Listen != DefaultListen || c.QueueTimeout != DefaultQueueTimeout || c.Name == "" {
		t.Fatalf("the template's defaults came out as %+v", c)
	}
}

// installTemplate is the heredoc the install script writes the conf from, with a real secret in place of
// the one genkey makes.
func installTemplate(t *testing.T) string {
	t.Helper()

	script, err := os.ReadFile(filepath.Join("..", "..", "deploy", "install.sh"))
	if err != nil {
		t.Fatalf("reading the install script: %v", err)
	}

	_, rest, found := strings.Cut(string(script), "<<CONF\n")
	if !found {
		t.Fatal("the install script has no conf heredoc; has it been rewritten?")
	}
	template, _, found := strings.Cut(rest, "\nCONF\n")
	if !found {
		t.Fatal("the install script's conf heredoc does not end")
	}

	return strings.Replace(template, "$secret", testSecret, 1)
}

// TestDefaults is every key the README says has one, left out.
func TestDefaults(t *testing.T) {
	c := good(t, `
[peer]
role = server
secret = `+testSecret+`

[provide "ollama"]
upstream = 127.0.0.1:11434
`)

	if c.Listen != DefaultListen {
		t.Errorf("listen is %q, want %q", c.Listen, DefaultListen)
	}
	if c.QueueTimeout != DefaultQueueTimeout {
		t.Errorf("queue_timeout is %v, want %v", c.QueueTimeout, DefaultQueueTimeout)
	}
	if c.Name == "" {
		t.Error("name is empty, want the hostname")
	}
	if c.Provide[0].MaxConcurrent != DefaultConcurrent {
		t.Errorf("max_concurrent is %d, want %d", c.Provide[0].MaxConcurrent, DefaultConcurrent)
	}
	if c.Provide[0].Priority != DefaultPriority {
		t.Errorf("priority is %d, want %d", c.Provide[0].Priority, DefaultPriority)
	}
}

// TestServer is a whole server conf, every value read back.
func TestServer(t *testing.T) {
	c := good(t, `
# the news box
[peer]
role = server
listen = 0.0.0.0:7420
secret = `+testSecret+`
name = uutiskooste
queue_timeout = 30s

[consume "ollama"]     # the site talks to this port
listen = 127.0.0.1:8080

[provide "ollama"]
upstream = 127.0.0.1:11434
max_concurrent = 2
priority = 200

[consume "whisper"]
listen = 127.0.0.1:8081
`)

	if c.Role != RoleServer || c.Listen != "0.0.0.0:7420" || c.Name != "uutiskooste" {
		t.Fatalf("peer section read back as %+v", c)
	}
	if c.QueueTimeout != 30*time.Second {
		t.Fatalf("queue_timeout is %v", c.QueueTimeout)
	}
	if len(c.Secret) != SecretSize {
		t.Fatalf("secret is %d bytes, want %d", len(c.Secret), SecretSize)
	}

	want := []Consume{{Service: "ollama", Listen: "127.0.0.1:8080"}, {Service: "whisper", Listen: "127.0.0.1:8081"}}
	if len(c.Consume) != len(want) {
		t.Fatalf("%d consume sections, want %d", len(c.Consume), len(want))
	}
	for i, w := range want {
		if c.Consume[i] != w {
			t.Errorf("consume %d is %+v, want %+v", i, c.Consume[i], w)
		}
	}

	provide := Provide{Service: "ollama", Upstream: "127.0.0.1:11434", MaxConcurrent: 2, Priority: 200}
	if len(c.Provide) != 1 || c.Provide[0] != provide {
		t.Fatalf("provide read back as %+v, want %+v", c.Provide, provide)
	}
}

// TestClient is the other role, with the keys that belong to it.
func TestClient(t *testing.T) {
	c := good(t, `
[peer]
role = client
server = pool.example.com:7420
secret = `+testSecret+`

[provide "ollama"]
upstream = 127.0.0.1:11434
`)

	if c.Role != RoleClient || c.Server != "pool.example.com:7420" {
		t.Fatalf("client conf read back as %+v", c)
	}
}

// TestRefusals is every way a conf is refused, each with the line it is refused on.
func TestRefusals(t *testing.T) {
	cases := []struct {
		name string
		text string
		line int
		want string
	}{
		{"unknown section", "[pool]\n", 1, "unknown section [pool]"},
		{"peer takes no name", "[peer \"x\"]\n", 1, "takes no name"},
		{"provide needs a name", "[provide]\n", 1, "needs a service name"},
		{"unquoted name", "[provide ollama]\n", 1, "not in double quotes"},
		{"empty name", "[provide \"\"]\n", 1, "name is empty"},
		{"unclosed header", "[peer\n", 1, "does not end in ]"},
		{"not a setting", "[peer]\nrole server\n", 2, "not key = value"},
		{"no value", "[peer]\nrole =\n", 2, `"role" has no value`},
		{"key before a section", "role = server\n", 1, "before any section"},
		{"unknown key", "[peer]\nnick = homebox\n", 2, `unknown key "nick"`},
		{"unknown key in provide", "[provide \"s\"]\nupstream = 127.0.0.1:1\nslots = 2\n", 3, `unknown key "slots"`},
		{"duplicate key", "[peer]\nrole = server\nrole = client\n", 3, "already set on line 2"},
		{"two peer sections", "[peer]\nrole = server\n\n[peer]\n", 4, "[peer] is already on line 1"},
		{"two provides of one service", "[provide \"s\"]\nupstream = 127.0.0.1:1\n\n[provide \"s\"]\nupstream = 127.0.0.1:2\n", 4, "already on line 1"},
		{"bad role", "[peer]\nrole = both\n", 2, "neither server nor client"},
		{"bad address", "[peer]\nlisten = 7420\n", 2, "not host:port"},
		{"named port", "[peer]\nlisten = 0.0.0.0:http\n", 2, "not a port number"},
		{"port out of range", "[peer]\nlisten = 0.0.0.0:70000\n", 2, "not a port number"},
		{"no host to dial", "[peer]\nserver = :7420\n", 2, "has no host"},
		{"short secret", "[peer]\nsecret = abcd\n", 2, "4 characters, want 64"},
		{"secret not hex", "[peer]\nsecret = " + strings.Repeat("z", 64) + "\n", 2, "not hexadecimal"},
		{"bad duration", "[peer]\nqueue_timeout = ten minutes\n", 2, "is not a duration"},
		{"zero duration", "[peer]\nqueue_timeout = 0s\n", 2, "not a wait"},
		{"zero slots", "[provide \"s\"]\nupstream = 127.0.0.1:1\nmax_concurrent = 0\n", 3, "want at least 1"},
		{"slots out of range", "[provide \"s\"]\nupstream = 127.0.0.1:1\nmax_concurrent = 70000\n", 3, "not a number from 1 to 65535"},
		{"consume without listen", "[consume \"s\"]\n", 1, "has no listen"},
		{"provide without upstream", "[provide \"s\"]\n", 1, "has no upstream"},
		{"role without secret", "[peer]\nrole = server\n", 1, "no secret"},
		{"client without server", "[peer]\nrole = client\nsecret = " + testSecret + "\n", 1, "no server to dial"},
		{"client that listens", "[peer]\nrole = client\nserver = a.example:1\nsecret = " + testSecret + "\nlisten = 0.0.0.0:7420\n", 5, "is a server key"},
		{"client with a queue timeout", "[peer]\nrole = client\nserver = a.example:1\nsecret = " + testSecret + "\nqueue_timeout = 5m\n", 5, "is a server key"},
		{"server that dials", "[peer]\nrole = server\nsecret = " + testSecret + "\nserver = a.example:1\n", 4, "is a client key"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parse(t, c.text)
			if err == nil {
				t.Fatalf("accepted the conf as %+v", got)
			}

			var refusal *Error
			if !errors.As(err, &refusal) {
				t.Fatalf("refused with %v, want a conf error with a line", err)
			}
			if refusal.Line != c.line {
				t.Errorf("refused on line %d, want line %d: %v", refusal.Line, c.line, err)
			}
			if !strings.Contains(refusal.Msg, c.want) {
				t.Errorf("refused with %q, want it to mention %q", refusal.Msg, c.want)
			}
			if !strings.HasPrefix(err.Error(), "httppooler.conf:") {
				t.Errorf("refusal does not name the file: %v", err)
			}
		})
	}
}

// TestCommentsAndBlanks is the rest of the grammar: # to the end of the line wherever it appears, and
// space that means nothing.
func TestCommentsAndBlanks(t *testing.T) {
	c := good(t, "# a whole line\n\n   \t \n"+
		"[peer]   # after a header\n"+
		"  role   =   server   # after a value\n"+
		"secret="+testSecret+"\n")

	if c.Role != RoleServer {
		t.Fatalf("role is %s, want server", c.Role)
	}
	if len(c.Secret) != SecretSize {
		t.Fatalf("secret is %d bytes", len(c.Secret))
	}
}

// TestNoRoleKeepsSections is a conf that is not finished yet: it parses whole, and the daemon idles on
// it instead of guessing which role its keys belong to.
func TestNoRoleKeepsSections(t *testing.T) {
	c := good(t, `
[peer]
listen = 0.0.0.0:7420
server = pool.example.com:7420
secret = `+testSecret+`

[provide "ollama"]
upstream = 127.0.0.1:11434
`)

	if c.Role != RoleNone {
		t.Fatalf("role is %s, want none", c.Role)
	}
	if len(c.Provide) != 1 {
		t.Fatalf("%d provide sections, want 1", len(c.Provide))
	}
}
