package peer

// A network in memory. Addresses are whatever string a conf writes, connections are pipes, and nothing
// the tests do opens a port or leaves the process.

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// memAddr is an address on the in-memory network.
type memAddr string

func (a memAddr) Network() string { return "mem" }
func (a memAddr) String() string  { return string(a) }

// memConn is a pipe that knows which addresses it joins.
type memConn struct {
	net.Conn
	local  memAddr
	remote memAddr
}

func (c *memConn) LocalAddr() net.Addr  { return c.local }
func (c *memConn) RemoteAddr() net.Addr { return c.remote }

// memNet is the whole network: every address something is listening on.
type memNet struct {
	mu        sync.Mutex
	listeners map[string]*memListener
}

func newMemNet() *memNet {
	return &memNet{listeners: map[string]*memListener{}}
}

// Listen takes an address on the network, which no one else may already hold.
func (n *memNet) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, taken := n.listeners[address]; taken {
		return nil, &net.OpError{Op: "listen", Net: network, Addr: memAddr(address), Err: errors.New("address in use")}
	}

	l := &memListener{net: n, addr: address, incoming: make(chan net.Conn, 16), closed: make(chan struct{})}
	n.listeners[address] = l
	return l, nil
}

// Dial connects to whatever is listening on an address, and is refused when nothing is.
func (n *memNet) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	n.mu.Lock()
	l := n.listeners[address]
	n.mu.Unlock()

	if l == nil {
		return nil, &net.OpError{Op: "dial", Net: network, Addr: memAddr(address),
			Err: errors.New("connection refused")}
	}

	here, there := net.Pipe()
	select {
	case l.incoming <- &memConn{Conn: there, local: memAddr(address), remote: memAddr("dialler")}:
		return &memConn{Conn: here, local: memAddr("dialler"), remote: memAddr(address)}, nil
	case <-l.closed:
		return nil, &net.OpError{Op: "dial", Net: network, Addr: memAddr(address),
			Err: errors.New("connection refused")}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// memListener is one address that accepts connections.
type memListener struct {
	net      *memNet
	addr     string
	incoming chan net.Conn
	closed   chan struct{}
	once     sync.Once
}

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		l.net.mu.Lock()
		delete(l.net.listeners, l.addr)
		l.net.mu.Unlock()
	})
	return nil
}

func (l *memListener) Addr() net.Addr { return memAddr(l.addr) }

// cable is a dialler that remembers what it opened, so a test can cut a peer's connection the way a
// network does: without telling either end.
type cable struct {
	net *memNet

	mu    sync.Mutex
	conns []net.Conn
}

func (c *cable) dial(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := c.net.Dial(ctx, network, address)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
	return conn, nil
}

// cut closes every connection this dialler opened.
func (c *cable) cut() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

// waitUntil spins until want is true, or fails the test as stuck.
func waitUntil(fail func(args ...any), want func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !want() {
		if time.Now().After(deadline) {
			fail("the peers never reached the state the test waited for")
			return
		}
		time.Sleep(time.Millisecond)
	}
}
