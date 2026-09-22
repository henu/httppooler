// Package pool is the server's queues and slot accounting: one queue per service, oldest request first,
// and among the providers with a free slot the smallest priority number wins.
//
// The pool knows nothing about connections or HTTP. A provider carries a Target the peer layer put
// there and the pool never looks inside; it only counts slots and hands them out in order.
package pool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Pool is every service's queue and every provider's slots.
type Pool struct {
	timeout time.Duration

	mu       sync.Mutex
	services map[string]*service
}

// New makes a pool whose queues give up after timeout, counted from a job's first arrival.
func New(timeout time.Duration) *Pool {
	return &Pool{timeout: timeout, services: map[string]*service{}}
}

// Provider is one upstream in one service's pool. Everything but Target is what its peer announced in
// HELLO; Target is whatever the caller needs to reach it again.
type Provider struct {
	Peer     string
	Service  string
	Slots    uint16
	Priority uint16
	Target   any

	// Guarded by the pool's lock.
	busy int
	gone bool
}

// service is one pool: the providers of a service and the jobs waiting for one.
type service struct {
	pool      *Pool
	name      string
	providers []*Provider
	waiters   []*waiter
}

// waiter is one job in a queue. The channel is buffered, so handing over a lease never blocks the
// goroutine that freed the slot.
type waiter struct {
	lease chan *Lease
}

// Lease is one slot of one provider, held from the moment the job is dispatched until it ends.
type Lease struct {
	service  *service
	provider *Provider
	released bool
}

// Provider is the upstream this lease dispatches to.
func (l *Lease) Provider() *Provider {
	return l.provider
}

// Release gives the slot back and hands it to the oldest waiting job, if there is one. Releasing twice
// does nothing the second time.
func (l *Lease) Release() {
	l.service.pool.mu.Lock()
	defer l.service.pool.mu.Unlock()

	if l.released {
		return
	}
	l.released = true
	l.provider.busy--
	l.service.dispatch()
}

// TimeoutError is a job that waited its whole queue timeout without a provider freeing a slot. Its text
// is what the server puts in the 503 body.
type TimeoutError struct {
	Service string
	Waited  time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("no provider of %q had a free slot within %v", e.Service, e.Waited)
}

// Add puts a provider in its service's pool and gives it to a waiting job at once if there is one.
func (p *Pool) Add(pr *Provider) {
	p.mu.Lock()
	defer p.mu.Unlock()

	s := p.service(pr.Service)
	s.providers = append(s.providers, pr)
	s.dispatch()
}

// Remove takes a provider out, which is what a lost connection means. Jobs already dispatched to it
// keep their leases until whoever holds them gives up on the connection and releases them.
func (p *Pool) Remove(pr *Provider) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pr.gone = true
	s := p.services[pr.Service]
	if s == nil {
		return
	}
	for i, other := range s.providers {
		if other == pr {
			s.providers = append(s.providers[:i], s.providers[i+1:]...)
			break
		}
	}
}

// Acquire waits for a free slot of service and returns the lease on it. since is when the job first
// arrived, not when this attempt started: a job requeued because a provider vanished keeps its original
// deadline. front puts the job at the head of the queue, which is where a requeued job belongs.
//
// It returns *TimeoutError when the queue timeout passes, and ctx.Err() when the job goes away first.
func (p *Pool) Acquire(ctx context.Context, name string, since time.Time, front bool) (*Lease, error) {
	p.mu.Lock()
	s := p.service(name)

	// A free provider with nobody ahead in the queue is taken without waiting at all.
	if len(s.waiters) == 0 {
		if pr := s.best(); pr != nil {
			lease := s.take(pr)
			p.mu.Unlock()
			return lease, nil
		}
	}

	w := &waiter{lease: make(chan *Lease, 1)}
	if front {
		s.waiters = append([]*waiter{w}, s.waiters...)
	} else {
		s.waiters = append(s.waiters, w)
	}
	p.mu.Unlock()

	timer := time.NewTimer(time.Until(since.Add(p.timeout)))
	defer timer.Stop()

	select {
	case lease := <-w.lease:
		return lease, nil
	case <-timer.C:
		return nil, p.abandon(s, w, &TimeoutError{Service: name, Waited: p.timeout})
	case <-ctx.Done():
		return nil, p.abandon(s, w, ctx.Err())
	}
}

// abandon takes a waiter out of its queue. A slot handed over at the same moment is given straight back,
// so it goes to the next job rather than to one that is no longer there.
func (p *Pool) abandon(s *service, w *waiter, err error) error {
	p.mu.Lock()
	for i, other := range s.waiters {
		if other == w {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			p.mu.Unlock()
			return err
		}
	}
	p.mu.Unlock()

	select {
	case lease := <-w.lease:
		lease.Release()
	default:
	}
	return err
}

// service finds a service's pool, making it the first time anyone asks. A job for a service nobody
// provides yet waits like any other: a provider may still connect before the timeout.
func (p *Pool) service(name string) *service {
	s := p.services[name]
	if s == nil {
		s = &service{pool: p, name: name}
		p.services[name] = s
	}
	return s
}

// dispatch hands free slots to waiting jobs, oldest first, until one of the two runs out.
func (s *service) dispatch() {
	for len(s.waiters) > 0 {
		pr := s.best()
		if pr == nil {
			return
		}
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		w.lease <- s.take(pr)
	}
}

// best is the free provider a job should go to: the smallest priority number, and among equals the one
// with the most room left, so work spreads instead of piling on the first.
func (s *service) best() *Provider {
	var best *Provider
	for _, pr := range s.providers {
		if pr.gone || pr.busy >= int(pr.Slots) {
			continue
		}
		if best == nil || pr.Priority < best.Priority ||
			(pr.Priority == best.Priority && pr.free() > best.free()) {
			best = pr
		}
	}
	return best
}

// take marks a slot busy and makes the lease that gives it back.
func (s *service) take(pr *Provider) *Lease {
	pr.busy++
	return &Lease{service: s, provider: pr}
}

// free is how many of a provider's slots are still open.
func (pr *Provider) free() int {
	return int(pr.Slots) - pr.busy
}

// Snapshot is what the pool looks like at one moment, for the log SIGUSR1 writes.
type Snapshot struct {
	Services []ServiceState
}

// ServiceState is one service's providers and the jobs waiting for one.
type ServiceState struct {
	Name      string
	Waiting   int
	Providers []ProviderState
}

// ProviderState is one upstream's slots, as many as are in use.
type ProviderState struct {
	Peer     string
	Busy     int
	Slots    uint16
	Priority uint16
}

// Snapshot reads the whole pool under its lock, in a stable order.
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	names := make([]string, 0, len(p.services))
	for name := range p.services {
		names = append(names, name)
	}
	sort.Strings(names)

	snap := Snapshot{Services: make([]ServiceState, 0, len(names))}
	for _, name := range names {
		s := p.services[name]
		state := ServiceState{Name: name, Waiting: len(s.waiters)}
		for _, pr := range s.providers {
			state.Providers = append(state.Providers, ProviderState{
				Peer:     pr.Peer,
				Busy:     pr.busy,
				Slots:    pr.Slots,
				Priority: pr.Priority,
			})
		}
		snap.Services = append(snap.Services, state)
	}
	return snap
}
