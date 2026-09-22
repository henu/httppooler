package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// provider makes one upstream in a service's pool.
func provider(peer, service string, slots, priority uint16) *Provider {
	return &Provider{Peer: peer, Service: service, Slots: slots, Priority: priority, Target: peer}
}

// acquire takes a slot now, or fails the test.
func acquire(t *testing.T, p *Pool, service string) *Lease {
	t.Helper()
	lease, err := p.Acquire(context.Background(), service, time.Now(), false)
	if err != nil {
		t.Fatalf("acquire %s: %v", service, err)
	}
	return lease
}

// TestSlotsAreCounted gives out every slot a provider has and no more.
func TestSlotsAreCounted(t *testing.T) {
	p := New(50 * time.Millisecond)
	p.Add(provider("kotikone", "ollama", 2, 100))

	first := acquire(t, p, "ollama")
	second := acquire(t, p, "ollama")

	if _, err := p.Acquire(context.Background(), "ollama", time.Now(), false); err == nil {
		t.Fatal("a third job got a slot from a provider with two")
	}

	// A released slot goes back into the pool.
	first.Release()
	third := acquire(t, p, "ollama")
	third.Release()
	second.Release()
}

// TestReleaseTwiceIsOnce keeps the accounting from drifting when a job ends more than once.
func TestReleaseTwiceIsOnce(t *testing.T) {
	p := New(50 * time.Millisecond)
	p.Add(provider("kotikone", "ollama", 1, 100))

	lease := acquire(t, p, "ollama")
	lease.Release()
	lease.Release()

	held := acquire(t, p, "ollama")
	if _, err := p.Acquire(context.Background(), "ollama", time.Now(), false); err == nil {
		t.Fatal("the one slot was handed out twice")
	}
	held.Release()
}

// TestPriorityWins sends work to the smallest priority number while it has room, and to the next one
// only when it has none.
func TestPriorityWins(t *testing.T) {
	p := New(time.Second)
	fast := provider("kotikone", "ollama", 2, 100)
	fallback := provider("varakone", "ollama", 2, 200)
	p.Add(fallback)
	p.Add(fast)

	var got []string
	for i := 0; i < 4; i++ {
		got = append(got, acquire(t, p, "ollama").Provider().Peer)
	}

	want := []string{"kotikone", "kotikone", "varakone", "varakone"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("job %d went to %s, want %s (all: %v)", i, got[i], want[i], got)
		}
	}
}

// TestSpreadsAcrossEqualPriority gives the next job to the provider with the most room left, so equal
// peers share the work instead of the first one taking it all.
func TestSpreadsAcrossEqualPriority(t *testing.T) {
	p := New(time.Second)
	one := provider("one", "ollama", 2, 100)
	two := provider("two", "ollama", 2, 100)
	p.Add(one)
	p.Add(two)

	acquire(t, p, "ollama")
	acquire(t, p, "ollama")

	if one.busy != 1 || two.busy != 1 {
		t.Fatalf("slots went %d and %d, want one each", one.busy, two.busy)
	}
}

// TestOldestFirst is the queue rule: jobs leave in the order they arrived.
func TestOldestFirst(t *testing.T) {
	p := New(2 * time.Second)
	p.Add(provider("kotikone", "ollama", 1, 100))

	held := acquire(t, p, "ollama")

	const jobs = 5
	served := make(chan int, jobs)
	for i := 0; i < jobs; i++ {
		go func(n int) {
			lease, err := p.Acquire(context.Background(), "ollama", time.Now(), false)
			if err != nil {
				t.Errorf("job %d: %v", n, err)
				return
			}
			served <- n
			lease.Release()
		}(i)

		// Each job is in the queue before the next one starts, so their order is the arrival order.
		waitFor(t, func() bool { return p.waiting("ollama") == i+1 })
	}

	held.Release()
	for i := 0; i < jobs; i++ {
		select {
		case n := <-served:
			if n != i {
				t.Fatalf("job %d was served in position %d", n, i)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a queued job was never served")
		}
	}
}

// TestRequeueGoesToTheFront is what a job does when the provider it was handed to disappears: it goes
// back to the head of the queue, ahead of jobs that arrived after it.
func TestRequeueGoesToTheFront(t *testing.T) {
	p := New(2 * time.Second)
	p.Add(provider("kotikone", "ollama", 1, 100))

	held := acquire(t, p, "ollama")

	served := make(chan string, 2)
	go func() {
		lease, err := p.Acquire(context.Background(), "ollama", time.Now(), false)
		if err != nil {
			t.Errorf("latecomer: %v", err)
			return
		}
		served <- "latecomer"
		lease.Release()
	}()
	waitFor(t, func() bool { return p.waiting("ollama") == 1 })

	go func() {
		lease, err := p.Acquire(context.Background(), "ollama", time.Now(), true)
		if err != nil {
			t.Errorf("requeued: %v", err)
			return
		}
		served <- "requeued"
		lease.Release()
	}()
	waitFor(t, func() bool { return p.waiting("ollama") == 2 })

	held.Release()
	if first := <-served; first != "requeued" {
		t.Fatalf("%s was served first, want the requeued job", first)
	}
}

// TestTimeoutCountsFromArrival is the deadline a requeued job keeps: it is measured from when the job
// first arrived, not from when it was queued again.
func TestTimeoutCountsFromArrival(t *testing.T) {
	p := New(200 * time.Millisecond)
	arrived := time.Now().Add(-150 * time.Millisecond)

	start := time.Now()
	_, err := p.Acquire(context.Background(), "ollama", arrived, true)
	waited := time.Since(start)

	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("waited and got %v, want a timeout", err)
	}
	if waited > 150*time.Millisecond {
		t.Fatalf("waited %v, want the rest of the 200ms the job already spent", waited)
	}
	if timeout.Service != "ollama" || timeout.Waited != 200*time.Millisecond {
		t.Fatalf("timeout says %+v", timeout)
	}
}

// TestNoProviderAtAll is a service nobody provides: the job waits like any other and is answered 503
// when its time is up.
func TestNoProviderAtAll(t *testing.T) {
	p := New(50 * time.Millisecond)

	var timeout *TimeoutError
	if _, err := p.Acquire(context.Background(), "nobody", time.Now(), false); !errors.As(err, &timeout) {
		t.Fatalf("acquire gave %v, want a timeout", err)
	}
}

// TestProviderArrivesLate serves a job that was already waiting when its provider connected.
func TestProviderArrivesLate(t *testing.T) {
	p := New(2 * time.Second)

	served := make(chan string, 1)
	go func() {
		lease, err := p.Acquire(context.Background(), "ollama", time.Now(), false)
		if err != nil {
			t.Errorf("acquire: %v", err)
			return
		}
		served <- lease.Provider().Peer
		lease.Release()
	}()
	waitFor(t, func() bool { return p.waiting("ollama") == 1 })

	p.Add(provider("kotikone", "ollama", 1, 100))
	select {
	case peer := <-served:
		if peer != "kotikone" {
			t.Fatalf("served by %s", peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the waiting job was not served when a provider connected")
	}
}

// TestCancelledJobLeaves is an application that goes away while its job is still queued.
func TestCancelledJobLeaves(t *testing.T) {
	p := New(2 * time.Second)
	p.Add(provider("kotikone", "ollama", 1, 100))
	held := acquire(t, p, "ollama")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := p.Acquire(ctx, "ollama", time.Now(), false)
		done <- err
	}()
	waitFor(t, func() bool { return p.waiting("ollama") == 1 })

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled job gave %v", err)
	}
	waitFor(t, func() bool { return p.waiting("ollama") == 0 })

	// The slot the cancelled job did not take is still there for the next one.
	held.Release()
	acquire(t, p, "ollama").Release()
}

// TestRemovedProviderTakesNoMoreWork is a connection that dropped: its provider leaves the pool, and
// what is left of the pool carries on.
func TestRemovedProviderTakesNoMoreWork(t *testing.T) {
	p := New(50 * time.Millisecond)
	gone := provider("kotikone", "ollama", 1, 100)
	p.Add(gone)

	lease := acquire(t, p, "ollama")
	p.Remove(gone)

	if _, err := p.Acquire(context.Background(), "ollama", time.Now(), false); err == nil {
		t.Fatal("a provider that had left took a job")
	}

	// The lease of the job it was running is released by whoever gave up on the connection.
	lease.Release()
	if _, err := p.Acquire(context.Background(), "ollama", time.Now(), false); err == nil {
		t.Fatal("a provider that had left took a job after its lease ended")
	}
}

// TestNeverOverSubscribed runs many jobs through a small pool at once and watches the slot count.
func TestNeverOverSubscribed(t *testing.T) {
	p := New(5 * time.Second)
	p.Add(provider("one", "ollama", 2, 100))
	p.Add(provider("two", "ollama", 3, 100))

	var running, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := p.Acquire(context.Background(), "ollama", time.Now(), false)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			now := running.Add(1)
			for {
				was := peak.Load()
				if now <= was || peak.CompareAndSwap(was, now) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
			lease.Release()
		}()
	}
	wg.Wait()

	if peak.Load() > 5 {
		t.Fatalf("%d jobs ran at once, want at most 5", peak.Load())
	}
	if p.waiting("ollama") != 0 {
		t.Fatalf("%d jobs left in the queue", p.waiting("ollama"))
	}
}

// TestSnapshot is what SIGUSR1 prints.
func TestSnapshot(t *testing.T) {
	p := New(time.Second)
	p.Add(provider("kotikone", "ollama", 2, 100))
	p.Add(provider("kotikone", "whisper", 1, 100))
	acquire(t, p, "ollama")

	snap := p.Snapshot()
	if len(snap.Services) != 2 || snap.Services[0].Name != "ollama" || snap.Services[1].Name != "whisper" {
		t.Fatalf("snapshot lists %+v", snap.Services)
	}
	if got := snap.Services[0].Providers[0]; got.Busy != 1 || got.Slots != 2 || got.Peer != "kotikone" {
		t.Fatalf("ollama's provider reads %+v", got)
	}
}

// waiting is how many jobs are queued for a service, for the tests only.
func (p *Pool) waiting(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.services[name]
	if s == nil {
		return 0
	}
	return len(s.waiters)
}

// waitFor spins until want is true or the test has waited long enough to call it stuck.
func waitFor(t *testing.T, want func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !want() {
		if time.Now().After(deadline) {
			t.Fatal("the pool never reached the state the test waited for")
		}
		time.Sleep(time.Millisecond)
	}
}
