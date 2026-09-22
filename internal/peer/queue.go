package peer

// A job's messages on their way from the connection's read loop to the goroutine that runs the job.
//
// The queue is unbounded on purpose. The read loop must never block: two peers that relay to each other
// would otherwise be able to wedge, each one's reader waiting for a queue the other's writer is waiting
// to fill. Version 0 has no per-job window, so what a job cannot pass on at once is held here, and
// PROTOCOL.md says so, and flow control waits for a later version.

import (
	"context"
	"sync"

	"github.com/henu/httppooler/internal/wire"
)

// jobQueue is one job's inbox.
type jobQueue struct {
	mu    sync.Mutex
	items []wire.Message
	err   error
	ready chan struct{}
}

func newJobQueue() *jobQueue {
	return &jobQueue{ready: make(chan struct{})}
}

// push adds a message and wakes whoever is waiting. It never blocks.
func (q *jobQueue) push(m wire.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return
	}
	q.items = append(q.items, m)
	q.wake()
}

// close ends the queue with the reason. Messages already in it are still handed out, so a body that
// arrived whole before the connection dropped is not thrown away.
func (q *jobQueue) close(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return
	}
	q.err = err
	q.wake()
}

// next waits for the next message. It returns the close reason once the queue is empty and closed, and
// ctx.Err() when the caller gives up first.
func (q *jobQueue) next(ctx context.Context) (wire.Message, error) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			m := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return m, nil
		}
		if q.err != nil {
			err := q.err
			q.mu.Unlock()
			return nil, err
		}
		ready := q.ready
		q.mu.Unlock()

		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// wake releases everyone waiting; the lock is held. A closed channel is the broadcast, and the next one
// takes its place.
func (q *jobQueue) wake() {
	close(q.ready)
	q.ready = make(chan struct{})
}
