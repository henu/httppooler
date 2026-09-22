package peer

// The two things every side of a job does with its queue: read the body that opens it, and keep
// listening for a CANCEL while the job runs.

import (
	"context"
	"errors"
	"fmt"

	"github.com/henu/httppooler/internal/wire"
)

// errCancelled is a job whose application went away. It is a reason, not a failure.
var errCancelled = errors.New("cancelled")

// collect reads a job's body: BODY messages until END, which is how a request body arrives whole so
// that the server can hand the job to another provider if the first one vanishes.
func collect(ctx context.Context, in *jobQueue) ([]byte, []wire.Header, error) {
	var body []byte
	for {
		m, err := in.next(ctx)
		if err != nil {
			return nil, nil, err
		}

		switch m := m.(type) {
		case *wire.Body:
			body = append(body, m.Bytes...)
		case *wire.End:
			return body, m.Trailers, nil
		case *wire.Cancel:
			return nil, nil, errCancelled
		default:
			return nil, nil, fmt.Errorf("peer: %s while a request body was still arriving", m.Kind())
		}
	}
}

// watchCancel keeps a job's queue read while the job runs, so a CANCEL arriving mid-flight ends it.
// Everything else on a provider side's queue after END is a peer out of step, and ends the job too.
func watchCancel(ctx context.Context, in *jobQueue, cancel context.CancelCauseFunc) {
	for {
		m, err := in.next(ctx)
		if err != nil {
			cancel(err)
			return
		}
		if _, ok := m.(*wire.Cancel); ok {
			cancel(errCancelled)
			return
		}
	}
}

// reason is what a job's FAIL says, which is the error unless the job was simply cancelled.
func reason(ctx context.Context, err error) string {
	if errors.Is(err, errCancelled) {
		return errCancelled.Error()
	}
	if cause := context.Cause(ctx); cause != nil && errors.Is(cause, errCancelled) {
		return errCancelled.Error()
	}
	return err.Error()
}
