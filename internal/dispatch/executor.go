package dispatch

import (
	"context"

	"github.com/roneettopiwala/relay/internal/task"
)

// Executor runs one task to completion and reports the result. The Docker
// implementation (Checkpoint 3) is the real one; the dispatcher only depends
// on this interface, so tests can swap in a fake with no container involved,
// and Phase 2's retry/backoff can wrap any Executor without the dispatcher
// changing at all.
type Executor interface {
	Execute(ctx context.Context, t task.Task) (task.Result, error)
}
