package dispatch

import (
	"context"
	"time"

	"github.com/roneettopiwala/relay/internal/task"
)

// DeadLetterEntry is a durable record of a task that failed permanently —
// either its retries were exhausted, or it was never retryable to begin with
// (a deterministic non-zero exit). Unlike Queue, which only ever carries a
// task id (see the queue-only persistence note on the dispatch package), an
// entry here carries the full Spec: its whole purpose is inspection after
// the fact, quite possibly long after the process that ran it is gone.
type DeadLetterEntry struct {
	TaskID   string    `json:"task_id"`
	Spec     task.Spec `json:"spec"`
	Error    string    `json:"error"`
	Attempts int       `json:"attempts"`
	FailedAt time.Time `json:"failed_at"`
}

// DeadLetterQueue records tasks that failed permanently, for inspection.
// Redis Streams (internal/queue) is the real implementation, on its own
// stream separate from the main work queue.
type DeadLetterQueue interface {
	Record(ctx context.Context, entry DeadLetterEntry) error
	List(ctx context.Context, limit int) ([]DeadLetterEntry, error)
}
