package dispatch

import "context"

// Queue is where pending task ids come from and where a dispatcher confirms
// it's done handling one. Redis Streams (internal/queue) is the real
// implementation; dispatcher_test.go uses an in-memory fake so these tests
// don't need a live Redis. The rest of the dispatcher — pool.go, executor.go,
// run() — doesn't know or care which one it's talking to.
type Queue interface {
	// Enqueue adds taskID to the queue. Called by Submit.
	Enqueue(ctx context.Context, taskID string) error

	// Dequeue waits for the next available task id. A timeout with nothing
	// available is reported as ok=false, err=nil (not an error) so the
	// dispatch loop can check for shutdown and simply call it again — this
	// is what keeps the loop from hanging forever on an empty queue while
	// still noticing Shutdown promptly.
	//
	// ackID is an opaque handle to pass to Ack; it is not necessarily equal
	// to taskID (Redis Streams uses a separate per-delivery entry id).
	Dequeue(ctx context.Context) (taskID, ackID string, ok bool, err error)

	// Ack confirms this delivery of a task is fully handled — regardless of
	// whether the task succeeded or failed, only that this consumer is done
	// with it. An un-acked entry is what makes crash recovery possible: it
	// stays claimable so another consumer can pick it back up.
	Ack(ctx context.Context, ackID string) error

	// Depth reports how many tasks are currently outstanding — enqueued but
	// not yet acked, i.e. queued-or-running. This is the backpressure signal
	// Submit checks before accepting new work.
	Depth(ctx context.Context) (int, error)
}
