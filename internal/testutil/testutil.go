// Package testutil holds small test doubles for dispatch.Queue and
// dispatch.Executor shared across this repo's own test suites (dispatch,
// api, relayclient, mcpserver) — each of those needed the same "in-memory
// queue" and "controllable executor" shapes, so this is the one
// implementation instead of a fourth near-identical copy.
package testutil

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/api"
	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// SleepExecutor is a controllable no-Docker dispatch.Executor: it waits Delay
// then reports success, so a test can exercise dispatch/HTTP behaviour
// without a container.
type SleepExecutor struct{ Delay time.Duration }

func (s SleepExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	select {
	case <-time.After(s.Delay):
		return task.Result{ExitCode: 0, Output: "ok"}, nil
	case <-ctx.Done():
		return task.Result{}, ctx.Err()
	}
}

// FakeQueue is an in-memory dispatch.Queue — no Redis needed. Dequeue mimics
// a Redis-style blocking read (returns ok=false after a short poll interval
// with nothing available) so callers exercise the same "recheck for
// shutdown" polling shape the real Redis-backed queue has. ackID == taskID
// here since there's no separate delivery-id concept to model.
type FakeQueue struct {
	ch chan string

	mu         sync.Mutex
	acked      []string
	enqueueErr error
	depth      int // enqueued - acked, mirrors RedisQueue.Depth's XLEN semantics
}

func NewFakeQueue() *FakeQueue { return &FakeQueue{ch: make(chan string, 4096)} }

func (q *FakeQueue) Enqueue(ctx context.Context, id string) error {
	q.mu.Lock()
	err := q.enqueueErr
	q.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case q.ch <- id:
		q.mu.Lock()
		q.depth++
		q.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *FakeQueue) Dequeue(ctx context.Context) (string, string, bool, error) {
	select {
	case id, open := <-q.ch:
		if !open {
			return "", "", false, nil
		}
		return id, id, true, nil
	case <-time.After(30 * time.Millisecond):
		return "", "", false, nil
	case <-ctx.Done():
		return "", "", false, ctx.Err()
	}
}

func (q *FakeQueue) Ack(ctx context.Context, ackID string) error {
	q.mu.Lock()
	q.acked = append(q.acked, ackID)
	q.depth--
	q.mu.Unlock()
	return nil
}

func (q *FakeQueue) Depth(ctx context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depth, nil
}

// AckedCount returns how many deliveries have been acked so far.
func (q *FakeQueue) AckedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.acked)
}

// Redeliver injects id directly, bypassing Enqueue — simulating the queue
// handing the same task out a second time (e.g. a real redelivery after a
// crash-recovery reclaim), without needing a second real Submit.
func (q *FakeQueue) Redeliver(id string) { q.ch <- id }

// SetEnqueueErr makes future Enqueue calls fail with err (nil to clear it).
func (q *FakeQueue) SetEnqueueErr(err error) {
	q.mu.Lock()
	q.enqueueErr = err
	q.mu.Unlock()
}

// NewAPIServer spins up the real internal/api handler (not a mock) — backed
// by an in-memory store and FakeQueue, with the given executor — behind a
// real httptest.Server, using a plain 2-worker Config (no retries). For tests
// that want genuine HTTP round-trips against Relay's actual API without
// Docker or Redis. dlq may be nil.
func NewAPIServer(t *testing.T, exec dispatch.Executor, dlq dispatch.DeadLetterQueue, limits api.Limits) *httptest.Server {
	t.Helper()
	return NewAPIServerWithConfig(t, exec, dlq, limits, dispatch.Config{Workers: 2})
}

// NewAPIServerWithConfig is NewAPIServer with a caller-supplied
// dispatch.Config, for tests that need to exercise retry/backpressure policy
// (e.g. MaxRetries) rather than the plain default.
func NewAPIServerWithConfig(t *testing.T, exec dispatch.Executor, dlq dispatch.DeadLetterQueue, limits api.Limits, cfg dispatch.Config) *httptest.Server {
	t.Helper()
	st := store.New()
	d := dispatch.New(st, exec, NewFakeQueue(), dlq, cfg)
	t.Cleanup(func() { d.Shutdown(context.Background()) })
	defaults := task.Defaults{Image: "alpine", CPULimit: 0.5, MemLimit: "128m", Timeout: 30 * time.Second}
	a := api.New(st, d, dlq, defaults, limits)
	srv := httptest.NewServer(a.Routes())
	t.Cleanup(srv.Close)
	return srv
}

// DefaultTestLimits is a reasonable api.Limits for tests that don't care
// about the specific ceilings (e.g. anything not testing validation itself).
var DefaultTestLimits = api.Limits{MaxCPU: 2.0, MaxMemBytes: 512 * 1024 * 1024, MaxTimeout: 60 * time.Second}
