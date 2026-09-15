package dispatch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// fakeExecutor is a test double: no Docker, just a controllable delay, so the
// pool's concurrency limit can be tested in isolation from container startup
// cost. panicAtCall, if non-zero, panics on that 1-based call number (call
// order across goroutines, not submission order) — chosen this way rather
// than "panic for task ID X" because the task ID isn't known until after
// Submit returns, which would force the test to write into the executor's
// state concurrently with tasks already running against it.
type fakeExecutor struct {
	delay       time.Duration
	panicAtCall int32

	mu      sync.Mutex
	calls   int
	running int
	peak    int
}

func newFakeExecutor(delay time.Duration, panicAtCall int32) *fakeExecutor {
	return &fakeExecutor{delay: delay, panicAtCall: panicAtCall}
}

func (f *fakeExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.running++
	if f.running > f.peak {
		f.peak = f.running
	}
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.running--
		f.mu.Unlock()
	}()

	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return task.Result{}, ctx.Err()
	}

	if f.panicAtCall != 0 && int32(n) == f.panicAtCall {
		panic("fakeExecutor: forced panic on call")
	}
	return task.Result{ExitCode: 0, Output: "ok"}, nil
}

func (f *fakeExecutor) Peak() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

// waitTerminal polls the store until id reaches a terminal status or the
// deadline passes, returning the final observed task.
func waitTerminal(t *testing.T, st *store.Store, id string, timeout time.Duration) task.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got, ok := st.Get(id)
		if !ok {
			t.Fatalf("task %s vanished from store", id)
		}
		if got.Terminal() {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s never reached a terminal state (status=%s)", id, got.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestPoolNeverExceedsWorkerCount is the Checkpoint 2 break-it test: with a
// 2-worker pool and 50 concurrent tasks — one of which panics mid-run — peak
// concurrent executions must never exceed 2, every task must reach a terminal
// state, and the pool must end up back at full strength (no leaked token).
// Run with -race.
func TestPoolNeverExceedsWorkerCount(t *testing.T) {
	const workers = 2
	const numTasks = 50
	const panicOnCall = 25

	exec := newFakeExecutor(100*time.Millisecond, panicOnCall)
	st := store.New()
	d := dispatch.New(st, workers, exec)
	defer d.Shutdown(context.Background())

	ids := make([]string, numTasks)
	for i := range ids {
		tk, err := d.Submit(task.Spec{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		ids[i] = tk.ID
	}

	failed := 0
	for _, id := range ids {
		if got := waitTerminal(t, st, id, 5*time.Second); got.Status == task.StatusFailed {
			failed++
		}
	}

	if peak := exec.Peak(); peak > workers {
		t.Errorf("peak concurrent executions = %d, want <= %d", peak, workers)
	}
	if idle := d.WorkersIdle(); idle != workers {
		t.Errorf("WorkersIdle after drain = %d, want %d (a worker token leaked)", idle, workers)
	}
	if failed < 1 {
		t.Errorf("expected the forced panic to fail at least one task, got %d failed", failed)
	}
}

// TestPerTaskTimeout checks the timeout fires at the boundary: an executor
// that takes 500ms with a 100ms task timeout must be cut off around 100ms,
// not immediately and not left to run to completion.
func TestPerTaskTimeout(t *testing.T) {
	exec := newFakeExecutor(500*time.Millisecond, 0)
	st := store.New()
	d := dispatch.New(st, 1, exec)
	defer d.Shutdown(context.Background())

	start := time.Now()
	tk, err := d.Submit(task.Spec{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got := waitTerminal(t, st, tk.ID, 2*time.Second)
	elapsed := time.Since(start)

	if got.Status != task.StatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if elapsed < 80*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Errorf("timeout fired at %s, want ~100ms (not before, not way after)", elapsed)
	}
}

// TestSubmitAfterShutdownRejected checks Submit refuses new work once
// Shutdown has been called, instead of panicking on a closed channel.
func TestSubmitAfterShutdownRejected(t *testing.T) {
	exec := newFakeExecutor(10*time.Millisecond, 0)
	st := store.New()
	d := dispatch.New(st, 1, exec)

	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if _, err := d.Submit(task.Spec{Timeout: time.Second}); err != dispatch.ErrShuttingDown {
		t.Errorf("Submit after Shutdown = %v, want ErrShuttingDown", err)
	}
}

// TestShutdownRespectsContextDeadline checks that when in-flight work outlives
// the ctx passed to Shutdown, Shutdown returns ctx.Err() rather than hanging,
// and that it forcibly cancels the in-flight executor's context.
func TestShutdownRespectsContextDeadline(t *testing.T) {
	exec := newFakeExecutor(2*time.Second, 0)
	st := store.New()
	d := dispatch.New(st, 1, exec)

	tk, err := d.Submit(task.Spec{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let it start running

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := d.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Errorf("Shutdown = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Shutdown took %s, want it to return promptly at the ctx deadline", elapsed)
	}

	got := waitTerminal(t, st, tk.ID, time.Second)
	if got.Status != task.StatusFailed {
		t.Errorf("in-flight task status = %s, want failed (cancelled by shutdown)", got.Status)
	}
}
