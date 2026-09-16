package dispatch_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
	"github.com/roneettopiwala/relay/internal/testutil"
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

// waitWorkersIdle polls until the pool reports want idle workers, or fails
// the test after timeout. Same reasoning as waitAckCount: the worker token
// is returned in run()'s deferred cleanup, strictly after the store write
// that waitTerminal observes, not atomically with it.
func waitWorkersIdle(t *testing.T, d *dispatch.Dispatcher, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if idle := d.WorkersIdle(); idle >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("WorkersIdle = %d after %s, want %d (a worker token leaked)", d.WorkersIdle(), timeout, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitAckCount polls until the queue's acked count reaches at least want, or
// fails the test after timeout. Acking happens in run()'s deferred cleanup,
// strictly after the store is updated to a terminal status (deliberately —
// see run()'s comments) — so a test must not assume the two are atomic, only
// that ack follows shortly after.
func waitAckCount(t *testing.T, q *testutil.FakeQueue, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if n := q.AckedCount(); n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acked count = %d after %s, want >= %d", q.AckedCount(), timeout, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
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
	d := dispatch.New(st, exec, testutil.NewFakeQueue(), nil, dispatch.Config{Workers: workers})
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
	// The store is marked terminal *before* run()'s deferred cleanup (which
	// returns the worker token) runs — so waitTerminal on the last task above
	// only proves the store write landed, not that its token is back in the
	// pool yet. Poll instead of reading WorkersIdle once, same reasoning as
	// waitAckCount.
	waitWorkersIdle(t, d, workers, time.Second)
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
	d := dispatch.New(st, exec, testutil.NewFakeQueue(), nil, dispatch.Config{Workers: 1})
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
	d := dispatch.New(st, exec, testutil.NewFakeQueue(), nil, dispatch.Config{Workers: 1})

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
	d := dispatch.New(st, exec, testutil.NewFakeQueue(), nil, dispatch.Config{Workers: 1})

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

// TestSubmit_EnqueueErrorPropagates checks that when the queue rejects an
// enqueue (e.g. Redis unreachable), Submit reports the error rather than
// silently leaving an orphaned "pending" task that will never run.
func TestSubmit_EnqueueErrorPropagates(t *testing.T) {
	exec := newFakeExecutor(10*time.Millisecond, 0)
	st := store.New()
	q := testutil.NewFakeQueue()
	q.SetEnqueueErr(errors.New("redis unreachable"))
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1})
	defer d.Shutdown(context.Background())

	_, err := d.Submit(task.Spec{Timeout: time.Second})
	if err == nil {
		t.Fatal("Submit succeeded despite Enqueue failing")
	}
}

// TestRun_AcksOnCompletion checks that a normal task run acks its delivery —
// without this, a completed task would eventually be reclaimed as "orphaned"
// and re-run for no reason.
func TestRun_AcksOnCompletion(t *testing.T) {
	exec := newFakeExecutor(10*time.Millisecond, 0)
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitTerminal(t, st, tk.ID, time.Second)
	waitAckCount(t, q, 1, time.Second)
}

// TestRun_SkipsRedeliveryOfTerminalTask simulates the queue handing out the
// same task a second time after it already finished (e.g. a real Ack that
// failed, followed by a reclaim). run() must not execute it again — it
// should just ack the redelivery and leave the task's result untouched.
func TestRun_SkipsRedeliveryOfTerminalTask(t *testing.T) {
	exec := newFakeExecutor(10*time.Millisecond, 0)
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	first := waitTerminal(t, st, tk.ID, time.Second)
	waitAckCount(t, q, 1, time.Second) // make sure the original delivery's ack has landed first

	q.Redeliver(tk.ID)
	waitAckCount(t, q, 2, time.Second) // the redelivery's ack landing proves run() finished with it

	second, _ := st.Get(tk.ID)
	if !second.FinishedAt.Equal(*first.FinishedAt) {
		t.Errorf("FinishedAt changed after redelivery: %s -> %s (task was re-executed)",
			first.FinishedAt, second.FinishedAt)
	}
}

// TestRun_DropsUnknownTaskID simulates a redelivery for a task id this
// process has no record of (e.g. after a restart, with Phase 2's queue-only
// persistence — see dispatcher.go's run()). It must not panic, and must ack
// the delivery so it doesn't loop as permanently "orphaned".
func TestRun_DropsUnknownTaskID(t *testing.T) {
	exec := newFakeExecutor(10*time.Millisecond, 0)
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1})
	defer d.Shutdown(context.Background())

	q.Redeliver("no-such-task-id")

	deadline := time.Now().Add(time.Second)
	for q.AckedCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("unknown task id was never acked")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// blockingExecutor never returns until release is closed — used to hold a
// task "in flight" (dequeued, running, not yet acked) for a controlled
// amount of time, so backpressure tests can pin the queue depth precisely
// instead of racing real timing.
type blockingExecutor struct{ release chan struct{} }

func (b *blockingExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	select {
	case <-b.release:
		return task.Result{ExitCode: 0}, nil
	case <-ctx.Done():
		return task.Result{}, ctx.Err()
	}
}

// TestSubmit_RejectsWhenOverloaded is the Checkpoint 2 break-it test: once
// Queue.Depth reaches maxQueueDepth, further submissions are rejected with
// ErrOverloaded instead of being accepted onto an ever-growing backlog.
func TestSubmit_RejectsWhenOverloaded(t *testing.T) {
	release := make(chan struct{})
	exec := &blockingExecutor{release: release}
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1, MaxQueueDepth: 2})
	defer func() {
		close(release) // let anything in flight finish so Shutdown can't hang
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		d.Shutdown(ctx)
	}()

	for i := 0; i < 2; i++ {
		if _, err := d.Submit(task.Spec{Timeout: time.Minute}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	if _, err := d.Submit(task.Spec{Timeout: time.Minute}); err != dispatch.ErrOverloaded {
		t.Errorf("Submit at capacity = %v, want ErrOverloaded", err)
	}
}

// TestSubmit_AllowsAgainAfterDrain checks backpressure lifts once outstanding
// work drops back below the threshold — it's dynamic load shedding, not a
// permanent lockout.
func TestSubmit_AllowsAgainAfterDrain(t *testing.T) {
	exec := newFakeExecutor(10*time.Millisecond, 0)
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1, MaxQueueDepth: 1})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit 1: %v", err)
	}

	if _, err := d.Submit(task.Spec{Timeout: time.Second}); err != dispatch.ErrOverloaded {
		t.Errorf("Submit while at capacity = %v, want ErrOverloaded", err)
	}

	waitTerminal(t, st, tk.ID, time.Second)
	waitAckCount(t, q, 1, time.Second)

	if _, err := d.Submit(task.Spec{Timeout: time.Second}); err != nil {
		t.Errorf("Submit after drain = %v, want nil (backpressure should have lifted)", err)
	}
}

// flakyExecutor returns failErr on its first failTimes calls, then succeeds
// — used to test that a retryable (executor-error) failure actually gets
// retried and can go on to succeed.
type flakyExecutor struct {
	failTimes int
	failErr   error

	mu    sync.Mutex
	calls int
}

func (f *flakyExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	if n <= f.failTimes {
		return task.Result{}, f.failErr
	}
	return task.Result{ExitCode: 0, Output: "ok"}, nil
}

func (f *flakyExecutor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// exitCodeExecutor always returns a fixed exit code with no error — used to
// test that a deterministic non-zero exit is treated as permanent, never
// retried.
type exitCodeExecutor struct {
	code int

	mu    sync.Mutex
	calls int
}

func (e *exitCodeExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	return task.Result{ExitCode: e.code}, nil
}

func (e *exitCodeExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// slowFirstExecutor blocks past ctx.Done() on its first call (so the
// dispatcher's own per-task timeout fires), then succeeds immediately on any
// later call — used to test that a timeout is retried.
type slowFirstExecutor struct {
	mu    sync.Mutex
	calls int
}

func (e *slowFirstExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	e.mu.Lock()
	e.calls++
	n := e.calls
	e.mu.Unlock()
	if n == 1 {
		<-ctx.Done()
		return task.Result{}, ctx.Err()
	}
	return task.Result{ExitCode: 0, Output: "ok"}, nil
}

// TestRetry_TransientErrorEventuallySucceeds is the Checkpoint 3 break-it
// test: an executor-level error is retried, and a task that fails twice then
// succeeds ends up Completed — not stuck Failed after the first failure.
func TestRetry_TransientErrorEventuallySucceeds(t *testing.T) {
	exec := &flakyExecutor{failTimes: 2, failErr: errors.New("transient docker error")}
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{
		Workers: 1, MaxRetries: 3, RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 50 * time.Millisecond,
	})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got := waitTerminal(t, st, tk.ID, 2*time.Second)
	if got.Status != task.StatusCompleted {
		t.Fatalf("status = %s, want completed (last error: %s)", got.Status, got.Error)
	}
	if got.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 (two retries before it succeeded)", got.Attempts)
	}
	if calls := exec.callCount(); calls != 3 {
		t.Errorf("executor called %d times, want 3 (original + 2 retries)", calls)
	}
}

// TestRetry_PermanentFailureNotRetried checks a deterministic non-zero exit
// is never retried, per the retryable-vs-permanent policy.
func TestRetry_PermanentFailureNotRetried(t *testing.T) {
	exec := &exitCodeExecutor{code: 1}
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{
		Workers: 1, MaxRetries: 3, RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 50 * time.Millisecond,
	})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got := waitTerminal(t, st, tk.ID, time.Second)
	if got.Status != task.StatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0 (a deterministic non-zero exit must not be retried)", got.Attempts)
	}

	time.Sleep(100 * time.Millisecond) // long enough a wrongly-scheduled retry would have fired
	if calls := exec.callCount(); calls != 1 {
		t.Errorf("executor called %d times, want exactly 1", calls)
	}
}

// TestRetry_TimeoutIsRetried checks a task that hits its own per-task timeout
// is treated as retryable, same as an executor error.
func TestRetry_TimeoutIsRetried(t *testing.T) {
	exec := &slowFirstExecutor{}
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{
		Workers: 1, MaxRetries: 2, RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 50 * time.Millisecond,
	})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got := waitTerminal(t, st, tk.ID, 2*time.Second)
	if got.Status != task.StatusCompleted {
		t.Fatalf("status = %s, want completed", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (one retry after the timeout)", got.Attempts)
	}
}

// TestRetry_ExhaustsAfterMaxAttempts checks a task that always fails
// retryably ends up permanently Failed once MaxRetries is reached, with the
// give-up reason recorded.
func TestRetry_ExhaustsAfterMaxAttempts(t *testing.T) {
	exec := &flakyExecutor{failTimes: 1000, failErr: errors.New("always fails")}
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{
		Workers: 1, MaxRetries: 2, RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 20 * time.Millisecond,
	})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got := waitTerminal(t, st, tk.ID, 2*time.Second)
	if got.Status != task.StatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 (MaxRetries)", got.Attempts)
	}
	if !strings.Contains(got.Error, "gave up after") {
		t.Errorf("Error = %q, want it to mention giving up after N attempts", got.Error)
	}
	if calls := exec.callCount(); calls != 3 { // original + 2 retries
		t.Errorf("executor called %d times, want 3", calls)
	}
}

// TestRetry_BackoffDelayActuallyWaits checks the retry isn't instant — there
// really is a backoff, not just an immediate re-run relabeled as a "retry".
func TestRetry_BackoffDelayActuallyWaits(t *testing.T) {
	exec := &flakyExecutor{failTimes: 1, failErr: errors.New("fail once")}
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{
		Workers: 1, MaxRetries: 1, RetryBaseDelay: 200 * time.Millisecond, RetryMaxDelay: time.Second,
	})
	defer d.Shutdown(context.Background())

	start := time.Now()
	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitTerminal(t, st, tk.ID, 2*time.Second)
	elapsed := time.Since(start)

	// backoffDelay applies up to ±20% jitter on top of the 200ms base, so the
	// real floor is 160ms — leave a margin below that so this only fails on
	// an actually-too-fast retry, not on jitter doing exactly what it's for.
	if elapsed < 140*time.Millisecond {
		t.Errorf("task finished after only %s, want at least ~160ms (200ms base minus jitter) before its retry", elapsed)
	}
}

// memDeadLetterQueue is an in-memory dispatch.DeadLetterQueue test double.
type memDeadLetterQueue struct {
	mu      sync.Mutex
	entries []dispatch.DeadLetterEntry
}

func (q *memDeadLetterQueue) Record(ctx context.Context, entry dispatch.DeadLetterEntry) error {
	q.mu.Lock()
	q.entries = append(q.entries, entry)
	q.mu.Unlock()
	return nil
}

func (q *memDeadLetterQueue) List(ctx context.Context, limit int) ([]dispatch.DeadLetterEntry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]dispatch.DeadLetterEntry(nil), q.entries...), nil
}

func (q *memDeadLetterQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

func (q *memDeadLetterQueue) last() dispatch.DeadLetterEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.entries[len(q.entries)-1]
}

// waitDLQCount polls until the dead-letter queue holds at least want entries.
// Recording happens after the store's terminal write, same ordering reason
// as waitAckCount/waitWorkersIdle.
func waitDLQCount(t *testing.T, dlq *memDeadLetterQueue, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if n := dlq.count(); n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead-letter count = %d after %s, want >= %d", dlq.count(), timeout, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDeadLetter_RecordedAfterRetriesExhausted is the Checkpoint 4 break-it
// test: a task that exhausts its retries must leave a durable record behind,
// not just a local StatusFailed the store might lose on restart.
func TestDeadLetter_RecordedAfterRetriesExhausted(t *testing.T) {
	exec := &flakyExecutor{failTimes: 1000, failErr: errors.New("always fails")}
	st := store.New()
	q := testutil.NewFakeQueue()
	dlq := &memDeadLetterQueue{}
	d := dispatch.New(st, exec, q, dlq, dispatch.Config{
		Workers: 1, MaxRetries: 2, RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 20 * time.Millisecond,
	})
	defer d.Shutdown(context.Background())

	spec := task.Spec{Image: "alpine", Cmd: []string{"x"}, Timeout: time.Second}
	tk, err := d.Submit(spec)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitTerminal(t, st, tk.ID, 2*time.Second)
	waitDLQCount(t, dlq, 1, time.Second)

	entry := dlq.last()
	if entry.TaskID != tk.ID {
		t.Errorf("entry.TaskID = %q, want %q", entry.TaskID, tk.ID)
	}
	if entry.Attempts != 3 { // original + 2 retries
		t.Errorf("entry.Attempts = %d, want 3", entry.Attempts)
	}
	if entry.Spec.Image != spec.Image {
		t.Errorf("entry.Spec.Image = %q, want %q (full Spec should be captured)", entry.Spec.Image, spec.Image)
	}
	if !strings.Contains(entry.Error, "gave up after") {
		t.Errorf("entry.Error = %q, want it to mention giving up", entry.Error)
	}
}

// TestDeadLetter_RecordedForPermanentFailure checks an immediate permanent
// failure (never retried at all) still gets dead-lettered, not just a
// retry-exhaustion.
func TestDeadLetter_RecordedForPermanentFailure(t *testing.T) {
	exec := &exitCodeExecutor{code: 1}
	st := store.New()
	q := testutil.NewFakeQueue()
	dlq := &memDeadLetterQueue{}
	d := dispatch.New(st, exec, q, dlq, dispatch.Config{Workers: 1})
	defer d.Shutdown(context.Background())

	tk, err := d.Submit(task.Spec{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitTerminal(t, st, tk.ID, time.Second)
	waitDLQCount(t, dlq, 1, time.Second)

	if entry := dlq.last(); entry.Attempts != 1 {
		t.Errorf("entry.Attempts = %d, want 1 (no retries happened)", entry.Attempts)
	}
	// metrics.Record happens right before recordDeadLetter in run() (see
	// dispatcher.go), so by the time waitDLQCount above observed the DLQ
	// entry, the latency sample is already in too.
	if n := d.MetricsSnapshot().SampleCount; n != 1 {
		t.Errorf("MetricsSnapshot().SampleCount = %d, want 1 (a permanent failure is still a latency sample)", n)
	}
}

// TestDeadLetter_NotRecordedOnShutdownCancel checks a task that fails only
// because Shutdown cancelled it does NOT get dead-lettered — that's noise on
// every restart, not a real failure of the task.
func TestDeadLetter_NotRecordedOnShutdownCancel(t *testing.T) {
	exec := &blockingExecutor{release: make(chan struct{})} // never released within this test
	st := store.New()
	q := testutil.NewFakeQueue()
	dlq := &memDeadLetterQueue{}
	d := dispatch.New(st, exec, q, dlq, dispatch.Config{Workers: 1})

	tk, err := d.Submit(task.Spec{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let it start running

	// The task never releases on its own, so Shutdown's grace period is
	// guaranteed to expire and force-cancel it — that's the DeadlineExceeded
	// this asserts, same as TestShutdownRespectsContextDeadline.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := d.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
	}

	got := waitTerminal(t, st, tk.ID, time.Second)
	if got.Status != task.StatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if n := dlq.count(); n != 0 {
		t.Errorf("dead-letter count = %d, want 0 (a shutdown-cancelled task shouldn't be dead-lettered)", n)
	}
	// Same exclusion applies to latency metrics: a duration cut short by
	// shutdown isn't a meaningful sample of "how long does this task take".
	if n := d.MetricsSnapshot().SampleCount; n != 0 {
		t.Errorf("metrics sample count = %d, want 0 (a shutdown-cancelled task shouldn't be recorded)", n)
	}
}

// TestMetricsSnapshot_RecordsCompletedTasks checks a successful task
// contributes a latency sample — this is the data behind the Phase 5
// dashboard's P50/P99 numbers. The failure-path equivalent is checked
// alongside TestDeadLetter_RecordedForPermanentFailure, which already has a
// permanently-failing executor set up.
func TestMetricsSnapshot_RecordsCompletedTasks(t *testing.T) {
	exec := newFakeExecutor(30*time.Millisecond, 0)
	st := store.New()
	q := testutil.NewFakeQueue()
	d := dispatch.New(st, exec, q, nil, dispatch.Config{Workers: 1})
	defer d.Shutdown(context.Background())

	for i := 0; i < 2; i++ {
		tk, err := d.Submit(task.Spec{Timeout: time.Second})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		waitTerminal(t, st, tk.ID, time.Second)
	}

	deadline := time.Now().Add(time.Second)
	for d.MetricsSnapshot().SampleCount < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("SampleCount = %d after 1s, want 2", d.MetricsSnapshot().SampleCount)
		}
		time.Sleep(5 * time.Millisecond)
	}

	s := d.MetricsSnapshot()
	if s.P50 < 20*time.Millisecond || s.P50 > 500*time.Millisecond {
		t.Errorf("P50 = %s, want roughly 30ms (the executor's delay)", s.P50)
	}
}
