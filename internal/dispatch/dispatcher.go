// Package dispatch is Relay's race-free task dispatcher: a worker pool built
// on the channel-as-semaphore pattern, plus the loop that hands pending tasks
// to free workers and runs them under a per-task timeout.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/roneettopiwala/relay/internal/metrics"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// ackTimeout bounds an Ack call, which always runs on its own fresh context
// rather than the per-task ctx — that one is frequently already expired
// (timeout, or Shutdown cancelling baseCtx) right when acking still needs to
// happen.
const ackTimeout = 5 * time.Second

// deadLetterTimeout bounds a DeadLetterQueue.Record call, same reasoning as
// ackTimeout: run on a fresh context, not the (likely expired) per-task one.
const deadLetterTimeout = 5 * time.Second

// ErrShuttingDown is returned by Submit once Shutdown has been called.
var ErrShuttingDown = errors.New("dispatch: shutting down")

// ErrOverloaded is returned by Submit when the queue's outstanding depth is
// at or above maxQueueDepth — this is Relay's backpressure: shed load with a
// clear signal instead of accepting an unbounded backlog.
var ErrOverloaded = errors.New("dispatch: overloaded, try again later")

// Config holds Dispatcher's tunables — everything beyond its core
// dependencies (store, executor, queue).
type Config struct {
	Workers int

	// MaxQueueDepth is the backpressure threshold (see ErrOverloaded). 0
	// disables backpressure — Phase 1's "always accept" behaviour.
	MaxQueueDepth int

	// MaxRetries caps how many times a retryable failure is retried (see
	// run()'s classification of retryable vs. permanent). 0 disables
	// retries — a retryable failure fails immediately, like Phase 1.
	MaxRetries int
	// RetryBaseDelay and RetryMaxDelay shape the exponential backoff between
	// attempts. If MaxRetries > 0 and either is left zero, New fills in a
	// sane default rather than retrying with no delay at all.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// Dispatcher assigns tasks to workers with no race conditions: two concurrent
// dispatches can never claim the same worker (see pool.go), and every worker
// token is returned exactly once per task, even if the executor errors or
// panics (see run).
type Dispatcher struct {
	store      *store.Store
	executor   Executor
	queue      Queue
	deadLetter DeadLetterQueue // may be nil: DLQ recording is then skipped entirely
	metrics    *metrics.Recorder
	pool       chan *Worker
	cfg        Config

	baseCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// loopDone is closed when loop() returns, i.e. once every task the loop
	// will ever dispatch has had wg.Add(1) called for it. sync.WaitGroup
	// requires that Add calls with a positive delta happen-before a Wait call
	// (racing them is a documented misuse, caught by -race) — waiting for
	// loopDone before calling wg.Wait() in Shutdown is what actually
	// guarantees that ordering, rather than hoping enough wall-clock time has
	// passed.
	loopDone chan struct{}

	// shutdownMu guards `closed`. Submit takes the read lock (many concurrent
	// submits proceed together); Shutdown takes the write lock to flip it —
	// the same RWMutex idiom as store.Store. loop() also reads `closed` (RLock)
	// once per iteration so it stops picking up new work promptly once
	// Shutdown begins, without needing a channel to close.
	shutdownMu sync.RWMutex
	closed     bool
}

// defaultRetryBaseDelay and defaultRetryMaxDelay are used when cfg.MaxRetries
// is set but the caller left the delay fields at zero.
const (
	defaultRetryBaseDelay = 500 * time.Millisecond
	defaultRetryMaxDelay  = 30 * time.Second
)

// New builds a Dispatcher and starts its dispatch loop. exec is what actually
// runs a task (a fake in tests, Docker in prod). q is where pending task ids
// come from (a fake in tests, Redis Streams in prod, via internal/queue). dlq
// records permanent failures for inspection (see DeadLetterQueue); pass nil
// to skip DLQ recording entirely.
func New(st *store.Store, exec Executor, q Queue, dlq DeadLetterQueue, cfg Config) *Dispatcher {
	if cfg.MaxRetries > 0 {
		if cfg.RetryBaseDelay <= 0 {
			cfg.RetryBaseDelay = defaultRetryBaseDelay
		}
		if cfg.RetryMaxDelay <= 0 {
			cfg.RetryMaxDelay = defaultRetryMaxDelay
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		store:      st,
		executor:   exec,
		queue:      q,
		deadLetter: dlq,
		// Unlike Executor/Queue/DeadLetterQueue, there's no "real vs fake"
		// split for this — it's pure in-memory bookkeeping with no I/O, so
		// Dispatcher just owns one rather than taking it as a constructor
		// dependency.
		metrics:  metrics.New(),
		pool:     newPool(cfg.Workers),
		cfg:      cfg,
		baseCtx:  ctx,
		cancel:   cancel,
		loopDone: make(chan struct{}),
	}
	go func() {
		d.loop()
		close(d.loopDone)
	}()
	return d
}

// Submit records spec as a new pending task and enqueues it for dispatch,
// returning immediately — it never blocks on a worker being free. It fails
// if the dispatcher is shutting down, if the queue is at capacity (see
// ErrOverloaded), or if the queue itself can't accept the enqueue (e.g.
// Redis unreachable).
func (d *Dispatcher) Submit(spec task.Spec) (task.Task, error) {
	d.shutdownMu.RLock()
	defer d.shutdownMu.RUnlock()
	if d.closed {
		return task.Task{}, ErrShuttingDown
	}

	if d.cfg.MaxQueueDepth > 0 {
		depth, err := d.queue.Depth(d.baseCtx)
		if err != nil {
			return task.Task{}, fmt.Errorf("dispatch: check queue depth: %w", err)
		}
		if depth >= d.cfg.MaxQueueDepth {
			// Soft limit: checking depth and enqueueing aren't one atomic
			// step, so under enough concurrent load a few submissions can
			// slip past this right around the threshold — acceptable for
			// backpressure (the point is shedding load under sustained
			// overload, not an exact admission count) and avoids the real
			// complexity a hard guarantee would need (a Lua script or a
			// WATCH-based transaction).
			return task.Task{}, ErrOverloaded
		}
	}

	t := d.store.Create(spec)
	if err := d.queue.Enqueue(d.baseCtx, t.ID); err != nil {
		// Don't leave a stray "pending" record that will never run — nobody
		// dequeues it, so nothing would ever move it out of that state.
		d.store.Update(t.ID, func(tk *task.Task) {
			now := time.Now()
			tk.Status = task.StatusFailed
			tk.FinishedAt = &now
			tk.Error = fmt.Sprintf("enqueue failed: %v", err)
		})
		return task.Task{}, fmt.Errorf("dispatch: enqueue: %w", err)
	}
	return t, nil
}

// WorkersTotal returns the size of the worker pool.
func (d *Dispatcher) WorkersTotal() int { return cap(d.pool) }

// WorkersIdle returns how many workers are currently free.
func (d *Dispatcher) WorkersIdle() int { return len(d.pool) }

// MetricsSnapshot returns current P50/P99 latency over recent terminal
// tasks (see internal/metrics) — the data behind the dashboard's latency
// numbers.
func (d *Dispatcher) MetricsSnapshot() metrics.Snapshot { return d.metrics.Snapshot() }

// loop is the single dispatch goroutine: pull the next pending task id from
// the queue, wait for a free worker (this is the backpressure point — it
// blocks when every worker is busy), then hand the task off to its own
// goroutine to run. It stops picking up new work as soon as Shutdown sets
// `closed` — Queue.Dequeue's own internal poll timeout (a couple of seconds
// for the Redis implementation) bounds how long that takes to notice.
func (d *Dispatcher) loop() {
	for {
		d.shutdownMu.RLock()
		closed := d.closed
		d.shutdownMu.RUnlock()
		if closed {
			return
		}

		taskID, ackID, ok, err := d.queue.Dequeue(d.baseCtx)
		if err != nil {
			if d.baseCtx.Err() != nil {
				return // Shutdown cancelled baseCtx; exit quietly
			}
			log.Printf("dispatch: dequeue error: %v", err)
			time.Sleep(time.Second) // avoid a hot loop against a down queue
			continue
		}
		if !ok {
			continue // just a poll timeout with nothing new; loop back and recheck closed
		}

		w := <-d.pool
		d.wg.Add(1)
		go d.run(taskID, ackID, w)
	}
}

// run executes one task on worker w. The worker token is returned to the pool
// exactly once, no matter how this function exits — success, a reported
// failure, or a panic from the executor — and the queue delivery is always
// acked, so a task is never redelivered just because this process finished
// handling it.
func (d *Dispatcher) run(id, ackID string, w *Worker) {
	defer d.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			reason := fmt.Sprintf("executor panic: %v", r)
			var spec task.Spec
			var attempts int
			d.store.Update(id, func(t *task.Task) {
				now := time.Now()
				t.FinishedAt = &now
				t.Status = task.StatusFailed
				t.Error = reason
				spec = t.Spec
				attempts = t.Attempts + 1
			})
			d.recordDeadLetter(id, spec, reason, attempts)
		}
		ackCtx, cancel := context.WithTimeout(context.Background(), ackTimeout)
		if err := d.queue.Ack(ackCtx, ackID); err != nil {
			// The delivery stays un-acked and will eventually be reclaimed
			// and retried by a live consumer (see internal/queue's
			// ReclaimOrphaned) — that's the queue's at-least-once guarantee
			// covering our own failure to ack, not silent data loss. It does
			// mean a task can rarely run twice; the Terminal() check below is
			// what protects against that in the common case.
			log.Printf("dispatch: ack failed for task %s: %v", id, err)
		}
		cancel()
		d.pool <- w // always return the token, even on panic or ack failure
	}()

	tk, ok := d.store.Get(id)
	if !ok {
		// No local record for this id. Phase 2 keeps task state in memory
		// only (queue-only persistence — see the dispatch package doc), so
		// this happens if this process restarted and then reclaimed an entry
		// from before the restart: we have no Spec to run, so there's
		// nothing to do but let the deferred Ack above drop it rather than
		// let it loop as "orphaned" forever. Logged because a task silently
		// vanishing, even when this is the documented/correct outcome, needs
		// to be visible to whoever's operating this — see it happen live by
		// killing -9 a running server mid-task and starting a fresh one.
		log.Printf("dispatch: dropping task %s: no local record (likely reclaimed after a process restart — queue-only persistence, see package doc)", id)
		return
	}
	if tk.Terminal() {
		// Redelivery of a task this process already finished — most likely a
		// prior Ack call failed even though the task completed. Don't run it
		// twice; just let the deferred Ack above confirm this delivery too.
		return
	}

	started := time.Now()
	d.store.Update(id, func(t *task.Task) {
		t.Status = task.StatusRunning
		t.StartedAt = &started
	})

	ctx := d.baseCtx
	if tk.Spec.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(d.baseCtx, tk.Spec.Timeout)
		defer cancel()
	}

	result, err := d.executor.Execute(ctx, tk)

	// Classify the outcome. Success falls straight through to a store update
	// and returns; every failure path decides retryable vs. permanent and
	// either schedules a retry or finalizes as StatusFailed below.
	var retryable, dueToShutdown bool
	var reason string
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		// The task's own timeout fired — plausibly a transient slow run,
		// worth retrying.
		retryable = true
		reason = fmt.Sprintf("timeout after %s", tk.Spec.Timeout)
	case errors.Is(ctx.Err(), context.Canceled):
		// baseCtx was cancelled — Shutdown, not the task's own timeout.
		// Retrying is pointless: this process is on its way down. Not
		// dead-lettered either — this isn't a real failure of the task, and
		// it would otherwise happen on every restart.
		retryable = false
		dueToShutdown = true
		reason = "cancelled by shutdown"
	case err != nil:
		// The executor itself failed (e.g. Docker daemon unreachable,
		// couldn't start the container) — an infrastructure problem, usually
		// transient, unlike a deterministic non-zero exit below.
		retryable = true
		reason = err.Error()
	case result.ExitCode != 0:
		// The command ran and deterministically exited non-zero. It will
		// fail the same way again, so retrying would just waste a worker
		// slot — this is exactly the "permanent" half of the retry policy.
		retryable = false
		reason = fmt.Sprintf("exit code %d", result.ExitCode)
	default:
		d.metrics.Record(time.Since(started))
		d.store.Update(id, func(t *task.Task) {
			now := time.Now()
			t.FinishedAt = &now
			t.Output = result.Output
			t.Status = task.StatusCompleted
			code := result.ExitCode
			t.ExitCode = &code
		})
		return
	}

	if retryable && tk.Attempts < d.cfg.MaxRetries {
		attempt := tk.Attempts + 1
		delay := backoffDelay(attempt, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay)
		d.store.Update(id, func(t *task.Task) {
			t.Status = task.StatusRetrying
			t.Attempts = attempt
			t.Error = reason
		})
		d.scheduleRetry(id, delay)
		return
	}

	finalReason := reason
	if tk.Attempts > 0 {
		finalReason = fmt.Sprintf("%s (gave up after %d attempts)", reason, tk.Attempts+1)
	}
	d.store.Update(id, func(t *task.Task) {
		now := time.Now()
		t.FinishedAt = &now
		t.Output = result.Output
		t.Status = task.StatusFailed
		t.Error = finalReason
		if result.ExitCode != 0 {
			code := result.ExitCode
			t.ExitCode = &code
		}
	})

	if !dueToShutdown {
		// Same exclusion as the dead-letter record just below: a
		// shutdown-cancelled duration isn't a meaningful latency sample —
		// it's an artifact of when the process happened to stop, not how
		// long the task actually took.
		d.metrics.Record(time.Since(started))
		d.recordDeadLetter(id, tk.Spec, finalReason, tk.Attempts+1)
	}
}

// recordDeadLetter writes a DeadLetterEntry for a permanently-failed task. A
// no-op if no DeadLetterQueue was configured; a failure to record is logged,
// not fatal — the task's local StatusFailed record (already written by the
// caller) is the primary source of truth, this is a secondary durable copy
// for inspection.
func (d *Dispatcher) recordDeadLetter(id string, spec task.Spec, reason string, attempts int) {
	if d.deadLetter == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadLetterTimeout)
	defer cancel()
	entry := DeadLetterEntry{
		TaskID:   id,
		Spec:     spec,
		Error:    reason,
		Attempts: attempts,
		FailedAt: time.Now(),
	}
	if err := d.deadLetter.Record(ctx, entry); err != nil {
		log.Printf("dispatch: dead-letter record failed for task %s: %v", id, err)
	}
}

// scheduleRetry waits delay, then re-enqueues id for another attempt onto the
// same queue — a retry is just another delivery of the same task id, so
// nothing downstream (pool.go, executor.go) needs to know retries exist. It
// counts toward wg (registered here, while run()'s own pending Done keeps the
// count above zero, satisfies the same Add-before-Wait ordering requirement
// documented on loopDone) so Shutdown waits for it — or for it to notice
// shutdown and bail — rather than the process exiting mid-backoff.
func (d *Dispatcher) scheduleRetry(id string, delay time.Duration) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		select {
		case <-time.After(delay):
		case <-d.baseCtx.Done():
			return // shutting down; don't bother retrying
		}

		d.shutdownMu.RLock()
		closed := d.closed
		d.shutdownMu.RUnlock()
		if closed {
			return
		}

		if err := d.queue.Enqueue(d.baseCtx, id); err != nil {
			log.Printf("dispatch: retry enqueue failed for task %s: %v", id, err)
		}
	}()
}

// backoffDelay returns an exponential delay for the given attempt (1-based):
// base, 2*base, 4*base, ..., capped at maxDelay. A ±20% jitter is mixed in so
// a burst of tasks that fail together don't all retry in lockstep and
// immediately re-collide (the classic "thundering herd" after an outage).
func backoffDelay(attempt int, base, maxDelay time.Duration) time.Duration {
	d := base << uint(attempt-1)
	if d <= 0 || d > maxDelay { // d<=0 also catches overflow from a large attempt
		d = maxDelay
	}
	if jitterRange := int64(d) / 5; jitterRange > 0 {
		d += time.Duration(rand.Int63n(2*jitterRange+1) - jitterRange)
	}
	if d < 0 {
		d = 0
	}
	return d
}

// Shutdown stops accepting new submissions and dequeues, and waits for
// in-flight tasks to finish, bounded by ctx. If ctx expires first, it cancels
// the base context (forcing every in-flight executor's ctx to end, e.g.
// killing containers) and returns ctx.Err(). Safe to call once; a second call
// is a no-op besides waiting.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.shutdownMu.Lock()
	d.closed = true
	d.shutdownMu.Unlock()

	done := make(chan struct{})
	go func() {
		<-d.loopDone // every wg.Add(1) the loop will ever issue has happened by now
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		d.cancel()
		return nil
	case <-ctx.Done():
		d.cancel()
		return ctx.Err()
	}
}
