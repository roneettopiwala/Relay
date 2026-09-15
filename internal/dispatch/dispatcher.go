// Package dispatch is Relay's race-free task dispatcher: a worker pool built
// on the channel-as-semaphore pattern, plus the loop that hands pending tasks
// to free workers and runs them under a per-task timeout.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// pendingCapacity bounds the in-process backlog of accepted-but-not-yet-running
// tasks. This is a deliberate Phase 1 stand-in: Submit never rejects (the load
// stays under this cap in Phase 1's load test), and Phase 2's Redis Streams
// replaces this channel as the real durable, effectively-unbounded queue —
// nothing else in this file needs to change when that happens.
const pendingCapacity = 4096

// ErrShuttingDown is returned by Submit once Shutdown has been called.
var ErrShuttingDown = errors.New("dispatch: shutting down")

// Dispatcher assigns tasks to workers with no race conditions: two concurrent
// dispatches can never claim the same worker (see pool.go), and every worker
// token is returned exactly once per task, even if the executor errors or
// panics (see run).
type Dispatcher struct {
	store     *store.Store
	executor  Executor
	pool      chan *Worker
	pendingCh chan string

	baseCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// loopDone is closed when loop() returns, i.e. once every pending id has
	// had wg.Add(1) called for it. sync.WaitGroup requires that Add calls with
	// a positive delta happen-before a Wait call (racing them is a documented
	// misuse, caught below by -race) — waiting for loopDone before calling
	// wg.Wait() in Shutdown is what actually guarantees that ordering, rather
	// than hoping enough wall-clock time has passed.
	loopDone chan struct{}

	// shutdownMu guards `closed` and the one-time close of pendingCh. Submit
	// takes the read lock (many concurrent submits proceed together); Shutdown
	// takes the write lock, so it is never possible for a Submit to be
	// mid-send on pendingCh while Shutdown closes it — the same RWMutex idiom
	// as store.Store, applied here to a boolean + a channel instead of a map.
	shutdownMu sync.RWMutex
	closed     bool
}

// New builds a Dispatcher with numWorkers workers and starts its dispatch
// loop. exec is what actually runs a task (a fake in tests, Docker in prod).
func New(st *store.Store, numWorkers int, exec Executor) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		store:     st,
		executor:  exec,
		pool:      newPool(numWorkers),
		pendingCh: make(chan string, pendingCapacity),
		baseCtx:   ctx,
		cancel:    cancel,
		loopDone:  make(chan struct{}),
	}
	go func() {
		d.loop()
		close(d.loopDone)
	}()
	return d
}

// Submit records spec as a new pending task and enqueues it for dispatch,
// returning immediately — it never blocks on a worker being free and never
// rejects for load reasons (see pendingCapacity). It only fails once the
// dispatcher is shutting down.
func (d *Dispatcher) Submit(spec task.Spec) (task.Task, error) {
	d.shutdownMu.RLock()
	defer d.shutdownMu.RUnlock()
	if d.closed {
		return task.Task{}, ErrShuttingDown
	}

	t := d.store.Create(spec)
	d.pendingCh <- t.ID
	return t, nil
}

// WorkersTotal returns the size of the worker pool.
func (d *Dispatcher) WorkersTotal() int { return cap(d.pool) }

// WorkersIdle returns how many workers are currently free.
func (d *Dispatcher) WorkersIdle() int { return len(d.pool) }

// loop is the single dispatch goroutine: pull the next pending task ID, wait
// for a free worker (this is the backpressure point — it blocks when every
// worker is busy), then hand the task off to its own goroutine to run.
func (d *Dispatcher) loop() {
	for id := range d.pendingCh {
		w := <-d.pool
		d.wg.Add(1)
		go d.run(id, w)
	}
}

// run executes one task on worker w. The worker token is returned to the pool
// exactly once, no matter how this function exits — success, a reported
// failure, or a panic from the executor.
func (d *Dispatcher) run(id string, w *Worker) {
	defer d.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			d.store.Update(id, func(t *task.Task) {
				now := time.Now()
				t.FinishedAt = &now
				t.Status = task.StatusFailed
				t.Error = fmt.Sprintf("executor panic: %v", r)
			})
		}
		d.pool <- w // always return the token, even on panic
	}()

	tk, ok := d.store.Get(id)
	if !ok {
		return // shouldn't happen: store.Create always precedes enqueue
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

	d.store.Update(id, func(t *task.Task) {
		now := time.Now()
		t.FinishedAt = &now
		t.Output = result.Output

		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			// The timeout fired, not a graceful shutdown cancel (that would be
			// context.Canceled, caught by the err != nil case below).
			t.Status = task.StatusFailed
			t.Error = fmt.Sprintf("timeout after %s", tk.Spec.Timeout)
		case err != nil:
			t.Status = task.StatusFailed
			t.Error = err.Error()
		default:
			code := result.ExitCode
			t.ExitCode = &code
			if code == 0 {
				t.Status = task.StatusCompleted
			} else {
				t.Status = task.StatusFailed
				t.Error = fmt.Sprintf("exit code %d", code)
			}
		}
	})
}

// Shutdown stops accepting new submissions and waits for in-flight tasks to
// finish, bounded by ctx. If ctx expires first, it cancels the base context
// (forcing every in-flight executor's ctx to end, e.g. killing containers)
// and returns ctx.Err(). Safe to call once; a second call is a no-op besides
// waiting.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.shutdownMu.Lock()
	if !d.closed {
		d.closed = true
		close(d.pendingCh)
	}
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
