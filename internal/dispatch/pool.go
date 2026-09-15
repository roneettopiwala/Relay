package dispatch

// Worker identifies one slot in the pool. It carries no state of its own —
// safety comes entirely from how the pool hands workers out (see newPool),
// not from anything stored on Worker.
type Worker struct {
	ID int
}

// newPool returns a buffered channel pre-filled with n workers. Its capacity
// IS the concurrency limit: pulling a worker out (<-pool) blocks automatically
// once all n are checked out — that block is the entire "wait for a free
// worker" behaviour, with no separate counter or condition variable to keep in
// sync. Returning a worker (pool <- w) can never double-hand the same worker
// to two callers, because a channel delivers each value to exactly one
// receiver — that's what makes double-assignment structurally impossible
// rather than merely "prevented by careful code."
func newPool(n int) chan *Worker {
	pool := make(chan *Worker, n)
	for i := 0; i < n; i++ {
		pool <- &Worker{ID: i}
	}
	return pool
}
