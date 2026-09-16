// Package metrics is a small, in-process rolling window of recent task
// outcomes — just enough to compute live P50/P99 latency without Relay
// needing to store a full time-series. The Phase 5 dashboard gets a real
// trend by polling this repeatedly and keeping its own client-side history;
// Relay only ever needs to answer "right now, what does recent latency
// look like."
package metrics

import (
	"sort"
	"sync"
	"time"
)

// windowSize is how many recent terminal task outcomes are kept. A fixed
// count (not a time window) keeps this simple — no pruning by timestamp,
// just overwrite the oldest entry — and "the last 200 tasks" is a
// reasonable, standard definition of "recent" for a live percentile.
const windowSize = 200

// Recorder tracks recent task durations in a fixed-size ring buffer. Safe
// for concurrent use. The zero value is not usable — use New.
type Recorder struct {
	mu     sync.Mutex
	buf    [windowSize]time.Duration
	filled bool // has the buffer wrapped at least once?
	next   int  // next write position
}

// New returns an empty Recorder.
func New() *Recorder {
	return &Recorder{}
}

// Record adds one task's duration to the rolling window, evicting the
// oldest entry once the window is full.
func (r *Recorder) Record(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.next] = d
	r.next++
	if r.next == windowSize {
		r.next = 0
		r.filled = true
	}
}

// Snapshot is a point-in-time read of the rolling window's percentiles.
type Snapshot struct {
	P50         time.Duration
	P99         time.Duration
	SampleCount int // how many entries are in the current window (<= windowSize)
}

// Snapshot computes P50/P99 over whatever's currently in the window, using
// the same nearest-rank method as cmd/loadtest's own percentile helper —
// simple and consistent across the codebase, not a metrics-system-grade
// implementation.
func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	n := windowSize
	if !r.filled {
		n = r.next
	}
	sorted := make([]time.Duration, n)
	copy(sorted, r.buf[:n])
	r.mu.Unlock()

	if n == 0 {
		return Snapshot{}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(p float64) time.Duration {
		i := int(p * float64(n-1))
		if i < 0 {
			i = 0
		}
		if i >= n {
			i = n - 1
		}
		return sorted[i]
	}
	return Snapshot{P50: at(0.50), P99: at(0.99), SampleCount: n}
}
