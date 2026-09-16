// Package task defines Relay's core data model: a Task, its lifecycle status,
// the immutable Spec describing what to run, and the Result an executor reports.
//
// Nothing here is concurrency-aware on its own. The rule the rest of the system
// relies on is that a Task is only ever mutated while its owning store holds a
// write lock, and readers get a deep Clone rather than the shared value.
package task

import "time"

// Status is a task's position in its lifecycle. A task that times out or
// whose container exits non-zero is StatusFailed with an explanatory Error,
// not a distinct "timeout"/"cancelled" status — the one exception is
// StatusRetrying (Phase 2), which exists specifically to make a scheduled
// retry observable, rather than have a backing-off task look identical to a
// fresh StatusPending one or a StatusFailed one it hasn't actually reached yet.
type Status string

const (
	StatusPending   Status = "pending"   // accepted, waiting for a free worker
	StatusRunning   Status = "running"   // handed to a worker, executor in progress
	StatusCompleted Status = "completed" // executor returned exit code 0
	StatusFailed    Status = "failed"    // permanently failed: non-zero exit, shutdown, or retries exhausted
	StatusRetrying  Status = "retrying"  // a retryable failure happened; waiting out a backoff before the next attempt
)

// Spec is the immutable description of what to run. It is supplied by the API
// caller, defaulted once via WithDefaults, and never mutated afterwards.
type Spec struct {
	Image    string        `json:"image"`     // container image, e.g. "alpine"
	Cmd      []string      `json:"cmd"`       // command + args to run in the container
	CPULimit float64       `json:"cpu_limit"` // docker --cpus (e.g. 0.5)
	MemLimit string        `json:"mem_limit"` // docker --memory (e.g. "256m")
	Timeout  time.Duration `json:"timeout"`   // hard wall-clock limit; exceeding it fails the task
}

// Defaults supplies fallback values for a Spec's unset fields. It is the single
// place Phase 1 decides what "unset" means; the API layer builds one from its
// flags and calls WithDefaults before the spec reaches the dispatcher.
type Defaults struct {
	Image    string
	CPULimit float64
	MemLimit string
	Timeout  time.Duration
}

// WithDefaults returns a copy of s with any zero-valued field filled from d.
func (s Spec) WithDefaults(d Defaults) Spec {
	if s.Image == "" {
		s.Image = d.Image
	}
	if s.CPULimit == 0 {
		s.CPULimit = d.CPULimit
	}
	if s.MemLimit == "" {
		s.MemLimit = d.MemLimit
	}
	if s.Timeout == 0 {
		s.Timeout = d.Timeout
	}
	return s
}

// Result is what an Executor reports after running a Spec.
type Result struct {
	ExitCode int
	Output   string
}

// Task is a single unit of work and everything Relay knows about its progress.
type Task struct {
	ID         string     `json:"id"`
	Spec       Spec       `json:"spec"`
	Status     Status     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	ExitCode   *int       `json:"exit_code,omitempty"`
	Output     string     `json:"output,omitempty"`
	Error      string     `json:"error,omitempty"`

	// Attempts is the number of retries issued so far (0 = still on the
	// first attempt, or it succeeded/permanently failed without retrying).
	Attempts int `json:"attempts"`
}

// Clone returns a deep copy. The pointer fields (StartedAt, FinishedAt,
// ExitCode) get fresh backing values, so a caller holding the copy can neither
// observe nor cause a mutation of the original. Spec.Cmd's backing array is
// shared, which is safe because a Spec is never mutated after task creation.
func (t Task) Clone() Task {
	c := t
	if t.StartedAt != nil {
		v := *t.StartedAt
		c.StartedAt = &v
	}
	if t.FinishedAt != nil {
		v := *t.FinishedAt
		c.FinishedAt = &v
	}
	if t.ExitCode != nil {
		v := *t.ExitCode
		c.ExitCode = &v
	}
	return c
}

// Terminal reports whether the task has reached an end state.
func (t Task) Terminal() bool {
	return t.Status == StatusCompleted || t.Status == StatusFailed
}
