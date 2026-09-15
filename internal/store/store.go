// Package store is Relay's in-memory record of every task. Phase 1 keeps all
// task state in one map guarded by a single sync.RWMutex.
//
// Two invariants make it race-safe without callers needing to think about locks:
//
//  1. Get returns a deep copy (task.Task.Clone), never the stored pointer. A
//     reader can never observe a field mid-write or alias a value a writer will
//     mutate.
//  2. All mutation goes through Update, which holds the write lock for the whole
//     mutator callback. A status transition and its timestamps land atomically.
//
// Phase 2 replaces this with a durable store; the Store method set is the seam.
package store

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/roneettopiwala/relay/internal/task"
)

// Store holds every task Relay knows about, keyed by ID.
type Store struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
}

// New returns an empty Store.
func New() *Store {
	return &Store{tasks: make(map[string]*task.Task)}
}

// Create records a new pending task for spec and returns a copy of it. The ID
// is 128 bits of crypto/rand, checked for collision under the lock (the loop
// effectively never iterates).
func (s *Store) Create(spec task.Spec) task.Task {
	t := &task.Task{
		Spec:      spec,
		Status:    task.StatusPending,
		CreatedAt: time.Now(),
	}
	s.mu.Lock()
	for {
		t.ID = newID()
		if _, exists := s.tasks[t.ID]; !exists {
			break
		}
	}
	s.tasks[t.ID] = t
	s.mu.Unlock()
	return t.Clone()
}

// Get returns a deep copy of the task and whether it exists.
func (s *Store) Get(id string) (task.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return task.Task{}, false
	}
	return t.Clone(), true
}

// Update applies mutate to the stored task under the write lock and reports
// whether the id existed. mutate must not retain the *task.Task past its return.
func (s *Store) Update(id string, mutate func(*task.Task)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return false
	}
	mutate(t)
	return true
}

// Stats is a point-in-time count of tasks by status.
type Stats struct {
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// Stats counts tasks by status under the read lock.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var st Stats
	for _, t := range s.tasks {
		switch t.Status {
		case task.StatusPending:
			st.Pending++
		case task.StatusRunning:
			st.Running++
		case task.StatusCompleted:
			st.Completed++
		case task.StatusFailed:
			st.Failed++
		}
	}
	return st
}

// newID returns a random 32-hex-character task ID. crypto/rand.Read does not
// fail in practice on Linux; if it somehow does we cannot mint a safe ID, so
// panicking is the correct response.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("relay/store: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
