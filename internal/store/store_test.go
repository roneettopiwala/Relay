package store_test

import (
	"sync"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// TestConcurrentCreateUpdateGet is the Checkpoint 1 "break it" test: many
// goroutines create a task, then repeatedly Update and Get it. Run with -race.
//
// It asserts three things the store claims:
//   - IDs are unique under concurrent Create (no map collision).
//   - Mutating the value returned by Get does not touch the store (deep copy).
//   - Every Update lands: final Attempts == the number of updates issued.
func TestConcurrentCreateUpdateGet(t *testing.T) {
	s := store.New()

	const goroutines = 200
	const updatesPer = 20

	var wg sync.WaitGroup
	ids := make(chan string, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := s.Create(task.Spec{Image: "alpine", Cmd: []string{"true"}})
			ids <- tk.ID

			for j := 0; j < updatesPer; j++ {
				s.Update(tk.ID, func(t *task.Task) {
					t.Status = task.StatusRunning
					t.Attempts++
				})

				got, ok := s.Get(tk.ID)
				if !ok {
					t.Errorf("Get(%s): missing", tk.ID)
					return
				}
				// Vandalise the copy; the store must be unaffected.
				got.Status = task.StatusFailed
				got.Attempts = -1
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]bool, goroutines)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate id: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != goroutines {
		t.Fatalf("unique ids = %d, want %d", len(seen), goroutines)
	}

	for id := range seen {
		got, _ := s.Get(id)
		if got.Status != task.StatusRunning || got.Attempts != updatesPer {
			t.Errorf("task %s: status=%s attempts=%d, want running/%d",
				id, got.Status, got.Attempts, updatesPer)
		}
	}

	st := s.Stats()
	if st.Running != goroutines || st.Pending != 0 {
		t.Errorf("Stats = %+v, want Running=%d Pending=0", st, goroutines)
	}
}

// TestGetReturnsDeepCopy specifically covers the pointer fields: mutating
// *ExitCode / *StartedAt on a Get result must not reach the stored task.
func TestGetReturnsDeepCopy(t *testing.T) {
	s := store.New()
	tk := s.Create(task.Spec{Image: "alpine"})

	s.Update(tk.ID, func(t *task.Task) {
		now := time.Now()
		code := 0
		t.StartedAt = &now
		t.ExitCode = &code
		t.Status = task.StatusRunning
	})

	got, ok := s.Get(tk.ID)
	if !ok {
		t.Fatal("Get: missing")
	}
	*got.ExitCode = 99
	*got.StartedAt = time.Unix(0, 0)
	got.Status = task.StatusCompleted

	again, _ := s.Get(tk.ID)
	if *again.ExitCode != 0 {
		t.Errorf("stored ExitCode mutated through copy: %d", *again.ExitCode)
	}
	if again.StartedAt.Equal(time.Unix(0, 0)) {
		t.Errorf("stored StartedAt mutated through copy")
	}
	if again.Status != task.StatusRunning {
		t.Errorf("stored Status mutated through copy: %s", again.Status)
	}
}

func TestUpdateMissingID(t *testing.T) {
	s := store.New()
	if s.Update("does-not-exist", func(*task.Task) { t.Fatal("mutate ran") }) {
		t.Error("Update on missing id returned true")
	}
}

func TestWithDefaults(t *testing.T) {
	d := task.Defaults{Image: "alpine", CPULimit: 1.0, MemLimit: "256m", Timeout: 30 * time.Second}

	got := task.Spec{Cmd: []string{"echo", "hi"}}.WithDefaults(d)
	if got.Image != "alpine" || got.CPULimit != 1.0 || got.MemLimit != "256m" || got.Timeout != 30*time.Second {
		t.Errorf("defaults not applied: %+v", got)
	}

	// Explicit values must survive.
	got = task.Spec{Image: "busybox", CPULimit: 0.5, MemLimit: "64m", Timeout: time.Second}.WithDefaults(d)
	if got.Image != "busybox" || got.CPULimit != 0.5 || got.MemLimit != "64m" || got.Timeout != time.Second {
		t.Errorf("explicit values overwritten: %+v", got)
	}
}
