package queue

import (
	"context"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/task"
)

// newTestDLQ builds a RedisDeadLetterQueue on its own throwaway stream,
// skipping the test if Redis isn't reachable.
func newTestDLQ(t *testing.T) *RedisDeadLetterQueue {
	t.Helper()
	stream, _ := uniqueNames(t)
	rdb := newTestRDB(t, stream)
	return newRedisDeadLetterQueue(rdb, stream)
}

func TestDeadLetterQueue_RecordAndList(t *testing.T) {
	dlq := newTestDLQ(t)
	ctx := context.Background()

	entry := dispatch.DeadLetterEntry{
		TaskID:   "abc123",
		Spec:     task.Spec{Image: "alpine", Cmd: []string{"echo", "hi"}, CPULimit: 0.5, MemLimit: "128m", Timeout: 30 * time.Second},
		Error:    "exit code 1",
		Attempts: 1,
		FailedAt: time.Now().Truncate(time.Second), // JSON round-trips at second precision by default via time.Time's RFC3339Nano — truncate for a clean equality check
	}

	if err := dlq.Record(ctx, entry); err != nil {
		t.Fatalf("Record: %v", err)
	}

	got, err := dlq.List(ctx, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(got))
	}

	if got[0].TaskID != entry.TaskID {
		t.Errorf("TaskID = %q, want %q", got[0].TaskID, entry.TaskID)
	}
	if got[0].Error != entry.Error {
		t.Errorf("Error = %q, want %q", got[0].Error, entry.Error)
	}
	if got[0].Attempts != entry.Attempts {
		t.Errorf("Attempts = %d, want %d", got[0].Attempts, entry.Attempts)
	}
	if got[0].Spec.Image != entry.Spec.Image || len(got[0].Spec.Cmd) != len(entry.Spec.Cmd) {
		t.Errorf("Spec = %+v, want %+v (full Spec should round-trip through JSON)", got[0].Spec, entry.Spec)
	}
	if !got[0].FailedAt.Equal(entry.FailedAt) {
		t.Errorf("FailedAt = %v, want %v", got[0].FailedAt, entry.FailedAt)
	}
}

func TestDeadLetterQueue_ListMostRecentFirst(t *testing.T) {
	dlq := newTestDLQ(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		entry := dispatch.DeadLetterEntry{TaskID: string(rune('a' + i)), Attempts: 1, FailedAt: time.Now()}
		if err := dlq.Record(ctx, entry); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	got, err := dlq.List(ctx, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d entries, want 5", len(got))
	}
	// Most recent (last recorded, "e") first.
	want := []string{"e", "d", "c", "b", "a"}
	for i, id := range want {
		if got[i].TaskID != id {
			t.Errorf("entry %d TaskID = %q, want %q (List should be most-recent-first)", i, got[i].TaskID, id)
		}
	}
}

func TestDeadLetterQueue_ListRespectsLimit(t *testing.T) {
	dlq := newTestDLQ(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := dlq.Record(ctx, dispatch.DeadLetterEntry{TaskID: string(rune('a' + i)), FailedAt: time.Now()}); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	got, err := dlq.List(ctx, 2)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d entries, want 2 (limit)", len(got))
	}
}
