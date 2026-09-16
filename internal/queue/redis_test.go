package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisTestAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

// uniqueNames returns a random stream/group pair so each test gets its own
// throwaway slice of the real Redis instance, isolated from other tests and
// from a real relay deployment sharing the same Redis.
func uniqueNames(t *testing.T) (stream, group string) {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	suffix := hex.EncodeToString(b[:])
	return "relay:test:" + suffix, "relay-test-group:" + suffix
}

// newTestRDB returns a live client, skipping the test (not failing it) if
// Redis isn't reachable — this file degrades gracefully without Redis
// running, the same way internal/executor's tests degrade without Docker.
func newTestRDB(t *testing.T, cleanupKeys ...string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisTestAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not reachable at " + redisTestAddr() + ": " + err.Error())
	}
	t.Cleanup(func() {
		if len(cleanupKeys) > 0 {
			rdb.Del(context.Background(), cleanupKeys...)
		}
		rdb.Close()
	})
	return rdb
}

// newTestQueue builds one RedisQueue on its own throwaway stream.
func newTestQueue(t *testing.T, blockFor, reclaimInterval, reclaimIdle time.Duration) (*RedisQueue, *redis.Client) {
	t.Helper()
	stream, group := uniqueNames(t)
	rdb := newTestRDB(t, stream)
	q, err := newRedisQueue(rdb, stream, group, blockFor, reclaimInterval, reclaimIdle)
	if err != nil {
		t.Fatalf("newRedisQueue: %v", err)
	}
	return q, rdb
}

func TestEnqueueDequeueAck(t *testing.T) {
	// reclaimInterval/Idle set to an hour: irrelevant to this test, just
	// needs to be long enough that the reclaim check never fires.
	q, rdb := newTestQueue(t, 200*time.Millisecond, time.Hour, time.Hour)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "task-1"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	id, ackID, ok, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if !ok {
		t.Fatal("Dequeue: ok = false, want true")
	}
	if id != "task-1" {
		t.Errorf("id = %q, want task-1", id)
	}

	if err := q.Ack(ctx, ackID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	n, err := rdb.XLen(ctx, q.stream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if n != 0 {
		t.Errorf("stream length after ack = %d, want 0 (Ack should XDEL, not just XACK)", n)
	}
}

func TestDequeueTimeoutReturnsOkFalse(t *testing.T) {
	q, _ := newTestQueue(t, 100*time.Millisecond, time.Hour, time.Hour)

	start := time.Now()
	id, ackID, ok, err := q.Dequeue(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if ok {
		t.Fatalf("ok = true on an empty queue (id=%q ackID=%q)", id, ackID)
	}
	if elapsed < 80*time.Millisecond || elapsed > 800*time.Millisecond {
		t.Errorf("Dequeue took %s, want ~100ms (the configured blockFor)", elapsed)
	}
}

// TestMultipleConsumersNoDoubleDelivery is this checkpoint's core safety
// claim: two independent consumers sharing one stream+group never both get
// the same entry, and between them every enqueued task is delivered exactly
// once.
func TestMultipleConsumersNoDoubleDelivery(t *testing.T) {
	stream, group := uniqueNames(t)
	rdb := newTestRDB(t, stream)
	ctx := context.Background()

	q1, err := newRedisQueue(rdb, stream, group, 100*time.Millisecond, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("newRedisQueue q1: %v", err)
	}
	q2, err := newRedisQueue(rdb, stream, group, 100*time.Millisecond, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("newRedisQueue q2: %v", err)
	}

	const n = 100
	for i := 0; i < n; i++ {
		if err := q1.Enqueue(ctx, fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	var mu sync.Mutex
	seen := make(map[string]bool, n)
	deadline := time.Now().Add(10 * time.Second)

	consume := func(q *RedisQueue, wg *sync.WaitGroup) {
		defer wg.Done()
		for time.Now().Before(deadline) {
			mu.Lock()
			done := len(seen) >= n
			mu.Unlock()
			if done {
				return
			}

			id, ackID, ok, err := q.Dequeue(ctx)
			if err != nil {
				t.Errorf("Dequeue: %v", err)
				return
			}
			if !ok {
				continue
			}
			mu.Lock()
			if seen[id] {
				t.Errorf("task %s delivered to more than one consumer", id)
			}
			seen[id] = true
			mu.Unlock()
			if err := q.Ack(ctx, ackID); err != nil {
				t.Errorf("Ack: %v", err)
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go consume(q1, &wg)
	go consume(q2, &wg)
	wg.Wait()

	if len(seen) != n {
		t.Errorf("delivered %d/%d unique tasks", len(seen), n)
	}
}

// TestReclaimOrphaned is the crash-recovery test: consumer A dequeues an
// entry and never acks it (simulating a crash mid-task). Consumer B, reading
// from the same stream+group, must eventually reclaim and receive that same
// entry once it's been idle long enough to be presumed orphaned.
func TestReclaimOrphaned(t *testing.T) {
	stream, group := uniqueNames(t)
	rdb := newTestRDB(t, stream)
	ctx := context.Background()

	const orphanIdle = 150 * time.Millisecond

	qA, err := newRedisQueue(rdb, stream, group, 100*time.Millisecond, time.Hour, orphanIdle)
	if err != nil {
		t.Fatalf("newRedisQueue qA: %v", err)
	}
	if err := qA.Enqueue(ctx, "orphan-task"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	id, _, ok, err := qA.Dequeue(ctx)
	if err != nil || !ok || id != "orphan-task" {
		t.Fatalf("qA.Dequeue: id=%q ok=%v err=%v", id, ok, err)
	}
	// No Ack call here — this is the simulated crash: qA claimed the entry
	// and then went silent.

	qB, err := newRedisQueue(rdb, stream, group, 100*time.Millisecond, 10*time.Millisecond, orphanIdle)
	if err != nil {
		t.Fatalf("newRedisQueue qB: %v", err)
	}

	time.Sleep(orphanIdle + 100*time.Millisecond) // let the entry actually become idle enough

	deadline := time.Now().Add(3 * time.Second)
	var reclaimedID, reclaimedAck string
	for time.Now().Before(deadline) {
		gotID, gotAck, ok, err := qB.Dequeue(ctx)
		if err != nil {
			t.Fatalf("qB.Dequeue: %v", err)
		}
		if ok {
			reclaimedID, reclaimedAck = gotID, gotAck
			break
		}
	}

	if reclaimedID != "orphan-task" {
		t.Fatalf("qB never reclaimed the orphaned entry (got id=%q)", reclaimedID)
	}
	if err := qB.Ack(ctx, reclaimedAck); err != nil {
		t.Fatalf("qB.Ack: %v", err)
	}
}

func TestQueueDoesNotGrowForever(t *testing.T) {
	q, rdb := newTestQueue(t, 200*time.Millisecond, time.Hour, time.Hour)
	ctx := context.Background()

	const n = 20
	for i := 0; i < n; i++ {
		if err := q.Enqueue(ctx, fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
		_, ackID, ok, err := q.Dequeue(ctx)
		if err != nil || !ok {
			t.Fatalf("Dequeue %d: ok=%v err=%v", i, ok, err)
		}
		if err := q.Ack(ctx, ackID); err != nil {
			t.Fatalf("Ack %d: %v", i, err)
		}
	}

	length, err := rdb.XLen(ctx, q.stream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != 0 {
		t.Errorf("stream length after %d enqueue+ack cycles = %d, want 0", n, length)
	}
}

// TestDepthReflectsOutstandingEntries checks Depth tracks "enqueued but not
// yet acked" — the backpressure signal Dispatcher.Submit checks — rather
// than just "not yet dequeued".
func TestDepthReflectsOutstandingEntries(t *testing.T) {
	q, _ := newTestQueue(t, 200*time.Millisecond, time.Hour, time.Hour)
	ctx := context.Background()

	assertDepth := func(want int) {
		t.Helper()
		got, err := q.Depth(ctx)
		if err != nil {
			t.Fatalf("Depth: %v", err)
		}
		if got != want {
			t.Errorf("Depth = %d, want %d", got, want)
		}
	}

	assertDepth(0)

	if err := q.Enqueue(ctx, "a"); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue(ctx, "b"); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}
	assertDepth(2) // both outstanding, neither dequeued yet

	_, ackID, ok, err := q.Dequeue(ctx)
	if err != nil || !ok {
		t.Fatalf("Dequeue: ok=%v err=%v", ok, err)
	}
	assertDepth(2) // dequeuing doesn't reduce depth — only Ack does

	if err := q.Ack(ctx, ackID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	assertDepth(1) // one finished, one still outstanding
}
