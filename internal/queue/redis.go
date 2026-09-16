// Package queue is the Redis Streams implementation of dispatch.Queue: a
// durable queue of pending task ids, shared via a consumer group so that
// multiple dispatcher processes could read from one stream without two of
// them ever claiming the same entry, and so that a delivery claimed by a
// consumer that goes quiet (crashes, hangs) gets reclaimed and retried by a
// live one instead of vanishing.
//
// See the package's design note in dispatch.Queue for the interface this
// implements, and internal/dispatch's dispatcher.go for how it's used.
package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultStream and defaultGroup are the production stream/consumer-group
// names. They're passed as plain parameters (not baked into the methods
// below) so tests can point independent RedisQueue instances at their own
// throwaway stream, isolated from each other and from a real deployment
// sharing the same Redis instance.
const (
	defaultStream = "relay:tasks"
	defaultGroup  = "relay-workers"
)

// RedisQueue implements dispatch.Queue on top of a Redis stream + consumer
// group.
type RedisQueue struct {
	rdb      *redis.Client
	stream   string
	group    string
	consumer string

	blockFor        time.Duration // how long one Dequeue call waits for new work
	reclaimInterval time.Duration // how often Dequeue checks for orphaned entries
	reclaimIdle     time.Duration // how long a delivery must sit unacked before it's orphaned

	lastReclaim time.Time // loop() only ever calls Dequeue from one goroutine, so no lock needed here
}

// NewRedisQueue creates the stream and consumer group if they don't already
// exist, and returns a queue backed by them. consumer identity is derived
// from hostname+pid so multiple relay processes sharing one Redis instance
// get distinct, stable-ish consumer names.
func NewRedisQueue(rdb *redis.Client) (*RedisQueue, error) {
	return newRedisQueue(rdb, defaultStream, defaultGroup, 2*time.Second, 10*time.Second, 90*time.Second)
}

// newRedisQueue is the unexported constructor with a configurable stream,
// group, and timing, used directly by tests that need an isolated stream and
// fast reclaim behaviour instead of the production defaults above.
func newRedisQueue(rdb *redis.Client, stream, group string, blockFor, reclaimInterval, reclaimIdle time.Duration) (*RedisQueue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !isBusyGroupErr(err) {
		return nil, fmt.Errorf("queue: create consumer group: %w", err)
	}

	host, _ := os.Hostname()
	if host == "" {
		host = "relay"
	}
	consumer := fmt.Sprintf("%s-%d", host, os.Getpid())

	return &RedisQueue{
		rdb:             rdb,
		stream:          stream,
		group:           group,
		consumer:        consumer,
		blockFor:        blockFor,
		reclaimInterval: reclaimInterval,
		reclaimIdle:     reclaimIdle,
	}, nil
}

// isBusyGroupErr reports whether err is Redis's "the group already exists"
// error, which is expected (and fine) on every startup after the first.
func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Enqueue implements dispatch.Queue.
func (q *RedisQueue) Enqueue(ctx context.Context, taskID string) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{"id": taskID},
	}).Err()
}

// Dequeue implements dispatch.Queue. Before waiting for fresh work, it
// periodically checks for entries orphaned by a dead consumer (see
// reclaimOrphaned) and adopts one if found — that adoption, combined with
// run()'s Terminal()/missing-record checks, is the actual crash-recovery
// mechanism: a task claimed by a consumer that never acks it doesn't vanish,
// it gets handed to a live one.
func (q *RedisQueue) Dequeue(ctx context.Context) (taskID, ackID string, ok bool, err error) {
	if time.Since(q.lastReclaim) > q.reclaimInterval {
		q.lastReclaim = time.Now()
		if id, entryID, found, rerr := q.reclaimOrphaned(ctx); rerr == nil && found {
			return id, entryID, true, nil
		}
	}

	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    q.blockFor,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", "", false, nil // block timeout elapsed, nothing new — not an error
		}
		return "", "", false, err
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return "", "", false, nil
	}

	msg := res[0].Messages[0]
	id, _ := msg.Values["id"].(string)
	if id == "" {
		// Malformed entry (shouldn't happen — we control Enqueue). Ack it so
		// it doesn't jam the queue forever, and report "nothing this round".
		_ = q.Ack(ctx, msg.ID)
		return "", "", false, nil
	}
	return id, msg.ID, true, nil
}

// reclaimOrphaned adopts one entry that's been claimed by some consumer for
// longer than reclaimIdle without being acked — i.e. that consumer is
// presumed dead. XAUTOCLAIM both reassigns ownership in Redis's bookkeeping
// and returns the entry's contents directly, so the result can be handed
// straight back to the caller exactly like a fresh Dequeue.
func (q *RedisQueue) reclaimOrphaned(ctx context.Context) (taskID, ackID string, found bool, err error) {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		Consumer: q.consumer,
		MinIdle:  q.reclaimIdle,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil || len(msgs) == 0 {
		return "", "", false, err
	}

	msg := msgs[0]
	id, _ := msg.Values["id"].(string)
	if id == "" {
		_ = q.Ack(ctx, msg.ID)
		return "", "", false, nil
	}
	return id, msg.ID, true, nil
}

// Depth implements dispatch.Queue. XLEN is exactly the right number here:
// Ack (below) deletes an entry from the stream once it's handled, so what's
// left is precisely "enqueued but not yet finished" — queued-or-running.
func (q *RedisQueue) Depth(ctx context.Context) (int, error) {
	n, err := q.rdb.XLen(ctx, q.stream).Result()
	return int(n), err
}

// Ack implements dispatch.Queue. It both acknowledges the delivery (removing
// it from the consumer group's pending-entries list, so it's no longer
// eligible for reclaim) and deletes the entry from the stream — Streams are
// append-only, so without an explicit delete the stream would grow forever
// even though every entry has long since been handled.
func (q *RedisQueue) Ack(ctx context.Context, ackID string) error {
	pipe := q.rdb.TxPipeline()
	pipe.XAck(ctx, q.stream, q.group, ackID)
	pipe.XDel(ctx, q.stream, ackID)
	_, err := pipe.Exec(ctx)
	return err
}
