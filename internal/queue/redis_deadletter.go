package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/roneettopiwala/relay/internal/dispatch"
)

// defaultDLQStream is the production dead-letter stream, separate from the
// main work queue (defaultStream) — recording a permanent failure has
// nothing to do with the dispatch/consumer-group machinery the main queue
// needs; it's just an append-and-read log.
const defaultDLQStream = "relay:tasks:dlq"

// dlqMaxLen approximately caps the dead-letter stream so a long-running
// server with a steady trickle of permanent failures doesn't grow it
// without bound. "Approximately" because exact trimming on every write is
// real overhead for no real benefit here — this is a debugging aid, not a
// system of record with a compliance requirement.
const dlqMaxLen = 10000

// RedisDeadLetterQueue implements dispatch.DeadLetterQueue on a Redis
// stream, separate from RedisQueue's work stream.
type RedisDeadLetterQueue struct {
	rdb    *redis.Client
	stream string
}

// NewRedisDeadLetterQueue returns a dead-letter queue on the production
// stream. No group/consumer setup needed — unlike the work queue, nothing
// here claims deliveries or needs crash recovery; it's a plain log.
func NewRedisDeadLetterQueue(rdb *redis.Client) *RedisDeadLetterQueue {
	return &RedisDeadLetterQueue{rdb: rdb, stream: defaultDLQStream}
}

// newRedisDeadLetterQueue is the unexported constructor with a stream
// override, used by tests to get an isolated throwaway stream.
func newRedisDeadLetterQueue(rdb *redis.Client, stream string) *RedisDeadLetterQueue {
	return &RedisDeadLetterQueue{rdb: rdb, stream: stream}
}

// Record implements dispatch.DeadLetterQueue. The entry is serialized as one
// JSON field rather than flat Redis fields because Spec.Cmd is a slice, which
// doesn't map cleanly onto flat field/value pairs.
func (q *RedisDeadLetterQueue) Record(ctx context.Context, entry dispatch.DeadLetterEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("queue: marshal dead-letter entry: %w", err)
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		MaxLen: dlqMaxLen,
		Approx: true,
		Values: map[string]any{"data": data},
	}).Err()
}

// List implements dispatch.DeadLetterQueue, returning up to limit entries,
// most recent first.
func (q *RedisDeadLetterQueue) List(ctx context.Context, limit int) ([]dispatch.DeadLetterEntry, error) {
	msgs, err := q.rdb.XRevRangeN(ctx, q.stream, "+", "-", int64(limit)).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]dispatch.DeadLetterEntry, 0, len(msgs))
	for _, msg := range msgs {
		raw, ok := msg.Values["data"].(string)
		if !ok {
			continue // malformed entry; skip rather than fail the whole list
		}
		var entry dispatch.DeadLetterEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
