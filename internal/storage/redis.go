package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements QueueNotifier as a thin pub/sub wrapper. It is
// deliberately not used for durable queue state — Postgres owns that —
// so a flushed or restarted Redis instance only costs wake-up latency
// (workers fall back to polling on an interval), never data loss.
type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func channelName(queue string) string {
	return fmt.Sprintf("taskqueue:notify:%s", queue)
}

func (s *RedisStore) Publish(ctx context.Context, queue string, taskID string) error {
	if err := s.client.Publish(ctx, channelName(queue), taskID).Err(); err != nil {
		return fmt.Errorf("redis: publish notification for queue %s: %w", queue, err)
	}
	return nil
}

func (s *RedisStore) Subscribe(ctx context.Context, queue string) (<-chan string, error) {
	sub := s.client.Subscribe(ctx, channelName(queue))
	if _, err := sub.Receive(ctx); err != nil {
		return nil, fmt.Errorf("redis: subscribe to queue %s: %w", queue, err)
	}

	out := make(chan string, 64)
	go func() {
		defer close(out)
		defer sub.Close()
		for msg := range sub.Channel() {
			select {
			case out <- msg.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
