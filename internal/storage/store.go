// Package storage defines the persistence boundary of the system.
// Postgres is the system of record for task state (durability, exactly
// the row-level transactions needed for at-least-once semantics); Redis
// backs the fast-path dequeue notification stream so workers don't poll
// Postgres directly under load. See docs/architecture.md for the split.
package storage

import (
	"context"

	"github.com/tanaydeps/distributed-task-queue/internal/model"
)

// TaskStore is the durable persistence interface, implemented by
// PostgresStore. Kept as an interface so internal/dlq, internal/lease and
// the gRPC server can be tested against an in-memory fake without a live
// database.
type TaskStore interface {
	Insert(ctx context.Context, t *model.Task) error
	Get(ctx context.Context, id string) (*model.Task, error)
	UpdateState(ctx context.Context, id string, state model.State) error
	Lease(ctx context.Context, id, workerID string, expiresAt int64) error
	ReleaseLease(ctx context.Context, id string) error
	IncrementAttempt(ctx context.Context, id string, lastErr string) (int, error)
	MarkDead(ctx context.Context, id string, reason string) error
	InsertDeadLetter(ctx context.Context, t *model.Task, reason string) error
	ListExpiredLeases(ctx context.Context) ([]*model.Task, error)
}

// QueueNotifier is the fast-path interface, implemented by RedisStore. It
// notifies idle workers that new work is available without them having to
// poll Postgres, and is used as a pub/sub companion to the durable store
// above, never as a source of truth.
type QueueNotifier interface {
	Publish(ctx context.Context, queue string, taskID string) error
	Subscribe(ctx context.Context, queue string) (<-chan string, error)
}
