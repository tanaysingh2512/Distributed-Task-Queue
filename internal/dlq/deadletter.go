// Package dlq handles routing of tasks that have exhausted their retry
// budget. Dead-lettered tasks are never silently dropped: they are
// persisted with their full failure history (internal/storage) so an
// operator can inspect, fix, and manually replay them.
package dlq

import (
	"context"
	"fmt"
	"time"

	"github.com/tanaydeps/distributed-task-queue/internal/model"
)

// Store is the persistence surface dlq needs; satisfied by
// internal/storage.TaskStore so this package stays decoupled from the
// concrete Postgres implementation.
type Store interface {
	MarkDead(ctx context.Context, taskID string, reason string) error
	InsertDeadLetter(ctx context.Context, t *model.Task, reason string) error
}

type Router struct {
	store Store
}

func NewRouter(store Store) *Router {
	return &Router{store: store}
}

// Route moves a task to the dead-letter queue after it has exhausted
// retries. Returns an error describing the terminal failure for logging/
// alerting; callers should emit this on the
// taskqueue_tasks_dead_lettered_total counter (internal/metrics).
func (r *Router) Route(ctx context.Context, t *model.Task) error {
	if !t.ExhaustedRetries() {
		return fmt.Errorf("dlq: task %s has not exhausted retries (%d/%d)", t.ID, t.Attempts, t.MaxAttempts)
	}

	reason := fmt.Sprintf("exhausted %d/%d attempts, last error: %s", t.Attempts, t.MaxAttempts, t.LastError)
	t.State = model.StateDead
	t.UpdatedAt = time.Now().UTC()

	if err := r.store.InsertDeadLetter(ctx, t, reason); err != nil {
		return fmt.Errorf("dlq: persisting dead letter for task %s: %w", t.ID, err)
	}
	if err := r.store.MarkDead(ctx, t.ID.String(), reason); err != nil {
		return fmt.Errorf("dlq: marking task %s dead in primary store: %w", t.ID, err)
	}
	return nil
}
