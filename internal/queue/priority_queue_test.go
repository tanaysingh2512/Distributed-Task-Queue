package queue

import (
	"testing"
	"time"

	"github.com/tanaydeps/distributed-task-queue/internal/model"
)

func TestPopReadyOrdersByPriorityThenAge(t *testing.T) {
	q := New()
	now := time.Now().UTC()

	low := model.NewTask("default", []byte("low"), model.PriorityLow, 3)
	low.AvailableAt = now

	high := model.NewTask("default", []byte("high"), model.PriorityHigh, 3)
	high.AvailableAt = now

	critical := model.NewTask("default", []byte("critical"), model.PriorityCritical, 3)
	critical.AvailableAt = now

	q.Push(low)
	q.Push(high)
	q.Push(critical)

	first := q.PopReady(now)
	if first == nil || first.Priority != model.PriorityCritical {
		t.Fatalf("expected critical priority task first, got %+v", first)
	}

	second := q.PopReady(now)
	if second == nil || second.Priority != model.PriorityHigh {
		t.Fatalf("expected high priority task second, got %+v", second)
	}

	third := q.PopReady(now)
	if third == nil || third.Priority != model.PriorityLow {
		t.Fatalf("expected low priority task third, got %+v", third)
	}
}

func TestPopReadyRespectsAvailableAt(t *testing.T) {
	q := New()
	now := time.Now().UTC()

	future := model.NewTask("default", []byte("delayed"), model.PriorityCritical, 3)
	future.AvailableAt = now.Add(1 * time.Hour)
	q.Push(future)

	if got := q.PopReady(now); got != nil {
		t.Fatalf("expected no ready task, got %+v", got)
	}

	if got := q.PopReady(now.Add(2 * time.Hour)); got == nil {
		t.Fatalf("expected task to become ready after AvailableAt elapses")
	}
}

func TestPushUpdatesExistingTaskInPlace(t *testing.T) {
	q := New()
	now := time.Now().UTC()

	task := model.NewTask("default", []byte("retry-me"), model.PriorityLow, 3)
	task.AvailableAt = now.Add(1 * time.Hour)
	q.Push(task)

	if q.Len() != 1 {
		t.Fatalf("expected queue length 1, got %d", q.Len())
	}

	// Simulate a NACK that reschedules the same task with a new priority
	// and an immediate AvailableAt.
	task.Priority = model.PriorityCritical
	task.AvailableAt = now
	q.Push(task)

	if q.Len() != 1 {
		t.Fatalf("expected re-push to update in place, got length %d", q.Len())
	}

	popped := q.PopReady(now)
	if popped == nil || popped.Priority != model.PriorityCritical {
		t.Fatalf("expected updated task with critical priority, got %+v", popped)
	}
}

func TestRemove(t *testing.T) {
	q := New()
	now := time.Now().UTC()

	task := model.NewTask("default", []byte("leased"), model.PriorityDefault, 3)
	task.AvailableAt = now
	q.Push(task)
	q.Remove(task.ID.String())

	if q.Len() != 0 {
		t.Fatalf("expected queue to be empty after remove, got length %d", q.Len())
	}
	if got := q.PopReady(now); got != nil {
		t.Fatalf("expected no task after remove, got %+v", got)
	}
}
