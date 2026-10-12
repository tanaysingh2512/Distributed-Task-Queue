package model

import (
	"time"

	"github.com/google/uuid"
)

// Priority determines scheduling order within the queue. Lower numeric
// value is serviced first, matching the convention used by the internal
// heap in internal/queue.
type Priority int

const (
	PriorityLow      Priority = 10
	PriorityDefault  Priority = 5
	PriorityHigh     Priority = 1
	PriorityCritical Priority = 0
)

// State represents the lifecycle of a task as it moves through the system.
type State string

const (
	StatePending   State = "PENDING"
	StateLeased    State = "LEASED"
	StateRunning   State = "RUNNING"
	StateSucceeded State = "SUCCEEDED"
	StateFailed    State = "FAILED"
	StateRetrying  State = "RETRYING"
	StateDead      State = "DEAD" // moved to the dead-letter queue
)

// Task is the unit of work tracked by the queue. It is persisted in
// Postgres (system of record) and mirrored into Redis for fast dequeue
// operations; see internal/storage for the split.
type Task struct {
	ID             uuid.UUID
	Queue          string
	Payload        []byte
	Priority       Priority
	State          State
	Attempts       int
	MaxAttempts    int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	AvailableAt    time.Time // task is not dequeued before this time (used by backoff)
	LeaseOwner     string    // worker ID currently holding the lease, empty if unleased
	LeaseExpiry    time.Time
	LastError      string
	IdempotencyKey string
}

// IsExpiredLease reports whether the current lease, if any, has elapsed
// without a heartbeat renewal. Used by the reaper (internal/lease) during
// crash recovery sweeps.
func (t *Task) IsExpiredLease(now time.Time) bool {
	if t.LeaseOwner == "" {
		return false
	}
	return now.After(t.LeaseExpiry)
}

// ExhaustedRetries reports whether the task has consumed its retry budget
// and should be routed to the dead-letter queue instead of being
// rescheduled.
func (t *Task) ExhaustedRetries() bool {
	return t.Attempts >= t.MaxAttempts
}

func NewTask(queue string, payload []byte, priority Priority, maxAttempts int) *Task {
	now := time.Now().UTC()
	return &Task{
		ID:          uuid.New(),
		Queue:       queue,
		Payload:     payload,
		Priority:    priority,
		State:       StatePending,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
		AvailableAt: now,
	}
}
