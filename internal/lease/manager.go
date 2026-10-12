// Package lease implements worker coordination via a heartbeat/lease
// protocol. A worker that dequeues a task is granted a time-bounded lease;
// it must renew the lease (heartbeat) before it expires or the task is
// considered abandoned and returned to the schedulable pool. This is what
// gives the system automatic crash recovery: a worker that dies holding a
// lease simply stops heartbeating, and the reaper reclaims its work after
// the lease TTL elapses — no explicit failure detection required.
package lease

import (
	"fmt"
	"sync"
	"time"

	"github.com/tanaydeps/distributed-task-queue/internal/model"
)

// DefaultTTL is how long a lease is valid without a renewing heartbeat.
// Chosen to comfortably exceed the gRPC heartbeat interval (see
// api/proto/taskqueue/v1/taskqueue.proto HeartbeatRequest) while staying
// short enough that a crashed worker's tasks are reclaimed quickly.
const DefaultTTL = 30 * time.Second

// Entry tracks the lease held by a worker over a task.
type Entry struct {
	TaskID    string
	WorkerID  string
	ExpiresAt time.Time
}

// Manager tracks all outstanding leases in memory. It is the server's
// source of truth for "who currently owns this task"; Postgres stores the
// same information durably (lease_owner/lease_expiry columns on the task
// row) so the manager can be rehydrated on restart.
type Manager struct {
	mu     sync.Mutex
	leases map[string]*Entry // taskID -> lease
	ttl    time.Duration
}

func NewManager(ttl time.Duration) *Manager {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Manager{
		leases: make(map[string]*Entry),
		ttl:    ttl,
	}
}

// Acquire grants workerID a fresh lease over taskID. Fails if the task is
// already validly leased by a different worker.
func (m *Manager) Acquire(taskID, workerID string, now time.Time) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.leases[taskID]; ok && existing.WorkerID != workerID && now.Before(existing.ExpiresAt) {
		return nil, fmt.Errorf("lease: task %s already leased by %s until %s", taskID, existing.WorkerID, existing.ExpiresAt)
	}

	entry := &Entry{
		TaskID:    taskID,
		WorkerID:  workerID,
		ExpiresAt: now.Add(m.ttl),
	}
	m.leases[taskID] = entry
	return entry, nil
}

// Renew extends an existing lease on heartbeat. Fails if the caller no
// longer owns the lease (e.g. it already expired and was reclaimed by
// another worker, or the task completed).
func (m *Manager) Renew(taskID, workerID string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.leases[taskID]
	if !ok || entry.WorkerID != workerID {
		return fmt.Errorf("lease: %s does not hold an active lease on task %s", workerID, taskID)
	}
	entry.ExpiresAt = now.Add(m.ttl)
	return nil
}

// Release voluntarily gives up a lease, typically after a successful ack
// or an explicit nack.
func (m *Manager) Release(taskID, workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.leases[taskID]; ok && entry.WorkerID == workerID {
		delete(m.leases, taskID)
	}
}

// Reap scans for leases past their expiry and removes them, returning the
// task IDs that were reclaimed so the caller can requeue them (and
// increment their attempt counters). Intended to run on a periodic
// ticker, e.g. every TTL/2.
func (m *Manager) Reap(now time.Time) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var reclaimed []string
	for taskID, entry := range m.leases {
		if now.After(entry.ExpiresAt) {
			reclaimed = append(reclaimed, taskID)
			delete(m.leases, taskID)
		}
	}
	return reclaimed
}

// Owner returns the current lease holder of a task, if any.
func (m *Manager) Owner(taskID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.leases[taskID]
	if !ok {
		return "", false
	}
	return entry.WorkerID, true
}

// ApplyToTask stamps lease fields onto a task model, used when persisting
// lease state to Postgres alongside the in-memory manager.
func ApplyToTask(t *model.Task, e *Entry) {
	t.LeaseOwner = e.WorkerID
	t.LeaseExpiry = e.ExpiresAt
	t.State = model.StateLeased
}
