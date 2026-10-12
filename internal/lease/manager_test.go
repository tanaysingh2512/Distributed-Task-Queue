package lease

import (
	"testing"
	"time"
)

func TestAcquireAndRenew(t *testing.T) {
	m := NewManager(10 * time.Second)
	now := time.Now().UTC()

	entry, err := m.Acquire("task-1", "worker-a", now)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if entry.WorkerID != "worker-a" {
		t.Fatalf("expected worker-a, got %s", entry.WorkerID)
	}

	if err := m.Renew("task-1", "worker-a", now.Add(5*time.Second)); err != nil {
		t.Fatalf("renew failed: %v", err)
	}

	if err := m.Renew("task-1", "worker-b", now.Add(5*time.Second)); err == nil {
		t.Fatalf("expected renew by non-owner to fail")
	}
}

func TestAcquireRejectsConflictingActiveLease(t *testing.T) {
	m := NewManager(10 * time.Second)
	now := time.Now().UTC()

	if _, err := m.Acquire("task-1", "worker-a", now); err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	if _, err := m.Acquire("task-1", "worker-b", now.Add(1*time.Second)); err == nil {
		t.Fatalf("expected second acquire by different worker to fail while lease is active")
	}
}

func TestAcquireSucceedsAfterExpiry(t *testing.T) {
	m := NewManager(5 * time.Second)
	now := time.Now().UTC()

	if _, err := m.Acquire("task-1", "worker-a", now); err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	// worker-a crashes and never renews; worker-b should be able to
	// acquire once the TTL has elapsed.
	later := now.Add(10 * time.Second)
	if _, err := m.Acquire("task-1", "worker-b", later); err != nil {
		t.Fatalf("expected acquire to succeed after expiry, got: %v", err)
	}
}

func TestReapReturnsExpiredTasks(t *testing.T) {
	m := NewManager(5 * time.Second)
	now := time.Now().UTC()

	if _, err := m.Acquire("task-1", "worker-a", now); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if _, err := m.Acquire("task-2", "worker-b", now); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	// Renew task-2 so it should survive the reap sweep below.
	if err := m.Renew("task-2", "worker-b", now.Add(20*time.Second)); err != nil {
		t.Fatalf("renew failed: %v", err)
	}

	reclaimed := m.Reap(now.Add(10 * time.Second))
	if len(reclaimed) != 1 || reclaimed[0] != "task-1" {
		t.Fatalf("expected only task-1 to be reclaimed, got %v", reclaimed)
	}

	if _, ok := m.Owner("task-2"); !ok {
		t.Fatalf("expected task-2 lease to survive reap after renewal")
	}
}

func TestRelease(t *testing.T) {
	m := NewManager(10 * time.Second)
	now := time.Now().UTC()

	if _, err := m.Acquire("task-1", "worker-a", now); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	m.Release("task-1", "worker-a")

	if _, ok := m.Owner("task-1"); ok {
		t.Fatalf("expected no owner after release")
	}

	// Released lease should be immediately acquirable by another worker.
	if _, err := m.Acquire("task-1", "worker-b", now); err != nil {
		t.Fatalf("expected acquire after release to succeed, got: %v", err)
	}
}
