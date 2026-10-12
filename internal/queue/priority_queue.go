// Package queue implements the in-memory scheduling structure used by a
// single server instance to decide dequeue order before a task is leased
// out to a worker. The durable queue state lives in Postgres/Redis
// (internal/storage); this heap is a working set rebuilt from storage on
// startup and kept in sync as tasks are enqueued, leased, and completed.
package queue

import (
	"container/heap"
	"sync"
	"time"

	"github.com/tanaydeps/distributed-task-queue/internal/model"
)

// item wraps a task with the bookkeeping container/heap needs.
type item struct {
	task  *model.Task
	index int
}

// minHeap orders items first by priority (ascending, so PriorityCritical=0
// surfaces first), then by AvailableAt (FIFO among equal priority), so
// backoff-delayed retries don't starve newer tasks of the same priority.
type minHeap []*item

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	if h[i].task.Priority != h[j].task.Priority {
		return h[i].task.Priority < h[j].task.Priority
	}
	return h[i].task.AvailableAt.Before(h[j].task.AvailableAt)
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *minHeap) Push(x any) {
	it := x.(*item)
	it.index = len(*h)
	*h = append(*h, it)
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}

// PriorityQueue is a thread-safe, per-queue-name priority scheduler.
type PriorityQueue struct {
	mu    sync.Mutex
	h     minHeap
	index map[string]*item // task ID -> heap entry, for O(log n) removal/update
}

func New() *PriorityQueue {
	return &PriorityQueue{
		h:     make(minHeap, 0),
		index: make(map[string]*item),
	}
}

// Push inserts a task into the schedule. If a task with the same ID is
// already present it is updated in place (used when a NACKed task is
// rescheduled with a new AvailableAt after backoff).
func (q *PriorityQueue) Push(t *model.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := t.ID.String()
	if existing, ok := q.index[id]; ok {
		existing.task = t
		heap.Fix(&q.h, existing.index)
		return
	}

	it := &item{task: t}
	heap.Push(&q.h, it)
	q.index[id] = it
}

// PopReady removes and returns the highest-priority task whose
// AvailableAt has elapsed, as of now. Returns nil if nothing is ready.
func (q *PriorityQueue) PopReady(now time.Time) *model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.h) == 0 {
		return nil
	}
	top := q.h[0]
	if top.task.AvailableAt.After(now) {
		return nil
	}

	it := heap.Pop(&q.h).(*item)
	delete(q.index, it.task.ID.String())
	return it.task
}

// Remove drops a task from the schedule (e.g. it was leased out and is no
// longer eligible for dequeue until the lease expires or it's renacked).
func (q *PriorityQueue) Remove(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.index[id]
	if !ok {
		return
	}
	heap.Remove(&q.h, it.index)
	delete(q.index, id)
}

// Len returns the current number of schedulable (not-yet-leased) tasks.
// Exposed for the `taskqueue_queue_depth` gauge in internal/metrics.
func (q *PriorityQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h)
}
