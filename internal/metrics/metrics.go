// Package metrics defines the Prometheus instrumentation surfaced at
// /metrics by both cmd/server and cmd/worker. Naming follows Prometheus
// convention: unit suffix, _total for counters.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "taskqueue_queue_depth",
			Help: "Number of tasks currently schedulable (pending, not leased), per queue and priority.",
		},
		[]string{"queue", "priority"},
	)

	TasksEnqueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_tasks_enqueued_total",
			Help: "Total number of tasks enqueued.",
		},
		[]string{"queue"},
	)

	TasksCompletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_tasks_completed_total",
			Help: "Total number of tasks that reached a terminal state.",
		},
		[]string{"queue", "outcome"}, // outcome: succeeded|failed|dead_lettered
	)

	TaskLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "taskqueue_task_latency_seconds",
			Help: "End-to-end latency from enqueue to terminal state.",
			// Tuned for sub-second to multi-minute task latencies; p50/p95/p99
			// are derived from this histogram in the Grafana dashboard
			// (deployments/grafana/dashboards/queue-overview.json).
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		},
		[]string{"queue"},
	)

	LeaseRenewalsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_lease_renewals_total",
			Help: "Total number of successful lease heartbeat renewals.",
		},
		[]string{"worker_id"},
	)

	LeasesReclaimedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_leases_reclaimed_total",
			Help: "Total number of leases reclaimed by the reaper after expiry (i.e. presumed worker crash).",
		},
		[]string{"queue"},
	)

	WorkerHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "taskqueue_worker_up",
			Help: "1 if the worker's most recent heartbeat is within the lease TTL window, 0 otherwise.",
		},
		[]string{"worker_id"},
	)

	RetryBackoffSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "taskqueue_retry_backoff_seconds",
			Help:    "Computed backoff delay applied before a retry becomes eligible for redelivery.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12),
		},
		[]string{"queue"},
	)
)

// Registry bundles all collectors for a single prometheus.Register() call
// from cmd/server and cmd/worker.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		QueueDepth,
		TasksEnqueuedTotal,
		TasksCompletedTotal,
		TaskLatencySeconds,
		LeaseRenewalsTotal,
		LeasesReclaimedTotal,
		WorkerHealth,
		RetryBackoffSeconds,
	}
}
