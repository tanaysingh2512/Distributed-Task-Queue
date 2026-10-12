# Architecture

## Why Postgres *and* Redis

Postgres is the system of record. Every state transition a task goes
through — enqueue, lease, heartbeat, ack, nack, dead-letter — is a
row-level update guarded by the task's current expected state, which is
what gives the system its at-least-once delivery guarantee: a task is
never considered "taken" by a worker unless that compare-and-swap commits.

Redis is not used for durability at all. It's a pub/sub wake-up channel so
idle workers don't have to poll Postgres on a tight interval waiting for
work. If Redis is flushed or goes down, the system degrades to polling —
slower dequeue latency, no correctness impact — because nothing that
matters for correctness is ever only in Redis.

## Lease / heartbeat protocol

See `internal/lease/manager.go` and `api/proto/taskqueue/v1/taskqueue.proto`.

1. A worker calls `Lease(queue, worker_id)`. The server performs a
   compare-and-swap UPDATE in Postgres (`lease_owner IS NULL OR
   lease_expiry < now()`) and mirrors the grant into the in-memory
   `lease.Manager` for fast heartbeat checks.
2. The worker heartbeats on an interval well under the lease TTL
   (`internal/lease.DefaultTTL = 30s`, heartbeat every ~10s).
3. If the worker crashes, heartbeats stop, the lease expires, and the
   server's reaper (`runReaper` in `cmd/server/main.go`, ticking every
   TTL/2) sweeps `ListExpiredLeases` and pushes those tasks back onto the
   schedulable priority queue. No separate failure detector is needed —
   lease expiry *is* the failure detector.

## Retry and dead-lettering

`internal/retry.Policy` implements full-jitter exponential backoff
(uniform random delay in `[0, min(base * 2^attempt, max)]`), which avoids
the thundering-herd retry pattern that plain exponential backoff produces
when many tasks fail at once (e.g. a downstream dependency blip).

Once a task's attempt count reaches `MaxAttempts`, `internal/dlq.Router`
routes it to the `dead_letters` table with its full failure reason instead
of being requeued, so a stuck poison-pill message can't loop forever and
an operator can inspect/replay it later.

## Priority scheduling

`internal/queue.PriorityQueue` is a binary min-heap ordered first by
priority (`PriorityCritical=0` serviced first), then by `AvailableAt`
within the same priority, so delayed retries don't jump ahead of older
same-priority tasks once they become eligible. This heap is the server's
working set; it's rebuilt from Postgres on startup and is not itself a
source of truth.

## What's implemented vs. stubbed

The core scheduling, retry, lease-coordination, and persistence logic
(`internal/queue`, `internal/retry`, `internal/lease`, `internal/dlq`,
`internal/storage`) is fully implemented and unit-tested.

The gRPC server/client wiring in `cmd/server` and `cmd/worker` is
structured and ready, but the generated protobuf stubs
(`taskqueue_grpc.pb.go`) aren't checked in — run `make proto` with
`protoc` installed to generate them, at which point the `TODO`-marked
handler registration in `cmd/server/main.go` and the RPC calls in
`cmd/worker/main.go` plug in directly against the types already defined
in `internal/model`, `internal/lease`, `internal/retry`, and
`internal/storage`.
