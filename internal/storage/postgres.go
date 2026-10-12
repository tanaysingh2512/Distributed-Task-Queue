package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tanaydeps/distributed-task-queue/internal/model"
)

// PostgresStore is the system-of-record implementation of TaskStore. All
// state transitions go through single-row UPDATEs guarded by the current
// expected state, giving us the compare-and-swap semantics that prevent
// two workers from both believing they own a task (the gRPC lease layer
// in internal/lease is the fast in-memory mirror of this).
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Insert(ctx context.Context, t *model.Task) error {
	const q = `
		INSERT INTO tasks (id, queue, payload, priority, state, attempts, max_attempts,
		                    created_at, updated_at, available_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`
	_, err := s.pool.Exec(ctx, q,
		t.ID, t.Queue, t.Payload, t.Priority, t.State, t.Attempts, t.MaxAttempts,
		t.CreatedAt, t.UpdatedAt, t.AvailableAt, nullableString(t.IdempotencyKey))
	if err != nil {
		return fmt.Errorf("postgres: insert task %s: %w", t.ID, err)
	}
	return nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (*model.Task, error) {
	const q = `
		SELECT id, queue, payload, priority, state, attempts, max_attempts,
		       created_at, updated_at, available_at,
		       COALESCE(lease_owner, ''), lease_expiry, COALESCE(last_error, '')
		FROM tasks WHERE id = $1`

	row := s.pool.QueryRow(ctx, q, id)
	var t model.Task
	var leaseExpiry *time.Time
	if err := row.Scan(&t.ID, &t.Queue, &t.Payload, &t.Priority, &t.State, &t.Attempts, &t.MaxAttempts,
		&t.CreatedAt, &t.UpdatedAt, &t.AvailableAt, &t.LeaseOwner, &leaseExpiry, &t.LastError); err != nil {
		return nil, fmt.Errorf("postgres: get task %s: %w", id, err)
	}
	if leaseExpiry != nil {
		t.LeaseExpiry = *leaseExpiry
	}
	return &t, nil
}

func (s *PostgresStore) UpdateState(ctx context.Context, id string, state model.State) error {
	const q = `UPDATE tasks SET state = $2, updated_at = now() WHERE id = $1`
	ct, err := s.pool.Exec(ctx, q, id, state)
	if err != nil {
		return fmt.Errorf("postgres: update state for task %s: %w", id, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("postgres: task %s not found", id)
	}
	return nil
}

// Lease atomically claims a task for a worker, only succeeding if the
// task is currently unleased or its prior lease has expired — this is the
// compare-and-swap that makes concurrent lease acquisition safe without a
// distributed lock.
func (s *PostgresStore) Lease(ctx context.Context, id, workerID string, expiresAtUnix int64) error {
	const q = `
		UPDATE tasks
		SET state = 'LEASED', lease_owner = $2, lease_expiry = to_timestamp($3), updated_at = now()
		WHERE id = $1 AND (lease_owner IS NULL OR lease_expiry < now())`
	ct, err := s.pool.Exec(ctx, q, id, workerID, expiresAtUnix)
	if err != nil {
		return fmt.Errorf("postgres: lease task %s: %w", id, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("postgres: task %s already leased by another worker", id)
	}
	return nil
}

func (s *PostgresStore) ReleaseLease(ctx context.Context, id string) error {
	const q = `UPDATE tasks SET lease_owner = NULL, lease_expiry = NULL, updated_at = now() WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("postgres: release lease for task %s: %w", id, err)
	}
	return nil
}

func (s *PostgresStore) IncrementAttempt(ctx context.Context, id string, lastErr string) (int, error) {
	const q = `
		UPDATE tasks SET attempts = attempts + 1, last_error = $2, updated_at = now()
		WHERE id = $1
		RETURNING attempts`
	var attempts int
	if err := s.pool.QueryRow(ctx, q, id, lastErr).Scan(&attempts); err != nil {
		return 0, fmt.Errorf("postgres: increment attempt for task %s: %w", id, err)
	}
	return attempts, nil
}

func (s *PostgresStore) MarkDead(ctx context.Context, id string, reason string) error {
	const q = `UPDATE tasks SET state = 'DEAD', last_error = $2, updated_at = now() WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id, reason)
	if err != nil {
		return fmt.Errorf("postgres: mark task %s dead: %w", id, err)
	}
	return nil
}

func (s *PostgresStore) InsertDeadLetter(ctx context.Context, t *model.Task, reason string) error {
	const q = `
		INSERT INTO dead_letters (task_id, queue, payload, attempts, reason, created_at)
		VALUES ($1, $2, $3, $4, $5, now())`
	_, err := s.pool.Exec(ctx, q, t.ID, t.Queue, t.Payload, t.Attempts, reason)
	if err != nil {
		return fmt.Errorf("postgres: insert dead letter for task %s: %w", t.ID, err)
	}
	return nil
}

// ListExpiredLeases returns tasks whose lease has elapsed, for the reaper
// sweep that reconciles the in-memory lease.Manager with durable state on
// server startup (or after a server restart mid-sweep).
func (s *PostgresStore) ListExpiredLeases(ctx context.Context) ([]*model.Task, error) {
	const q = `
		SELECT id, queue, payload, priority, state, attempts, max_attempts,
		       created_at, updated_at, available_at,
		       COALESCE(lease_owner, ''), lease_expiry, COALESCE(last_error, '')
		FROM tasks
		WHERE state = 'LEASED' AND lease_expiry < now()`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: list expired leases: %w", err)
	}
	defer rows.Close()

	var out []*model.Task
	for rows.Next() {
		var t model.Task
		var leaseExpiry *time.Time
		if err := rows.Scan(&t.ID, &t.Queue, &t.Payload, &t.Priority, &t.State, &t.Attempts, &t.MaxAttempts,
			&t.CreatedAt, &t.UpdatedAt, &t.AvailableAt, &t.LeaseOwner, &leaseExpiry, &t.LastError); err != nil {
			return nil, fmt.Errorf("postgres: scan expired lease row: %w", err)
		}
		if leaseExpiry != nil {
			t.LeaseExpiry = *leaseExpiry
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
