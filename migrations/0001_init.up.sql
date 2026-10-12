CREATE TABLE IF NOT EXISTS tasks (
    id              UUID PRIMARY KEY,
    queue           TEXT NOT NULL,
    payload         BYTEA NOT NULL,
    priority        SMALLINT NOT NULL DEFAULT 5,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 8,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner     TEXT,
    lease_expiry    TIMESTAMPTZ,
    last_error      TEXT,
    idempotency_key TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_tasks_dequeue
    ON tasks (queue, priority, available_at)
    WHERE state = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_tasks_lease_expiry
    ON tasks (lease_expiry)
    WHERE state = 'LEASED';

CREATE TABLE IF NOT EXISTS dead_letters (
    id          BIGSERIAL PRIMARY KEY,
    task_id     UUID NOT NULL,
    queue       TEXT NOT NULL,
    payload     BYTEA NOT NULL,
    attempts    INTEGER NOT NULL,
    reason      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_queue ON dead_letters (queue, created_at DESC);
