-- Stub for the river_job table managed by the River queue library.
-- This file is NOT a migration — it exists only so sqlc can resolve
-- the river_job table when queries join against it.
-- River creates this table at startup via its own migration mechanism.

CREATE TYPE river_job_state AS ENUM (
    'available',
    'cancelled',
    'completed',
    'discarded',
    'pending',
    'retryable',
    'running',
    'scheduled'
);

CREATE TABLE river_job (
    id            BIGINT      PRIMARY KEY,
    state         river_job_state NOT NULL DEFAULT 'available',
    attempt       SMALLINT    NOT NULL DEFAULT 0,
    max_attempts  SMALLINT    NOT NULL,
    attempted_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finalized_at  TIMESTAMPTZ,
    scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    priority      SMALLINT    NOT NULL DEFAULT 1,
    args          JSONB       NOT NULL,
    attempted_by  TEXT[],
    errors        JSONB[],
    kind          TEXT        NOT NULL,
    metadata      JSONB       NOT NULL DEFAULT '{}',
    queue         TEXT        NOT NULL DEFAULT 'default',
    tags          TEXT[]      NOT NULL DEFAULT '{}'
);
