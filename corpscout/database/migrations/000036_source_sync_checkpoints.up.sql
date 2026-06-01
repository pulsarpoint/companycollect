CREATE TABLE source_sync_checkpoints (
    source_name       TEXT        PRIMARY KEY,
    cursor            TEXT        NOT NULL DEFAULT '',
    last_completed_at TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
